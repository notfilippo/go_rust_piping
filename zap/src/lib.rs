use std::fs::File;
use std::os::fd::{FromRawFd, RawFd};
use std::sync::Arc;
use std::thread::JoinHandle;

use arrow::array::{Array, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Schema};
use arrow::ffi::{from_ffi_and_data_type, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::{FutureExt, StreamExt};
use table::OneShotStreamWrapper;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

mod contract;
mod pipe;
mod table;

use tikv_jemallocator::Jemalloc;
use tokio::sync::mpsc::error::SendError;

use crate::contract::{InputMessage, MessagePipe, OutputMessage};

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// A query to be executed by the executor.
struct Query {
    sql: String,
    schema: Schema,
    input_receiver: File, // file descriptor for reading input data (pipe read end)
    output_sender: File,  // file descriptor for writing output data (pipe write end)
}

/// An instance of the executor. The instance holds the channel that will be used to schedule
/// queries and the join handle for the executor task.
struct Instance {
    query_input_tx: Sender<Query>,
    join_handle: JoinHandle<()>,
}

impl Instance {
    /// Create a new instance of the executor.
    pub fn new(buffer: usize) -> Self {
        let (query_input_tx, query_input_rx) = channel(buffer);

        let join_handle = std::thread::spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime")
                .block_on(entrypoint(query_input_rx))
                .expect("executor start");
        });

        Self {
            query_input_tx,
            join_handle,
        }
    }

    /// Submit a query to the executor.
    pub fn query(&self, query: Query) -> Result<(), SendError<Query>> {
        self.query_input_tx.blocking_send(query)
    }

    /// Stop the executor and join the execution task on the current.
    pub fn stop(self) {
        drop(self.query_input_tx);
        self.join_handle.join().expect("executor stop");
        eprintln!("zap: executor stopped");
    }
}

// The entrypoint for the executor.
//
// How do query work?
// - The executor receives a query from the query_input_rx channel.
// - The executor create a new task for the query.
// - The task reads input data from the input_receiver file descriptor.
// - The task executes the query.
// - The task writes output data to the output_sender file descriptor.
//
// How is data sent via file descriptors?
// - Input data:
//   - Sent as a sequence of pointers to C Data Arrow arrays.
//   - Each pointer to an Arrow array is serialized as a byte array.
//   - The byte array is written to the input_receiver file descriptor.
// - Output data:
//   - Sent as a sequence of pointers to C Data Arrow arrays and schemas.
//   - Each pointer to an Arrow array is serialized as a byte array.
//   - Each pointer to an Arrow schema is serialized as a byte array.
//   - The byte arrays are written to the output_sender file descriptor.
//
// example:
// input: [ArrowArray*] [ArrowArray*] [ArrowArray*] ...
// output: [ArrowArray* ArrowSchema*] [ArrowArray* ArrowSchema*] [ArrowArray* ArrowSchema*] ...
async fn entrypoint(mut query_input_rx: Receiver<Query>) -> anyhow::Result<()> {
    eprintln!("zap: hello, world!");

    while let Some(query) = query_input_rx.recv().await {
        tokio::spawn(
            async move {
                let input_receiver = MessagePipe::<InputMessage>::try_from(query.input_receiver)
                    .expect("input receiver");
                let output_sender = MessagePipe::<OutputMessage>::try_from(query.output_sender)
                    .expect("output sender");

                let ctx = SessionContext::new();

                let (internal_input_tx, internal_input_rx) =
                    channel::<Result<RecordBatch, DataFusionError>>(1);

                let schema = Arc::new(query.schema);

                let stream = ReceiverStream::new(internal_input_rx);
                let stream = RecordBatchStreamAdapter::new(schema.clone(), stream);
                let stream: SendableRecordBatchStream = Box::pin(stream);
                let stream = OneShotStreamWrapper::from(stream);

                let provider = table::OneShotStreamProvider {
                    schema: schema.clone(),
                    stream: Arc::new(stream),
                };

                ctx.register_table("data", Arc::new(provider))
                    .expect("register table");

                let data_type = DataType::Struct(schema.fields().clone());

                tokio::spawn(
                    async move {
                        let mut index = 0;
                        while let Some(input) = input_receiver.recv().await.transpose() {
                            let input = input.expect("input");
                            let array = unsafe {
                                let ptr = input.array as *mut FFI_ArrowArray;
                                FFI_ArrowArray::from_raw(ptr)
                            };

                            let data = unsafe {
                                from_ffi_and_data_type(array, data_type.clone()).expect("from ffi")
                            };
                            let array = StructArray::from(data);
                            let record = RecordBatch::from(array);
                            let _ = internal_input_tx.send(Ok(record)).await;
                            eprintln!("zap: received input {}", index);
                            index += 1;
                        }
                    }
                    .then(|_| async {
                        eprintln!("zap: input stream closed");
                    }),
                );

                let df = ctx.sql(&query.sql).await.expect("sql");
                let mut stream = df.execute_stream().await.expect("execute stream");

                let mut index = 0;

                while let Some(batch) = stream.next().await {
                    let batch = batch.expect("batch");
                    let data = StructArray::from(batch).into_data();
                    let (array, schema) = to_ffi(&data).expect("to ffi");

                    let array_ptr = Box::into_raw(Box::new(array));
                    let schema_ptr = Box::into_raw(Box::new(schema));

                    let message = OutputMessage {
                        array: array_ptr as *mut std::ffi::c_void,
                        schema: schema_ptr as *mut std::ffi::c_void,
                    };

                    match output_sender.send(message).await {
                        Ok(_) => {
                            eprintln!("zap: sent output {}", index);
                            index += 1;
                            continue;
                        }
                        Err(contract::SendError(message, err)) => {
                            unsafe {
                                let _ = Box::from_raw(message.array as *mut FFI_ArrowArray);
                                let _ = Box::from_raw(message.schema as *mut FFI_ArrowSchema);
                            }

                            if let Some(err) = err {
                                panic!("send error: {}", err);
                            } else {
                                // EOF
                                break;
                            }
                        }
                    }
                }
            }
            .then(|_| async {
                eprintln!("zap: output stream closed");
            }),
        );
    }

    eprintln!("zap: goodbye, world!");

    Ok(())
}

/// Start the executor and return a pointer to the instance. The caller is
/// responsible for stopping the executor and freeing the memory when it is no
/// longer needed by calling [[zap_stop].
#[no_mangle]
pub extern "C" fn zap_start() -> *mut std::ffi::c_void {
    let instance = Instance::new(1);
    Box::into_raw(Box::new(instance)) as *mut std::ffi::c_void
}

/// Submit a query to the executor. The caller needs to provide:
/// - a pointer to the executor instance returned by [zap_start],
/// - a pointer to a null-terminated string containing the SQL query,
/// - a pointer to an FFI_ArrowSchema value describing the input schema,
/// - a file descriptor for reading input data.
/// The function returns a file descriptor for writing output data.
#[no_mangle]
pub extern "C" fn zap_query(
    ptr: *mut std::ffi::c_void,
    sql: *const std::ffi::c_char,
    schema: *mut std::ffi::c_void,
    input_receiver: RawFd,
) -> RawFd {
    // SAFETY: ptr is assumed to be valid pointer to a ZapInstance value.
    let instance = unsafe { &*(ptr as *mut Instance) };

    // SAFETY: sql is assumed to be valid pointer to a null-terminated string.
    let sql = unsafe { std::ffi::CStr::from_ptr(sql) }
        .to_str()
        .expect("sql")
        .to_string();

    // SAFETY: schema is assumed to be valid pointer to an FFI_ArrowSchema value.
    let schema = unsafe { FFI_ArrowSchema::from_raw(schema as *mut FFI_ArrowSchema) };
    let schema = Schema::try_from(&schema).expect("schema");

    let [output_receiver, output_sender] = pipe::new_raw().expect("pipe");

    // SAFETY: input_receiver and output_sender are valid file descriptors.
    let input_receiver = unsafe { File::from_raw_fd(input_receiver) };
    let output_sender = unsafe { File::from_raw_fd(output_sender) };

    let query = Query {
        sql,
        schema,
        input_receiver,
        output_sender,
    };

    instance.query(query).expect("submit query");
    output_receiver
}

/// Stop the executor and clean up resources. This function will block until the
/// executor has stopped and the thread has been joined.
#[no_mangle]
pub extern "C" fn zap_stop(ptr: *mut std::ffi::c_void) {
    // SAFETY: ptr is assumed to be valid pointer to a ZapInstance value.
    let instance = unsafe { Box::from_raw(ptr as *mut Instance) };
    instance.stop()
}

/// Create a non-blocking pipe and return the file descriptors. The caller is
/// responsible for closing the file descriptors when they are no longer needed.
#[no_mangle]
pub extern "C" fn zap_pipe(fds: *mut RawFd) -> std::ffi::c_int {
    let [input_receiver, output_sender] = match pipe::new_raw() {
        Ok(fds) => fds,
        Err(err) => return err.raw_os_error().unwrap_or(-1),
    };

    // SAFETY: fds is assumed to be valid pointer to an array of two c_int.
    unsafe {
        *fds = input_receiver;
        *fds.add(1) = output_sender;
        0
    }
}

#[cfg(test)]
mod test {
    use crate::Instance;

    #[test]
    fn it_executes_a_query() {
        let instance = Instance::new(1);

        instance.stop();
    }
}
