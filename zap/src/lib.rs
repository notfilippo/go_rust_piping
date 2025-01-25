use std::{
    fs::File,
    io::{self, Read, Write},
    os::fd::{FromRawFd, RawFd},
    sync::Arc,
    thread::JoinHandle,
};

use arrow::{
    array::{Array, RecordBatch, StructArray},
    datatypes::{DataType, Schema},
    ffi::{from_ffi_and_data_type, to_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};
use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter, prelude::SessionContext,
};
use futures::{FutureExt, StreamExt};
use table::StreamWrapper;
use tokio::{
    io::unix::AsyncFd,
    sync::mpsc::{channel, Receiver, Sender},
};
use tokio_stream::wrappers::ReceiverStream;

mod pipe;
mod table;

use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

struct Query {
    sql: String,
    schema: Schema,
    input_receiver: File,
    output_sender: File,
}

struct ZapInstance {
    query_input_tx: Sender<Query>,
    join_handle: JoinHandle<()>,
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
// How is the data being sent via file descriptors?
// - The input data is sent as a sequence of pointers to C Data Arrow arrays:
//   - The pointer to the Arrow array is serialized as a byte array.
//   - The byte array is written to the input_receiver file descriptor.
// - The output data is sent as a sequence of pointers to C Data Arrow arrays and schemas:
//   - The pointer to the Arrow array is serialized as a byte array.
//   - The pointer to the Arrow schema is serialized as a byte array.
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
                let input_receiver = AsyncFd::new(query.input_receiver).expect("input receiver");
                let output_sender = AsyncFd::new(query.output_sender).expect("output sender");

                let ctx = SessionContext::new();

                let (internal_input_tx, internal_input_rx) =
                    channel::<Result<RecordBatch, DataFusionError>>(1);

                let schema = Arc::new(query.schema);

                let stream = ReceiverStream::new(internal_input_rx);
                let stream = RecordBatchStreamAdapter::new(schema.clone(), stream);
                let stream: SendableRecordBatchStream = Box::pin(stream);
                let stream = StreamWrapper::from(stream);

                let provider = table::OneShotStreamProvider {
                    schema: schema.clone(),
                    stream: Arc::new(stream),
                };

                ctx.register_table("data", Arc::new(provider))
                    .expect("register table");

                let data_type = DataType::Struct(schema.fields().clone());

                tokio::spawn(
                    async move {
                        let mut buf = [0; std::mem::size_of::<*mut FFI_ArrowArray>()];

                        loop {
                            let input = input_receiver.readable().await.expect("input");
                            match input.get_inner().read(&mut buf) {
                                Ok(size) => {
                                    if size == 0 {
                                        break;
                                    }

                                    assert_eq!(size, buf.len());
                                    let array = unsafe {
                                        let ptr = *(buf.as_ptr() as *const *mut FFI_ArrowArray);
                                        FFI_ArrowArray::from_raw(ptr)
                                    };
                                    let data = unsafe {
                                        from_ffi_and_data_type(array, data_type.clone())
                                            .expect("from ffi")
                                    };
                                    let array = StructArray::from(data);
                                    let record = RecordBatch::from(array);
                                    let _ = internal_input_tx.send(Ok(record)).await;
                                }
                                Err(ref err)
                                    if err.kind() == io::ErrorKind::WouldBlock
                                        || err.kind() == io::ErrorKind::Interrupted =>
                                {
                                    continue;
                                }
                                Err(e) => {
                                    eprintln!("zap: input read error: {:?}", e);
                                    break;
                                }
                            }
                        }
                    }
                    .then(|_| async {
                        eprintln!("zap: input stream closed");
                    }),
                );

                let df = ctx.sql(&query.sql).await.expect("sql");
                let mut stream = df.execute_stream().await.expect("execute stream");

                while let Some(batch) = stream.next().await {
                    let batch = batch.expect("batch");
                    let data = StructArray::from(batch).into_data();
                    let (array, schema) = to_ffi(&data).expect("to ffi");

                    let mut buf = [0; std::mem::size_of::<*mut FFI_ArrowArray>()
                        + std::mem::size_of::<*mut FFI_ArrowSchema>()];
                    let array_ptr = Box::into_raw(Box::new(array));
                    let schema_ptr = Box::into_raw(Box::new(schema));

                    unsafe {
                        *(buf.as_mut_ptr() as *mut *mut FFI_ArrowArray) = array_ptr;
                        *(buf
                            .as_mut_ptr()
                            .add(std::mem::size_of::<*mut FFI_ArrowArray>())
                            as *mut *mut FFI_ArrowSchema) = schema_ptr;
                    }

                    loop {
                        let output = output_sender.writable().await.expect("output");
                        match output.get_inner().write(&buf) {
                            Ok(size) => {
                                assert_eq!(size, buf.len());
                                break;
                            }
                            Err(ref err)
                                if err.kind() == io::ErrorKind::WouldBlock
                                    || err.kind() == io::ErrorKind::Interrupted =>
                            {
                                continue;
                            }
                            Err(e) => {
                                eprintln!("zap: output write error: {:?}", e);
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

// Start the executor and return a pointer to the instance. The caller is
// responsible for stopping the executor and freeing the memory when it is no
// longer needed by calling [zap_stop].
#[no_mangle]
pub extern "C" fn zap_start() -> *mut std::ffi::c_void {
    let (query_input_tx, query_input_rx) = channel(1);

    let join_handle = std::thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(entrypoint(query_input_rx))
            .expect("executor start");
    });

    let instance = ZapInstance {
        query_input_tx,
        join_handle,
    };

    Box::into_raw(Box::new(instance)) as *mut std::ffi::c_void
}

// Submit a query to the executor. The caller needs to provide:
// - a pointer to the executor instance returned by [zap_start],
// - a pointer to a null-terminated string containing the SQL query,
// - a pointer to an FFI_ArrowSchema value describing the input schema,
// - a file descriptor for reading input data.
// The function returns a file descriptor for writing output data.
#[no_mangle]
pub extern "C" fn zap_query(
    ptr: *mut std::ffi::c_void,
    sql: *const std::ffi::c_char,
    schema: *mut std::ffi::c_void,
    input_receiver: RawFd,
) -> RawFd {
    // SAFETY: ptr is assumed to be valid pointer to a ZapInstance value.
    let instance = unsafe { &*(ptr as *mut ZapInstance) };

    // SAFETY: sql is assumed to be valid pointer to a null-terminated string.
    let sql = unsafe { std::ffi::CStr::from_ptr(sql) }
        .to_str()
        .expect("sql")
        .to_string();

    let [output_receiver, output_sender] = pipe::new_raw().expect("pipe");

    // SAFETY: input_receiver and output_sender are valid file descriptors.
    let input_receiver = unsafe { File::from_raw_fd(input_receiver) };
    let output_sender = unsafe { File::from_raw_fd(output_sender) };

    // SAFETY: schema is assumed to be valid pointer to an FFI_ArrowSchema value.
    let schema = unsafe { FFI_ArrowSchema::from_raw(schema as *mut FFI_ArrowSchema) };
    let schema = Schema::try_from(&schema).expect("schema");

    let query = Query {
        sql,
        schema,
        input_receiver,
        output_sender,
    };

    instance
        .query_input_tx
        .blocking_send(query)
        .expect("executor query");
    output_receiver
}

// Stop the executor and clean up resources. This function will block until the
// executor has stopped and the thread has been joined.
#[no_mangle]
pub extern "C" fn zap_stop(ptr: *mut std::ffi::c_void) {
    // SAFETY: ptr is assumed to be valid pointer to a ZapInstance value.
    let instance = unsafe { Box::from_raw(ptr as *mut ZapInstance) };
    drop(instance.query_input_tx);
    instance.join_handle.join().expect("executor stop");
}

// Create a non-blocking pipe and return the file descriptors. The caller is
// responsible for closing the file descriptors when they are no longer needed.
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
