use std::fs::File;
use std::os::fd::{FromRawFd, RawFd};
use std::sync::{Arc, Once};
use std::thread::JoinHandle;

use arrow::array::{Array, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Schema};
use arrow::ffi::{from_ffi_and_data_type, to_ffi, FFI_ArrowSchema};
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::{FutureExt, StreamExt};
use prost::Message;
use table::OneShotStreamWrapper;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

mod contract;
mod pipe;
mod table;

use tikv_jemallocator::Jemalloc;
use tokio::sync::mpsc::error::SendError;
use tokio_util::task::TaskTracker;

use crate::contract::{InputMessage, MessagePipe, OutputMessage};

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
static START: Once = Once::new();

/// A query to be executed by the executor.
struct Query {
    plan: datafusion_substrait::substrait::proto::Plan,
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
        log::trace!("zap: executor stopped");
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
// input: [ArrowArray] [ArrowArray] [ArrowArray] ...
// output: [ArrowArray ArrowSchema] [ArrowArray ArrowSchema] [ArrowArray ArrowSchema] ...
async fn entrypoint(mut query_input_rx: Receiver<Query>) -> anyhow::Result<()> {
    log::trace!("zap: hello, world!");

    let tracker = TaskTracker::new();

    while let Some(query) = query_input_rx.recv().await {
        tracker.spawn(async move {
            let input_receiver = MessagePipe::<InputMessage>::try_from(query.input_receiver)
                .expect("input receiver");
            let output_sender =
                MessagePipe::<OutputMessage>::try_from(query.output_sender).expect("output sender");

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

            let join_receiver = tokio::spawn(
                async move {
                    let mut index = 0;
                    while let Some(input) = input_receiver.recv().await.transpose() {
                        let input = input.expect("input");
                        let data = unsafe {
                            from_ffi_and_data_type(input.array, data_type.clone())
                                .expect("from ffi")
                        };

                        let array = StructArray::from(data);
                        let record = RecordBatch::from(array);
                        let _ = internal_input_tx.send(Ok(record)).await;
                        log::trace!("zap: received input {}", index);
                        index += 1;
                    }
                }
                .then(|_| async {
                    log::trace!("zap: input stream closed");
                }),
            );

            let join_sender = tokio::spawn(
                async move {
                    let plan = datafusion_substrait::logical_plan::consumer::from_substrait_plan(
                        &ctx.state(),
                        &query.plan,
                    )
                    .await
                    .expect("logical plan");

                    let df = ctx.execute_logical_plan(plan).await.expect("dataframe");
                    let mut stream = df.execute_stream().await.expect("execute stream");

                    let mut index = 0;

                    while let Some(batch) = stream.next().await {
                        let batch = batch.expect("batch");
                        let data = StructArray::from(batch).into_data();
                        let (array, schema) = to_ffi(&data).expect("to ffi");

                        let message = OutputMessage { array, schema };

                        match output_sender.send(message).await {
                            Ok(msg) => {
                                std::mem::forget(msg.array);
                                std::mem::forget(msg.schema);

                                log::trace!("zap: sent output {}", index);
                                index += 1;
                                continue;
                            }
                            Err(contract::SendError(_msg, err)) => {
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
                    log::trace!("zap: output stream closed");
                }),
            );

            // Wait for the input and output streams to complete.
            join_receiver.await.expect("join receiver");
            join_sender.await.expect("join sender");
        });
    }

    tracker.close();
    tracker.wait().await;

    log::trace!("zap: goodbye, world!");

    Ok(())
}

/// Start the executor and return a pointer to the instance. The caller is
/// responsible for stopping the executor and freeing the memory when it is no
/// longer needed by calling [zap_stop].
#[no_mangle]
pub extern "C" fn zap_start() -> *mut std::ffi::c_void {
    START.call_once(env_logger::init);

    let instance = Instance::new(1);
    Box::into_raw(Box::new(instance)) as *mut std::ffi::c_void
}

/// Submit a query to the executor. The caller needs to provide:
/// - a pointer to the executor instance returned by [zap_start],
/// - a pointer to a null-terminated string containing the SQL query,
/// - a pointer to an FFI_ArrowSchema value describing the input schema,
/// - a file descriptor for reading input data.
///
/// The function returns a file descriptor for writing output data.
///
/// # Safety
///
/// The caller must ensure that the pointers are valid and that the file
/// descriptors are valid.
#[no_mangle]
pub unsafe extern "C" fn zap_query(
    ptr: *mut std::ffi::c_void,
    plan_bytes: *const std::ffi::c_uchar,
    plan_len: usize,
    schema: *const std::ffi::c_void,
    input_receiver: RawFd,
) -> RawFd {
    // SAFETY: ptr is assumed to be valid pointer to a ZapInstance value.
    let instance = unsafe { &*(ptr as *mut Instance) };

    // SAFETY: plan_bytes is assumed to be valid pointer to a buffer of plan_len bytes.
    let plan_bytes = unsafe { std::slice::from_raw_parts(plan_bytes, plan_len) };
    let plan =
        datafusion_substrait::substrait::proto::Plan::decode(plan_bytes).expect("decoded plan");

    // SAFETY: schema is assumed to be valid pointer to an FFI_ArrowSchema value.
    let schema: &FFI_ArrowSchema = unsafe { &*(schema as *const FFI_ArrowSchema) };
    let schema = Schema::try_from(schema).expect("schema");

    let [output_receiver, output_sender] = pipe::new_raw().expect("pipe");

    // SAFETY: input_receiver and output_sender are valid file descriptors.
    let input_receiver = unsafe { File::from_raw_fd(input_receiver) };
    let output_sender = unsafe { File::from_raw_fd(output_sender) };

    let query = Query {
        plan,
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
///
/// # Safety
///
/// The caller must ensure that fds is a valid pointer to an array of two c_int.
#[no_mangle]
pub unsafe extern "C" fn zap_pipe(fds: *mut RawFd) -> std::ffi::c_int {
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

/// Create a substrait plan from a SQL query and an Arrow Schema. The serialized
/// plan will be written in the `out_bytes` buffer and the length of the buffer
/// will be written in the `out_len` pointer. The caller is responsible for the
/// memory allocated by this function and will need to call [[zap_plan_drop]] to
/// free it.
///
/// # Safety
///
/// The caller must ensure that the pointers of both the input SQL query and the
/// Arrow Schema are valid. The caller must also ensure that the `out_bytes` and
/// `out_len` pointers are valid.
#[no_mangle]
pub unsafe extern "C" fn zap_plan(
    sql: *const std::ffi::c_char,
    schema: *const std::ffi::c_void,
    out_bytes: *mut *mut std::ffi::c_uchar,
    out_len: *mut usize,
) {
    // SAFETY: sql is assumed to be valid pointer to a null-terminated string.
    let sql = unsafe { std::ffi::CStr::from_ptr(sql) }
        .to_str()
        .expect("sql")
        .to_string();

    // SAFETY: schema is assumed to be valid pointer to an FFI_ArrowSchema value.
    let schema: &FFI_ArrowSchema = unsafe { &*(schema as *const FFI_ArrowSchema) };
    let schema = Schema::try_from(schema).expect("schema");

    let ctx = SessionContext::new();

    let table = MemTable::try_new(schema.into(), vec![]).expect("mem table");

    let _ = ctx.register_table("data", Arc::new(table));

    let df = futures::executor::block_on(ctx.sql(&sql)).expect("dataframe");
    let plan = df.into_optimized_plan().expect("optimized plan");
    let plan = datafusion_substrait::logical_plan::producer::to_substrait_plan(&plan, &ctx.state())
        .expect("substrait plan");
    let mut buf = Vec::new();
    plan.encode(&mut buf).expect("encoded plan");

    // SAFETY: out_bytes and out_len are assumed to be valid pointers.
    unsafe {
        let buf = buf.into_boxed_slice();
        *out_len = buf.len();
        *out_bytes = Box::into_raw(buf) as *mut std::ffi::c_uchar;
    }
}

/// Free the memory allocated by the `zap_plan` function.
///
/// # Safety
///
/// The caller must ensure that the `bytes` pointer is valid and that the `len`
/// value is correct.
#[no_mangle]
pub unsafe extern "C" fn zap_plan_drop(bytes: *mut std::ffi::c_uchar, len: usize) {
    // SAFETY: bytes is assumed to be valid pointer to a buffer of len bytes.
    unsafe {
        let _ = Box::from_raw(std::slice::from_raw_parts_mut(bytes, len));
    }
}
