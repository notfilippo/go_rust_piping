use std::error::Error;
use std::fs::File;
use std::io::{Read, Write};
use std::{fmt, io};

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use tokio::io::unix::AsyncFd;

/// The input message sent from the client to the executor.
/// The `array` field is a valid pointer to an [arrow::ffi::Ffi_ArrowArray].
#[repr(C)]
pub struct InputMessage {
    pub(crate) array: FFI_ArrowArray,
}

unsafe impl Send for InputMessage {}
unsafe impl Sync for InputMessage {}

impl Default for InputMessage {
    fn default() -> Self {
        Self {
            array: FFI_ArrowArray::empty(),
        }
    }
}

/// The output message sent from the executor to the client.
/// The `array` field is a valid pointer to an [arrow::ffi::Ffi_ArrowArray].
/// The `schema` field is a valid pointer to an [arrow::ffi::Ffi_ArrowSchema].
#[repr(C)]
pub struct OutputMessage {
    pub(crate) array: FFI_ArrowArray,
    pub(crate) schema: FFI_ArrowSchema
}

unsafe impl Send for OutputMessage {}
unsafe impl Sync for OutputMessage {}

impl Default for OutputMessage {
    fn default() -> Self {
        Self {
            array: FFI_ArrowArray::empty(),
            schema: FFI_ArrowSchema::empty(),
        }
    }
}

/// Utility function to convert a reference to a type to a slice of bytes
/// without copying the data.
pub(crate) unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    core::slice::from_raw_parts((p as *const T) as *const u8, size_of::<T>())
}

/// Utility function to convert a mutable reference to a type to a mutable slice of bytes
/// without copying the data.
pub(crate) unsafe fn any_as_u8_slice_mut<T: Sized>(p: &mut T) -> &mut [u8] {
    core::slice::from_raw_parts_mut((p as *const T) as *mut u8, size_of::<T>())
}

pub struct MessagePipe<T> {
    file: AsyncFd<File>,
    marker: std::marker::PhantomData<T>,
}

impl<T> TryFrom<File> for MessagePipe<T> {
    type Error = io::Error;

    fn try_from(file: File) -> Result<Self, Self::Error> {
        Ok(Self {
            file: AsyncFd::new(file)?,
            marker: std::marker::PhantomData,
        })
    }
}

pub struct SendError<T>(pub T, pub Option<io::Error>);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError").finish_non_exhaustive()
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "channel closed")
    }
}

impl<T> Error for SendError<T> {}

impl<T: Default> MessagePipe<T> {
    pub async fn send(&self, msg: T) -> Result<T, SendError<T>> {
        let mut buf = unsafe { any_as_u8_slice(&msg) };

        while !buf.is_empty() {
            let guard = self.file.writable().await;
            match guard {
                Ok(guard) => match guard.get_inner().write(buf) {
                    Ok(0) => return Err(SendError(msg, None)),
                    Ok(n) => buf = &buf[n..],
                    Err(ref e) if is_retry(e) => continue,
                    Err(e) => return Err(SendError(msg, Some(e))),
                },
                Err(e) => return Err(SendError(msg, Some(e))),
            }
        }

        Ok(msg)
    }

    pub async fn recv(&self) -> io::Result<Option<T>> {
        let mut msg = T::default();
        let mut buf = unsafe { any_as_u8_slice_mut(&mut msg) };

        while !buf.is_empty() {
            let guard = self.file.readable().await?;
            match guard.get_inner().read(buf) {
                Ok(0) => return Ok(None),
                Ok(n) => buf = &mut buf[n..],
                Err(ref e) if is_retry(e) => continue,
                Err(e) => return Err(e),
            }
        }

        Ok(Some(msg))
    }
}

fn is_retry(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::Interrupted
}
