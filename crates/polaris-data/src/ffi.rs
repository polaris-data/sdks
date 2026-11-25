use std::{
    ffi::c_void,
    ptr::{self, NonNull},
    slice,
    sync::Mutex,
};

use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{PolarisError, errors::Fault, generated::TableDataMessage};

#[repr(C)]
#[derive(Clone, Copy)]
struct RawBuffer {
    ptr: *mut u8,
    len: usize,
}

struct Buffer(RawBuffer);

unsafe extern "C" {
    fn polaris_v1_validate(ptr: *const u8, len: usize) -> RawBuffer;
    fn polaris_v1_request(ptr: *const u8, len: usize) -> RawBuffer;
    fn polaris_v1_free(buffer: RawBuffer);
    fn polaris_v1_cursor_create() -> *mut c_void;
    fn polaris_v1_cursor_remember(cursor: *mut c_void, data: *const u8, len: usize) -> bool;
    fn polaris_v1_cursor_destroy(cursor: *mut c_void);
    fn polaris_v1_stream_create(ptr: *const u8, len: usize, out: *mut *mut c_void) -> RawBuffer;
    fn polaris_v1_stream_next(stream: *mut c_void) -> RawBuffer;
    fn polaris_v1_stream_cancel(stream: *mut c_void);
    fn polaris_v1_stream_destroy(stream: *mut c_void);
}

impl Drop for Buffer {
    fn drop(&mut self) {
        unsafe { polaris_v1_free(self.0) }
    }
}

impl Buffer {
    fn decode<T: DeserializeOwned>(&self) -> Result<T, PolarisError> {
        #[derive(Deserialize)]
        #[serde(rename_all = "lowercase")]
        enum Reply<T> {
            Ok(T),
            Error(Fault),
        }
        if self.0.ptr.is_null() {
            return Err(PolarisError::Request(
                "Native response allocation failed".into(),
            ));
        }
        let data = unsafe { slice::from_raw_parts(self.0.ptr, self.0.len) };
        match serde_json::from_slice(data)
            .map_err(|error| PolarisError::InvalidResponse(error.to_string()))?
        {
            Reply::Ok(value) => Ok(value),
            Reply::Error(fault) => Err(fault.into()),
        }
    }
}

pub(crate) fn request<T: DeserializeOwned>(input: &[u8]) -> Result<T, PolarisError> {
    Buffer(unsafe { polaris_v1_request(input.as_ptr(), input.len()) }).decode()
}

pub(crate) fn validate(input: &[u8]) -> Result<(), PolarisError> {
    Buffer(unsafe { polaris_v1_validate(input.as_ptr(), input.len()) }).decode()
}

pub(crate) struct CursorSet(NonNull<c_void>);

// A tracker has one mutable owner. Zig uses the thread-safe libc allocator.
unsafe impl Send for CursorSet {}

impl CursorSet {
    pub(crate) fn new(cursor: Option<&str>) -> Result<Self, PolarisError> {
        let mut tracker = NonNull::new(unsafe { polaris_v1_cursor_create() })
            .map(Self)
            .ok_or_else(|| PolarisError::Request("Could not allocate cursor tracker".into()))?;
        if let Some(cursor) = cursor {
            tracker.remember(cursor)?;
        }
        Ok(tracker)
    }

    pub(crate) fn remember(&mut self, cursor: &str) -> Result<(), PolarisError> {
        if unsafe { polaris_v1_cursor_remember(self.0.as_ptr(), cursor.as_ptr(), cursor.len()) } {
            Ok(())
        } else {
            Err(PolarisError::InvalidResponse(
                "Data page repeated a cursor".into(),
            ))
        }
    }
}

impl Drop for CursorSet {
    fn drop(&mut self) {
        unsafe { polaris_v1_cursor_destroy(self.0.as_ptr()) }
    }
}

pub(crate) struct Stream {
    ptr: NonNull<c_void>,
    read: Mutex<()>,
}

// Native cancellation is thread-safe. Serialize reads and keep the handle
// alive through each call, including when the owning iterator is dropped.
unsafe impl Send for Stream {}
unsafe impl Sync for Stream {}

impl Stream {
    pub(crate) fn new(input: &[u8]) -> Result<Self, PolarisError> {
        let mut ptr = ptr::null_mut();
        let result =
            Buffer(unsafe { polaris_v1_stream_create(input.as_ptr(), input.len(), &mut ptr) });
        let stream = NonNull::new(ptr).map(|ptr| Self {
            ptr,
            read: Mutex::new(()),
        });
        result.decode::<Value>()?;
        stream.ok_or_else(|| PolarisError::StreamConnection("Could not allocate stream".into()))
    }

    pub(crate) fn next(&self) -> Result<Option<TableDataMessage>, PolarisError> {
        let _guard = self.read.lock().expect("stream read lock");
        Buffer(unsafe { polaris_v1_stream_next(self.ptr.as_ptr()) }).decode()
    }

    pub(crate) fn cancel(&self) {
        unsafe { polaris_v1_stream_cancel(self.ptr.as_ptr()) }
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        unsafe { polaris_v1_stream_destroy(self.ptr.as_ptr()) }
    }
}
