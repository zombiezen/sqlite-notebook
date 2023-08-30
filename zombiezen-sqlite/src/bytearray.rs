use std::ffi::c_void;
use std::ptr::NonNull;

use dashmap::DashMap;
use lazy_static::lazy_static;

lazy_static! {
    static ref MAP: DashMap<ArrayPointerId, ArrayPointer> = DashMap::new();
}

/// Unwrap a `Vec<u8>` into a C byte array.
/// The caller is responsible for releasing the memory with [`destroy`].
pub(crate) fn new(v: Vec<u8>) -> (NonNull<c_void>, usize) {
    let n = v.len();
    let slice_ptr = Box::into_raw(v.into_boxed_slice());
    let slice_ptr = unsafe { NonNull::new_unchecked(slice_ptr) };
    let void_ptr = slice_ptr.cast::<c_void>();
    MAP.insert(ArrayPointerId(void_ptr), ArrayPointer(slice_ptr));
    (void_ptr, n)
}

/// Release the memory of a byte array previously created by [`new`].
pub(crate) extern "C" fn destroy(ptr: *mut c_void) {
    if let Some((_, ArrayPointer(ptr))) =
        NonNull::new(ptr).and_then(|ptr| MAP.remove(&ArrayPointerId(ptr)))
    {
        drop(unsafe { Box::from_raw(ptr.as_ptr()) });
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
struct ArrayPointerId(NonNull<c_void>);

unsafe impl Send for ArrayPointerId {}
unsafe impl Sync for ArrayPointerId {}

#[derive(Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
struct ArrayPointer(NonNull<[u8]>);

unsafe impl Send for ArrayPointer {}
unsafe impl Sync for ArrayPointer {}
