use std::ffi::c_int;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::slice;
use std::str;

use libsqlite3_sys::{
    sqlite3_value, sqlite3_value_bytes, sqlite3_value_dup, sqlite3_value_free, sqlite3_value_text,
    sqlite3_value_type, SQLITE_BLOB, SQLITE_FLOAT, SQLITE_INTEGER, SQLITE_NULL, SQLITE_TEXT,
};

use crate::*;

/// A reference to a SQLite value.
#[derive(Clone, Copy, Debug)]
pub struct UnprotectedValue<'a> {
    ptr: NonNull<sqlite3_value>,
    phantom_ref: PhantomData<&'a sqlite3_value>,
}

impl<'a> UnprotectedValue<'a> {
    #[inline]
    pub(crate) unsafe fn new(ptr: NonNull<sqlite3_value>) -> Self {
        UnprotectedValue {
            ptr,
            phantom_ref: PhantomData,
        }
    }
}

impl<'a> Value for UnprotectedValue<'a> {
    fn as_ptr(&self) -> *mut sqlite3_value {
        self.ptr.as_ptr()
    }
}

unsafe impl<'a> Send for UnprotectedValue<'a> {}
unsafe impl<'a> Sync for UnprotectedValue<'a> {}

/// A mutex-protected reference to a SQLite value.
#[repr(transparent)]
#[derive(Debug)]
pub struct ProtectedValue<'a> {
    ptr: NonNull<sqlite3_value>,
    phantom_ref: PhantomData<&'a mut sqlite3_value>,
}

impl<'a> ProtectedValue<'a> {
    #[inline(always)]
    pub(crate) unsafe fn new(ptr: NonNull<sqlite3_value>) -> Self {
        ProtectedValue {
            ptr,
            phantom_ref: PhantomData,
        }
    }

    /// Reports whether the value represents `NULL`.
    #[inline]
    pub fn is_null(&self) -> bool {
        (unsafe { sqlite3_value_type(self.as_ptr()) }) == SQLITE_NULL
    }

    /// Returns the type of the value.
    pub fn r#type(&self) -> ColumnType {
        ColumnType::from_int(unsafe { sqlite3_value_type(self.ptr.as_ptr()) }).unwrap()
    }

    pub fn text<'b>(&'b mut self) -> Result<&'b str, ColumnTextError<'b>> {
        let bytes = unsafe {
            let ptr = sqlite3_value_text(self.ptr.as_ptr());
            if ptr.is_null() {
                return Ok("");
            }
            let n = sqlite3_value_bytes(self.ptr.as_ptr());
            slice::from_raw_parts(ptr, n as usize)
        };
        str::from_utf8(bytes).map_err(|err| ColumnTextError::new(bytes, err))
    }
}

impl<'a> Value for ProtectedValue<'a> {
    fn as_ptr(&self) -> *mut sqlite3_value {
        self.ptr.as_ptr()
    }
}

/// An owned SQLite value.
#[repr(transparent)]
#[derive(Debug)]
pub struct DupValue {
    ptr: NonNull<sqlite3_value>,
}

impl DupValue {
    /// Borrows the value as a protected value.
    #[inline]
    pub fn as_mut<'a>(&'a mut self) -> ProtectedValue<'a> {
        unsafe { ProtectedValue::new(self.ptr) }
    }
}

impl Value for DupValue {
    fn as_ptr(&self) -> *mut sqlite3_value {
        self.ptr.as_ptr()
    }
}

impl Clone for DupValue {
    fn clone(&self) -> Self {
        self.dup()
    }
}

impl Drop for DupValue {
    fn drop(&mut self) {
        unsafe { sqlite3_value_free(self.ptr.as_ptr()) }
    }
}

/// Trait that is one of [`UnprotectedValue`], [`ProtectedValue`], or [`DupValue`].
/// This trait is sealed and cannot be implemented for types outside this crate.
pub trait Value: private::Sealed {
    /// Make an owned copy of the value.
    fn dup(&self) -> DupValue {
        DupValue {
            ptr: NonNull::new(unsafe { sqlite3_value_dup(self.as_ptr()) }).expect("out of memory"),
        }
    }

    #[doc(hidden)]
    fn as_ptr(&self) -> *mut sqlite3_value;
}

mod private {
    pub trait Sealed {}

    impl<'a> Sealed for super::UnprotectedValue<'a> {}
    impl<'a> Sealed for super::ProtectedValue<'a> {}
    impl Sealed for super::DupValue {}
}

/// Enumeration of the SQLite fundamental datatypes.
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ColumnType {
    /// 64-bit signed integer.
    Integer = SQLITE_INTEGER as u8,
    /// 64-bit IEEE floating point number.
    Float = SQLITE_FLOAT as u8,
    /// String.
    Text = SQLITE_TEXT as u8,
    /// BLOB.
    Blob = SQLITE_BLOB as u8,
    /// NULL.
    Null = SQLITE_NULL as u8,
}

impl ColumnType {
    pub(crate) fn from_int(i: c_int) -> Option<ColumnType> {
        match i {
            SQLITE_INTEGER => Some(ColumnType::Integer),
            SQLITE_FLOAT => Some(ColumnType::Float),
            SQLITE_TEXT => Some(ColumnType::Text),
            SQLITE_BLOB => Some(ColumnType::Blob),
            SQLITE_NULL => Some(ColumnType::Null),
            _ => None,
        }
    }

    /// Reports whether the column type is equal to [`ColumnType::Null`].
    #[inline]
    pub fn is_null(self) -> bool {
        self == ColumnType::Null
    }
}

impl Default for ColumnType {
    fn default() -> Self {
        ColumnType::Null
    }
}
