//! Low-level binding to SQLite library.

use std::borrow::Cow;
use std::ffi::CStr;
use std::fmt::Debug;
use std::str;

use libsqlite3_sys::sqlite3_libversion;

mod auth;
mod bytearray;
pub mod column_metadata;
mod connection;
mod function;
mod glob;
mod quote;
mod result;
mod statement;
mod value;

pub use auth::*;
pub use connection::*;
pub use function::*;
pub use glob::*;
pub use quote::*;
pub use result::*;
pub use statement::*;
pub use value::*;

/// Extension trait for `Result<&str, TextError>`.
pub trait ResultExt<'a> {
    /// Converts the result into a string by replacing invalid UTF-8
    /// with the Unicode replacement character if needed.
    fn to_string_lossy(self) -> Cow<'a, str>;
}

impl<'a> ResultExt<'a> for Result<&'a str, TextError<'a>> {
    fn to_string_lossy(self) -> Cow<'a, str> {
        self.map_or_else(|err| String::from_utf8_lossy(err.as_bytes()), |s| s.into())
    }
}

/// Returns the version of SQLite as a string.
pub fn version() -> &'static str {
    let s = unsafe { CStr::from_ptr(sqlite3_libversion()) };
    std::str::from_utf8(s.to_bytes()).unwrap()
}
