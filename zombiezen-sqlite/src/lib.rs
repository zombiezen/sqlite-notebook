use std::ffi::{c_char, c_int, c_void, CStr};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self, null, NonNull};
use std::str::{self, Utf8Error};
use std::{fmt, slice};

mod result;

pub use result::*;

use bitflags::bitflags;
use libsqlite3_sys::{
    sqlite3, sqlite3_clear_bindings, sqlite3_close, sqlite3_column_blob, sqlite3_column_bytes,
    sqlite3_column_count, sqlite3_column_double, sqlite3_column_int64, sqlite3_column_name,
    sqlite3_column_text, sqlite3_column_type, sqlite3_complete, sqlite3_db_config,
    sqlite3_db_handle, sqlite3_finalize, sqlite3_libversion, sqlite3_open_v2, sqlite3_prepare_v2,
    sqlite3_reset, sqlite3_step, sqlite3_stmt, SQLITE_BLOB, SQLITE_DBCONFIG_DQS_DDL,
    SQLITE_DBCONFIG_DQS_DML, SQLITE_DONE, SQLITE_FLOAT, SQLITE_INTEGER, SQLITE_NOMEM, SQLITE_NULL,
    SQLITE_OPEN_CREATE, SQLITE_OPEN_FULLMUTEX, SQLITE_OPEN_MEMORY, SQLITE_OPEN_NOMUTEX,
    SQLITE_OPEN_READONLY, SQLITE_OPEN_READWRITE, SQLITE_OPEN_SHAREDCACHE, SQLITE_OPEN_URI,
    SQLITE_ROW, SQLITE_TEXT,
};

#[repr(transparent)]
#[derive(Debug)]
pub struct Connection(NonNull<sqlite3>);

impl Connection {
    pub fn open(filename: impl AsRef<CStr>, flags: OpenFlags) -> Result<Connection> {
        let mut db = MaybeUninit::uninit();
        let rc = ResultCode(unsafe {
            sqlite3_open_v2(
                filename.as_ref().as_ptr(),
                db.as_mut_ptr(),
                flags.bits() as c_int,
                null(),
            )
        });
        let db = match NonNull::new(unsafe { db.assume_init() }) {
            Some(db) => db,
            None => return Err(ResultCode::NOMEM.to_error().unwrap()),
        };
        if rc != ResultCode::OK {
            let err = Error::get(db).expect("Result was successful, but not SQLITE_OK");
            unsafe {
                sqlite3_close(db.as_ptr());
            }
            return Err(err);
        }
        let conn = Connection(db);
        unsafe {
            sqlite3_db_config(
                db.as_ptr(),
                SQLITE_DBCONFIG_DQS_DML,
                0 as c_int,
                ptr::null::<c_void>(),
            );
            sqlite3_db_config(
                db.as_ptr(),
                SQLITE_DBCONFIG_DQS_DDL,
                0 as c_int,
                ptr::null::<c_void>(),
            );
        }
        Ok(conn)
    }

    fn error(&self) -> Option<Error> {
        Error::get(self.0)
    }

    pub fn prepare<'c, 's>(&'c self, sql: &'s str) -> Result<Option<(Statement<'c>, &'s str)>> {
        let n_byte: c_int = sql
            .len()
            .try_into()
            .map_err(|_| ResultCode::TOOBIG.to_error().unwrap())?;
        let mut stmt = MaybeUninit::uninit();
        let z_sql = sql.as_ptr() as *const c_char;
        let mut tail = MaybeUninit::uninit();
        let rc = ResultCode(unsafe {
            sqlite3_prepare_v2(
                self.0.as_ptr(),
                z_sql,
                n_byte,
                stmt.as_mut_ptr(),
                tail.as_mut_ptr(),
            )
        });
        if rc != ResultCode::OK {
            debug_assert!(unsafe { stmt.assume_init() }.is_null());
            return Err(self.error().unwrap());
        }
        Ok(NonNull::new(unsafe { stmt.assume_init() }).map(|ptr| {
            let n = unsafe { tail.assume_init().offset_from(z_sql) };
            let stmt = Statement {
                ptr,
                has_row: false,
                conn: PhantomData,
            };
            let tail = sql.split_at(n as usize).1;
            (stmt, tail)
        }))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe {
            assert_eq!(ResultCode(sqlite3_close(self.0.as_ptr())), ResultCode::OK);
        }
    }
}

#[derive(Debug)]
pub struct Statement<'c> {
    ptr: NonNull<sqlite3_stmt>,
    has_row: bool,
    conn: PhantomData<&'c Connection>,
}

impl<'c> Statement<'c> {
    fn error(&self) -> Option<Error> {
        let db = NonNull::new(unsafe { sqlite3_db_handle(self.ptr.as_ptr()) });
        db.and_then(|db| Error::get(db))
    }

    pub fn step(&mut self) -> Result<StepResult> {
        let rc = ResultCode(unsafe { sqlite3_step(self.ptr.as_ptr()) });
        match rc {
            ResultCode::ROW => {
                self.has_row = true;
                Ok(StepResult::Row)
            }
            ResultCode::DONE => {
                self.has_row = false;
                Ok(StepResult::Done)
            }
            _ => {
                self.has_row = false;
                Err(self.error().unwrap())
            }
        }
    }

    /// Resets the prepared statement to its initial state.
    pub fn reset(&mut self) -> Result<()> {
        self.has_row = false;
        let rc = ResultCode(unsafe { sqlite3_reset(self.ptr.as_ptr()) });
        match rc {
            ResultCode::OK => Ok(()),
            _ => Err(self.error().unwrap()),
        }
    }

    pub fn clear_bindings(&mut self) {
        unsafe {
            sqlite3_clear_bindings(self.ptr.as_ptr());
        }
    }

    pub fn column_count(&self) -> usize {
        (unsafe { sqlite3_column_count(self.ptr.as_ptr()) }) as usize
    }

    pub fn column_name(&self, i: usize) -> Option<String> {
        if i >= self.column_count() {
            return None;
        }
        // Unfortunately, as per the docs at
        // https://www.sqlite.org/c3ref/column_name.html,
        // "[t]he returned string pointer is valid [...]
        // until the next call to sqlite3_column_name() [...]
        // on the same column."
        unsafe {
            let s = sqlite3_column_name(self.ptr.as_ptr(), i as c_int);
            if s.is_null() {
                return None;
            }
            let s = CStr::from_ptr(s);
            s.to_str().ok().map(|s| s.to_string())
        }
    }

    pub fn column_type(&self, i: usize) -> Option<ColumnType> {
        if i >= self.column_count() || !self.has_row {
            return None;
        }
        ColumnType::from_int(unsafe { sqlite3_column_type(self.ptr.as_ptr(), i as c_int) })
    }

    pub fn column_i64(&mut self, i: usize) -> Option<i64> {
        if i >= self.column_count() || !self.has_row {
            return None;
        }
        Some(unsafe { sqlite3_column_int64(self.ptr.as_ptr(), i as c_int) })
    }

    pub fn column_f64(&mut self, i: usize) -> Option<f64> {
        if i >= self.column_count() || !self.has_row {
            return None;
        }
        Some(unsafe { sqlite3_column_double(self.ptr.as_ptr(), i as c_int) })
    }

    pub fn column_text(&mut self, i: usize) -> Option<Result<&str, ColumnTextError>> {
        if i >= self.column_count() || !self.has_row {
            return None;
        }
        let bytes = unsafe {
            let ptr = sqlite3_column_text(self.ptr.as_ptr(), i as c_int);
            if ptr.is_null() {
                return Some(Ok(""));
            }
            let n = sqlite3_column_bytes(self.ptr.as_ptr(), i as c_int);
            slice::from_raw_parts(ptr, n as usize)
        };
        Some(str::from_utf8(bytes).map_err(|err| ColumnTextError { bytes, err }))
    }

    pub fn column_blob(&mut self, i: usize) -> Option<&[u8]> {
        if i >= self.column_count() || !self.has_row {
            return None;
        }
        Some(unsafe {
            let ptr = sqlite3_column_blob(self.ptr.as_ptr(), i as c_int);
            if ptr.is_null() {
                return Some(b"");
            }
            let n = sqlite3_column_bytes(self.ptr.as_ptr(), i as c_int);
            slice::from_raw_parts(ptr as *const u8, n as usize)
        })
    }
}

impl<'c> Drop for Statement<'c> {
    fn drop(&mut self) {
        unsafe { sqlite3_finalize(self.ptr.as_ptr()) };
    }
}

/// An error value encountered when a column value contains
/// [invalid UTF-8](https://www.sqlite.org/invalidutf.html).
#[derive(Clone, Copy, Debug)]
pub struct ColumnTextError<'a> {
    bytes: &'a [u8],
    err: Utf8Error,
}

impl<'a> ColumnTextError<'a> {
    /// Returns a slice of bytes that were attempt to convert to a `&str`.
    #[inline]
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Fetch a [`Utf8Error`] to get more details about the conversion failure.
    #[inline]
    pub fn utf8_error(&self) -> Utf8Error {
        self.err
    }
}

impl<'a> fmt::Display for ColumnTextError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.err.fmt(f)
    }
}

impl<'a> std::error::Error for ColumnTextError<'a> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.err)
    }
}

bitflags! {
    #[repr(transparent)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct OpenFlags: c_int {
        const READONLY = SQLITE_OPEN_READONLY;
        const READWRITE = SQLITE_OPEN_READWRITE;
        const CREATE = SQLITE_OPEN_CREATE;
        const URI = SQLITE_OPEN_URI;
        const MEMORY = SQLITE_OPEN_MEMORY;
        const NOMUTEX = SQLITE_OPEN_NOMUTEX;
        const FULLMUTEX = SQLITE_OPEN_FULLMUTEX;
        const SHAREDCACHE = SQLITE_OPEN_SHAREDCACHE;
    }
}

impl Default for OpenFlags {
    fn default() -> Self {
        OpenFlags::READWRITE | OpenFlags::CREATE | OpenFlags::URI | OpenFlags::NOMUTEX
    }
}

/// Codes for each of the SQLite fundamental datatypes.
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
    fn from_int(i: c_int) -> Option<ColumnType> {
        match i {
            SQLITE_INTEGER => Some(ColumnType::Integer),
            SQLITE_FLOAT => Some(ColumnType::Float),
            SQLITE_TEXT => Some(ColumnType::Text),
            SQLITE_BLOB => Some(ColumnType::Blob),
            SQLITE_NULL => Some(ColumnType::Null),
            _ => None,
        }
    }
}

impl Default for ColumnType {
    fn default() -> Self {
        ColumnType::Null
    }
}

/// A subset of [`ResultCode`] returned by [`Stmt::step`].
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum StepResult {
    /// Returned each time a new row of data is ready for processing by the caller.
    Row = SQLITE_ROW as u8,
    /// Means that the statement has finished executing successfully.
    Done = SQLITE_DONE as u8,
}

impl From<StepResult> for ResultCode {
    fn from(rc: StepResult) -> Self {
        ResultCode(rc as c_int)
    }
}

pub fn is_complete(s: impl AsRef<CStr>) -> bool {
    let s = s.as_ref().as_ptr();
    let rc = unsafe { sqlite3_complete(s) };
    match rc {
        0 => false,
        SQLITE_NOMEM => panic!("out of memory"),
        _ => true,
    }
}

pub fn version() -> &'static str {
    let s = unsafe { CStr::from_ptr(sqlite3_libversion()) };
    std::str::from_utf8(s.to_bytes()).unwrap()
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use super::*;

    #[test]
    fn test_prepare_empty() {
        let conn =
            Connection::open(&CString::new(":memory:").unwrap(), OpenFlags::default()).unwrap();
        let result = conn.prepare("").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_read_values() {
        let conn =
            Connection::open(&CString::new(":memory:").unwrap(), OpenFlags::default()).unwrap();
        let (mut stmt, _) = conn
            .prepare("select 123 as \"int\", 'foo' as \"text\";")
            .unwrap()
            .expect("statement is not empty");
        assert_eq!(stmt.column_count(), 2);
        assert_eq!(stmt.column_name(0), Some(String::from("int")));
        assert_eq!(stmt.column_name(1), Some(String::from("text")));
        assert_eq!(stmt.column_name(2), None);

        assert_eq!(stmt.step().unwrap(), StepResult::Row);
        assert_eq!(stmt.column_type(0), Some(ColumnType::Integer));
        assert_eq!(stmt.column_i64(0), Some(123));
        assert_eq!(stmt.column_type(1), Some(ColumnType::Text));
        assert_eq!(stmt.column_text(1).unwrap().unwrap(), "foo");

        assert_eq!(stmt.step().unwrap(), StepResult::Done);
    }
}
