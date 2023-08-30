use std::ffi::{c_char, c_int, c_uint, c_void, CStr};
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
    sqlite3_reset, sqlite3_step, sqlite3_stmt, sqlite3_strlike, SQLITE_BLOB,
    SQLITE_DBCONFIG_DQS_DDL, SQLITE_DBCONFIG_DQS_DML, SQLITE_DONE, SQLITE_FLOAT, SQLITE_INTEGER,
    SQLITE_NOMEM, SQLITE_NULL, SQLITE_OPEN_CREATE, SQLITE_OPEN_FULLMUTEX, SQLITE_OPEN_MEMORY,
    SQLITE_OPEN_NOMUTEX, SQLITE_OPEN_READONLY, SQLITE_OPEN_READWRITE, SQLITE_OPEN_SHAREDCACHE,
    SQLITE_OPEN_URI, SQLITE_ROW, SQLITE_TEXT,
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

/// A single SQL statement that has been compiled into binary form
/// and is ready to be evaluated.
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

    /// Evaluate the statement, stopping at the next row returned.
    /// Once [`StepResult::Done`] is returned,
    /// the statement has finished executing successfully
    /// and `step` should not be called again without first calling [`reset`].
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

    /// Resets the prepared statement to its initial state,
    /// returning an error if the previous evaluation of the statement
    /// did not complete successfully.
    /// Even if there were no previous errors, `reset` may still return an error
    /// in the case that a new error was caused by resetting the statement.
    ///
    /// This does not change the bindings: use [`clear_bindings`] to do that.
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

    /// Returns the number of columns in the result set returned by the statement.
    /// If `column_count` returns 0, then the statement returns no data.
    pub fn column_count(&self) -> usize {
        (unsafe { sqlite3_column_count(self.ptr.as_ptr()) }) as usize
    }

    /// Returns the name assigned to a particular column using the "AS" clause.
    /// The leftmost column is number 0.
    /// Will be `None` if `i >= self.column_count()`
    /// or SQLite failed to allocate the column name.
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

    #[inline(always)]
    fn check_col(&self, i: usize) {
        assert!(self.has_row);
        let n = self.column_count();
        assert!(i < n, "{} >= column_count={}", i, n);
    }

    /// Returns the datatype of the value in the `i`th column.
    /// The leftmost column is number 0.
    /// The return value is undefined after calling any of
    /// [`column_i64`], [`column_f64`], [`column_text`], or [`column_blob`]
    /// on the column, as these can all perform conversions.
    ///
    /// # Panics
    ///
    /// Panics if the statement has not returned a row
    /// or if `i >= self.column_count()`.
    pub fn column_type(&self, i: usize) -> ColumnType {
        self.check_col(i);
        ColumnType::from_int(unsafe { sqlite3_column_type(self.ptr.as_ptr(), i as c_int) })
            .expect("SQLite returned an unknown column type")
    }

    /// Returns the value in the `i`th column as 64-bit integer,
    /// [converting it] if necessary.
    /// The leftmost column is number 0.
    ///
    /// [converting it]: https://www.sqlite.org/c3ref/column_blob.html
    ///
    /// # Panics
    ///
    /// Panics if the statement has not returned a row
    /// or if `i >= self.column_count()`.
    pub fn column_i64(&mut self, i: usize) -> i64 {
        self.check_col(i);
        unsafe { sqlite3_column_int64(self.ptr.as_ptr(), i as c_int) }
    }

    /// Returns the value in the `i`th column as 64-bit floating point number,
    /// [converting it] if necessary.
    /// The leftmost column is number 0.
    ///
    /// [converting it]: https://www.sqlite.org/c3ref/column_blob.html
    ///
    /// # Panics
    ///
    /// Panics if the statement has not returned a row
    /// or if `i >= self.column_count()`.
    pub fn column_f64(&mut self, i: usize) -> f64 {
        self.check_col(i);
        unsafe { sqlite3_column_double(self.ptr.as_ptr(), i as c_int) }
    }

    /// Returns the value in the `i`th column as `TEXT` (a UTF-8 string),
    /// [converting it] if necessary.
    /// The leftmost column is number 0.
    ///
    /// [converting it]: https://www.sqlite.org/c3ref/column_blob.html
    ///
    /// # Errors
    ///
    /// If the column contains [invalid UTF-8],
    /// then `column_text` returns a [`ColumnTextError`].
    /// This can be used to retrieve the returned byte slice,
    /// which you can use to pass to [`String::from_utf8_lossy`] for example:
    ///
    /// ```rust
    /// use std::borrow::Cow;
    /// # use std::ffi::CString;
    /// # use zombiezen_sqlite::{Connection, OpenFlags, StepResult};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// #     let conn = Connection::open(
    /// #         &CString::new(":memory:")?,
    /// #         OpenFlags::default(),
    /// #     )?;
    /// #     let (mut stmt, _) = conn.prepare("values ('Hello, World!');")?.unwrap();
    /// #     assert_eq!(stmt.step()?, StepResult::Row);
    /// let s: Cow<str> = stmt.column_text(0).map_or_else(
    ///     |err| String::from_utf8_lossy(err.as_bytes()),
    ///     |s| s.into(),
    /// );
    /// #     assert_eq!(s, "Hello, World!");
    /// #     Ok(())
    /// # }
    /// ```
    ///
    /// [invalid UTF-8]: https://www.sqlite.org/invalidutf.html
    /// [`String::from_utf8_lossy`]: https://doc.rust-lang.org/std/string/struct.String.html#method.from_utf8_lossy
    ///
    /// # Panics
    ///
    /// Panics if the statement has not returned a row
    /// or if `i >= self.column_count()`.
    pub fn column_text(&mut self, i: usize) -> Result<&str, ColumnTextError> {
        self.check_col(i);
        let bytes = unsafe {
            let ptr = sqlite3_column_text(self.ptr.as_ptr(), i as c_int);
            if ptr.is_null() {
                return Ok("");
            }
            let n = sqlite3_column_bytes(self.ptr.as_ptr(), i as c_int);
            slice::from_raw_parts(ptr, n as usize)
        };
        str::from_utf8(bytes).map_err(|err| ColumnTextError { bytes, err })
    }

    /// Returns the value in the `i`th column as a `BLOB` (byte slice),
    /// [converting it] if necessary.
    /// The leftmost column is number 0.
    ///
    /// [converting it]: https://www.sqlite.org/c3ref/column_blob.html
    ///
    /// # Panics
    ///
    /// Panics if the statement has not returned a row
    /// or if `i >= self.column_count()`.
    pub fn column_blob(&mut self, i: usize) -> &[u8] {
        self.check_col(i);
        unsafe {
            let ptr = sqlite3_column_blob(self.ptr.as_ptr(), i as c_int);
            if ptr.is_null() {
                return b"";
            }
            let n = sqlite3_column_bytes(self.ptr.as_ptr(), i as c_int);
            slice::from_raw_parts(ptr as *const u8, n as usize)
        }
    }

    /// Releases any resources associated with the statement
    /// and returns any error from the most recent evaluation of the statement.
    /// Even if there were no previous errors, `finalize` may still return an error
    /// in the case that a new error was caused by finalizing the statement.
    ///
    /// Calling `finalize` is equivalent to `Drop`ping the statement,
    /// but allows the error to be inspected.
    pub fn finalize(mut self) -> Result<()> {
        self.finalize_internal()
    }

    fn finalize_internal(&mut self) -> Result<()> {
        if ResultCode(unsafe { sqlite3_finalize(self.ptr.as_ptr()) }).is_success() {
            Ok(())
        } else {
            Err(self.error().expect("sqlite3_finalize returned an error"))
        }
    }
}

impl<'c> Drop for Statement<'c> {
    fn drop(&mut self) {
        let _ = self.finalize_internal();
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

/// Reports whether `s` matches the [`LIKE`] pattern `glob`
/// with the given escape character.
/// For `s LIKE glob` without the `ESCAPE` clause,
/// use 0 for `escape_char`.
/// As with the `LIKE` operator, the `strlike` function is case insensitive -
/// equivalent upper and lower case ASCII characters match one another.
/// The `strlike` function matches Unicode characters,
/// though only ASCII characters are case folded.
///
/// [`LIKE`]: https://www.sqlite.org/lang_expr.html#like
pub fn strlike(glob: impl AsRef<CStr>, s: impl AsRef<CStr>, escape_char: char) -> bool {
    (unsafe {
        sqlite3_strlike(
            glob.as_ref().as_ptr(),
            s.as_ref().as_ptr(),
            escape_char as c_uint,
        )
    }) == 0
}

pub fn version() -> &'static str {
    let s = unsafe { CStr::from_ptr(sqlite3_libversion()) };
    std::str::from_utf8(s.to_bytes()).unwrap()
}

#[cfg(test)]
mod tests {
    use zombiezen_const_cstr::{const_cstr, ConstCStr};

    use super::*;

    const MEMORY: ConstCStr = const_cstr!(":memory:");

    #[test]
    fn test_prepare_empty() {
        let conn = Connection::open(MEMORY, OpenFlags::default()).unwrap();
        let result = conn.prepare("").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_read_values() {
        let conn = Connection::open(MEMORY, OpenFlags::default()).unwrap();
        let (mut stmt, _) = conn
            .prepare("select 123 as \"int\", 'foo' as \"text\";")
            .unwrap()
            .expect("statement is not empty");
        assert_eq!(stmt.column_count(), 2);
        assert_eq!(stmt.column_name(0), Some(String::from("int")));
        assert_eq!(stmt.column_name(1), Some(String::from("text")));
        assert_eq!(stmt.column_name(2), None);

        assert_eq!(stmt.step().unwrap(), StepResult::Row);
        assert_eq!(stmt.column_type(0), ColumnType::Integer);
        assert_eq!(stmt.column_i64(0), 123);
        assert_eq!(stmt.column_type(1), ColumnType::Text);
        assert_eq!(stmt.column_text(1).unwrap(), "foo");

        assert_eq!(stmt.step().unwrap(), StepResult::Done);
    }

    #[test]
    fn test_strlike() {
        assert!(strlike(const_cstr!("foo"), const_cstr!("foo"), '\x00'));
        assert!(strlike(const_cstr!("%oob%"), const_cstr!("foobar"), '\x00'));
        assert!(strlike(const_cstr!("a\\%b"), const_cstr!("a\\b"), '\x00'));
        assert!(!strlike(const_cstr!("a\\%b"), const_cstr!("a\\b"), '\\'));
        assert!(strlike(const_cstr!("a\\%b"), const_cstr!("a%b"), '\\'));
        assert!(!strlike(
            const_cstr!("%bork%"),
            const_cstr!("foobar"),
            '\x00'
        ));
    }
}
