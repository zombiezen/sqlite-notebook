use std::borrow::{Borrow, Cow};
use std::cell::Cell;
use std::cmp::{min, Ordering};
use std::ffi::{c_char, c_int, c_uchar, c_uint, c_void, CStr};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ptr::{self, null, NonNull};
use std::rc::Rc;
use std::str::{self, Utf8Error};
use std::{fmt, slice};

mod bytearray;
mod function;
mod result;
mod txn_state;

pub use function::*;
pub use result::*;
pub use txn_state::*;

use bitflags::bitflags;
use libsqlite3_sys::{
    sqlite3, sqlite3_bind_blob64, sqlite3_bind_double, sqlite3_bind_int64, sqlite3_bind_null,
    sqlite3_bind_parameter_count, sqlite3_bind_parameter_name, sqlite3_bind_text64,
    sqlite3_bind_zeroblob64, sqlite3_clear_bindings, sqlite3_close, sqlite3_column_blob,
    sqlite3_column_bytes, sqlite3_column_count, sqlite3_column_double, sqlite3_column_int64,
    sqlite3_column_name, sqlite3_column_text, sqlite3_column_type, sqlite3_column_value,
    sqlite3_complete, sqlite3_db_config, sqlite3_db_handle, sqlite3_db_readonly, sqlite3_finalize,
    sqlite3_libversion, sqlite3_open_v2, sqlite3_prepare_v2, sqlite3_reset, sqlite3_step,
    sqlite3_stmt, sqlite3_strlike, sqlite3_strnicmp, SQLITE_BLOB, SQLITE_DBCONFIG_DQS_DDL,
    SQLITE_DBCONFIG_DQS_DML, SQLITE_DONE, SQLITE_FLOAT, SQLITE_INTEGER, SQLITE_NOMEM, SQLITE_NULL,
    SQLITE_OPEN_CREATE, SQLITE_OPEN_FULLMUTEX, SQLITE_OPEN_MEMORY, SQLITE_OPEN_NOMUTEX,
    SQLITE_OPEN_READONLY, SQLITE_OPEN_READWRITE, SQLITE_OPEN_SHAREDCACHE, SQLITE_OPEN_URI,
    SQLITE_ROW, SQLITE_TEXT, SQLITE_UTF8,
};

type PhantomUnsend = PhantomData<Rc<()>>;
type PhantomUnsync = PhantomData<Cell<()>>;

#[repr(transparent)]
pub struct Conn {
    db: sqlite3,
    unsync: PhantomUnsync,
}

impl Conn {
    fn error(&self) -> Option<Error> {
        Error::get(self.as_nonnull())
    }

    #[inline]
    fn as_nonnull(&self) -> NonNull<sqlite3> {
        unsafe { NonNull::new_unchecked(self.as_ptr()) }
    }

    #[inline]
    fn as_ptr(&self) -> *mut sqlite3 {
        ptr::addr_of!(self.db) as *mut sqlite3
    }

    pub fn prepare<'c, 's>(&'c self, sql: &'s str) -> Result<Option<(Statement<'c>, &'s str)>> {
        let n_byte: c_int = sql
            .len()
            .try_into()
            .map_err(|_| ResultCode::TOOBIG.to_result().unwrap_err())?;
        let mut stmt = MaybeUninit::uninit();
        let z_sql = sql.as_ptr() as *const c_char;
        let mut tail = MaybeUninit::uninit();
        let rc = ResultCode(unsafe {
            sqlite3_prepare_v2(
                self.as_ptr(),
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

    /// Reports whether the given schema is attached as read-only.
    /// Returns `None` if the argument does not name a database on the connection.
    pub fn db_readonly(&self, schema: &(impl AsRef<CStr> + ?Sized)) -> Option<bool> {
        let result = unsafe { sqlite3_db_readonly(self.as_ptr(), schema.as_ref().as_ptr()) };
        match result {
            -1 => None,
            0 => Some(false),
            1 => Some(true),
            _ => panic!("unhandled result {} from sqlite3_db_readonly", result),
        }
    }
}

impl Debug for Conn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Pointer::fmt(&&self.db, f)
    }
}

#[repr(transparent)]
#[derive(Debug)]
pub struct Connection(NonNull<Conn>);

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
            Some(db) => db.cast::<Conn>(),
            None => return Err(ResultCode::NOMEM.to_result().unwrap_err()),
        };
        let conn = Connection(db); // Now will drop properly.
        if rc != ResultCode::OK {
            return Err(conn.error().unwrap());
        }
        unsafe {
            sqlite3_db_config(
                db.as_ptr() as *mut sqlite3,
                SQLITE_DBCONFIG_DQS_DML,
                0 as c_int,
                ptr::null::<c_void>(),
            );
            sqlite3_db_config(
                db.as_ptr() as *mut sqlite3,
                SQLITE_DBCONFIG_DQS_DDL,
                0 as c_int,
                ptr::null::<c_void>(),
            );
        }
        Ok(conn)
    }
}

impl Deref for Connection {
    type Target = Conn;

    fn deref(&self) -> &Self::Target {
        self.borrow()
    }
}

impl Borrow<Conn> for Connection {
    fn borrow(&self) -> &Conn {
        unsafe { self.0.as_ref() }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe {
            assert_eq!(
                ResultCode(sqlite3_close(self.0.as_ptr() as *mut sqlite3)),
                ResultCode::OK
            );
        }
    }
}

/// A single SQL statement that has been compiled into binary form
/// and is ready to be evaluated.
///
/// # Example
///
/// ```rust
/// # use std::ffi::CString;
/// # use zombiezen_sqlite::{Connection, OpenFlags, StepResult};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// #     let conn = Connection::open(
/// #         &CString::new(":memory:")?,
/// #         OpenFlags::default(),
/// #     )?;
/// let (mut stmt, _) = conn.prepare("values (123);")?.unwrap();
/// # let mut row_count = 0usize;
/// while stmt.step()?.has_row() {
///     let col_value = stmt.column_i64(0);
/// #     assert_eq!(col_value, 123);
/// #     row_count += 1;
/// #     if false {
///     println!("{}", col_value);
/// #     }
/// }
/// #     assert_eq!(row_count, 1);
/// #     Ok(())
/// # }
/// ```
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

    /// Reset all host parameters to `NULL`.
    pub fn clear_bindings(&mut self) {
        unsafe {
            sqlite3_clear_bindings(self.ptr.as_ptr());
        }
    }

    /// Returns the index of the largest (rightmost) parameter.
    /// For all forms except `?NNN`,
    /// this will correspond to the number of unique parameters.
    pub fn bind_parameter_count(&self) -> usize {
        (unsafe { sqlite3_bind_parameter_count(self.ptr.as_ptr()) }) as usize
    }

    #[inline(always)]
    fn usize_to_int(i: usize) -> Result<c_int, Error> {
        c_int::try_from(i).map_err(|_| ResultCode::RANGE.to_result().unwrap_err())
    }

    /// Returns the name of the `i`th [SQL parameter] in the statement.
    /// The first host parameter has an index of 1.
    /// The initial `":"` or `"$"` or `"@"` or `"?"` is included as part of the name.
    ///
    /// [SQL parameter]: https://www.sqlite.org/c3ref/bind_blob.html
    pub fn bind_parameter_name(&self, i: usize) -> Option<&CStr> {
        let i = Self::usize_to_int(i).ok()?;
        unsafe {
            let ptr = sqlite3_bind_parameter_name(self.ptr.as_ptr(), i);
            if ptr.is_null() {
                None
            } else {
                Some(CStr::from_ptr(ptr))
            }
        }
    }

    /// Returns the index of a [SQL parameter] given its `name`.
    /// The index value returned is suitable
    /// for use as the index parameter to the `bind_*` functions.
    ///
    /// [SQL parameter]: https://www.sqlite.org/c3ref/bind_blob.html
    pub fn bind_parameter_index(&self, name: &str) -> Option<usize> {
        // Calling sqlite3_bind_parameter_index would require allocating a C string.
        // Since SQLite will always store these as UTF-8
        // and we can guarantee the input is valid UTF-8 (more convenient anyway),
        // we just do the iteration ourselves.
        (1..=self.bind_parameter_count()).find_map(|i| {
            self.bind_parameter_name(i).and_then(|name_i| {
                if name.as_bytes() == name_i.to_bytes() {
                    Some(i)
                } else {
                    None
                }
            })
        })
    }

    #[inline(always)]
    fn bind<F>(&mut self, i: usize, f: F) -> Result<()>
    where
        F: FnOnce(*mut sqlite3_stmt, c_int) -> c_int,
    {
        let i = Self::usize_to_int(i)?;
        let rc = ResultCode(f(self.ptr.as_ptr(), i));
        if rc.is_success() {
            Ok(())
        } else {
            Err(self.error().expect("sqlite3_bind_* returned an error but "))
        }
    }

    /// Sets a host parameter in a statement to `NULL`.
    /// The first host parameter has an index of 1.
    pub fn bind_null(&mut self, i: usize) -> Result<()> {
        self.bind(i, |stmt, i| unsafe { sqlite3_bind_null(stmt, i) })
    }

    /// Sets a host parameter in a statement to a 64-bit integer.
    /// The first host parameter has an index of 1.
    pub fn bind_i64(&mut self, i: usize, v: i64) -> Result<()> {
        self.bind(i, |stmt, i| unsafe { sqlite3_bind_int64(stmt, i, v) })
    }

    /// Sets a host parameter in a statement to a 64-bit floating point number.
    /// The first host parameter has an index of 1.
    pub fn bind_f64(&mut self, i: usize, v: f64) -> Result<()> {
        self.bind(i, |stmt, i| unsafe { sqlite3_bind_double(stmt, i, v) })
    }

    /// Sets a host parameter in a statement to a UTF-8 string.
    /// The first host parameter has an index of 1.
    pub fn bind_text(&mut self, i: usize, v: impl Into<String>) -> Result<()> {
        self.bind(i, |stmt, i| {
            let (ptr, n) = bytearray::new(v.into().into_bytes());
            unsafe {
                sqlite3_bind_text64(
                    stmt,
                    i,
                    ptr.cast::<c_char>().as_ptr(),
                    n as u64,
                    Some(bytearray::destroy),
                    SQLITE_UTF8 as c_uchar,
                )
            }
        })
    }

    /// Sets a host parameter in a statement to a `BLOB` (byte slice).
    /// The first host parameter has an index of 1.
    pub fn bind_blob(&mut self, i: usize, v: impl Into<Vec<u8>>) -> Result<()> {
        self.bind(i, |stmt, i| unsafe {
            let (ptr, n) = bytearray::new(v.into());
            sqlite3_bind_blob64(stmt, i, ptr.as_ptr(), n as u64, Some(bytearray::destroy))
        })
    }

    /// Sets a host parameter in a statement to a zero-filled `BLOB` with length `n`.
    /// A zeroblob uses a fixed amount of memory while it is being processed.
    /// The first host parameter has an index of 1.
    pub fn bind_zeroblob(&mut self, i: usize, n: u64) -> Result<()> {
        self.bind(i, |stmt, i| unsafe { sqlite3_bind_zeroblob64(stmt, i, n) })
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
    /// If replacement is acceptable, you can use [`ResultExt::to_string_lossy`].
    ///
    /// ```rust
    /// use std::borrow::Cow;
    /// use zombiezen_sqlite::ResultExt;
    /// # use std::ffi::CString;
    /// # use zombiezen_sqlite::{Connection, OpenFlags, StepResult};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// #     let conn = Connection::open(
    /// #         &CString::new(":memory:")?,
    /// #         OpenFlags::default(),
    /// #     )?;
    /// #     let (mut stmt, _) = conn.prepare("values ('Hello, World!');")?.unwrap();
    /// #     assert_eq!(stmt.step()?, StepResult::Row);
    /// let s: Cow<str> = stmt.column_text(0).to_string_lossy();
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

    /// Returns the unprotected value of the `i`th column.
    /// This is normally only useful in the application-defined SQL functions.
    /// The leftmost column is number 0.
    ///
    /// # Panics
    ///
    /// Panics if the statement has not returned a row
    /// or if `i >= self.column_count()`.
    pub fn column_value(&self, i: usize) -> &Value<Unprotected> {
        self.check_col(i);
        unsafe {
            let ptr = sqlite3_column_value(self.ptr.as_ptr(), i as c_int);
            debug_assert!(!ptr.is_null());
            &*(ptr as *mut Value<Unprotected>)
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
        fmt::Display::fmt(&self.err, f)
    }
}

impl<'a> std::error::Error for ColumnTextError<'a> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.err)
    }
}

/// Extension trait for `Result<&str, ColumnTextError>`.
pub trait ResultExt<'a> {
    /// Converts the result into a string by replacing invalid UTF-8
    /// with the Unicode replacement character if needed.
    fn to_string_lossy(self) -> Cow<'a, str>;
}

impl<'a> ResultExt<'a> for Result<&'a str, ColumnTextError<'a>> {
    fn to_string_lossy(self) -> Cow<'a, str> {
        self.map_or_else(|err| String::from_utf8_lossy(err.as_bytes()), |s| s.into())
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

impl StepResult {
    /// Reports whether the result equals [`StepResult::Row`].
    #[inline]
    pub const fn has_row(self) -> bool {
        match self {
            Self::Row => true,
            _ => false,
        }
    }
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

/// Compare the contents of two strings
/// using the same definition of case independence that SQLite uses internally
/// when comparing identifiers.
pub fn stricmp(left: impl AsRef<str>, right: impl AsRef<str>) -> Ordering {
    let left = left.as_ref();
    let right = right.as_ref();
    let common_length = min(left.len(), right.len());

    let result = unsafe {
        sqlite3_strnicmp(
            left.as_ptr() as *const c_char,
            right.as_ptr() as *const c_char,
            c_int::try_from(common_length).expect("strings too large"),
        )
    };
    if result > 0 {
        Ordering::Greater
    } else if result < 0 {
        Ordering::Less
    } else {
        left.len().cmp(&right.len())
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
            .prepare("select 123 as \"int\", 'foo' as \"text\", null as \"null\";")
            .unwrap()
            .expect("statement is not empty");
        assert_eq!(stmt.column_count(), 3);
        assert_eq!(stmt.column_name(0), Some(String::from("int")));
        assert_eq!(stmt.column_name(1), Some(String::from("text")));
        assert_eq!(stmt.column_name(2), Some(String::from("null")));
        assert_eq!(stmt.column_name(3), None);

        assert_eq!(stmt.step().unwrap(), StepResult::Row);
        assert_eq!(stmt.column_type(0), ColumnType::Integer);
        assert_eq!(stmt.column_i64(0), 123);
        assert_eq!(stmt.column_type(1), ColumnType::Text);
        assert_eq!(stmt.column_text(1).unwrap(), "foo");
        assert_eq!(stmt.column_type(2), ColumnType::Null);
        assert_eq!(
            stmt.column_value(2).dup().as_mut().r#type(),
            ColumnType::Null
        );

        assert_eq!(stmt.step().unwrap(), StepResult::Done);
    }

    #[test]
    fn test_bind_text() {
        let conn = Connection::open(MEMORY, OpenFlags::default()).unwrap();
        let (mut stmt, _) = conn
            .prepare("select ?1;")
            .unwrap()
            .expect("statement is not empty");
        const WANT: &str = "Hello, World!\n";
        stmt.bind_text(1, WANT).unwrap();

        assert_eq!(stmt.step().unwrap(), StepResult::Row);
        assert_eq!(stmt.column_type(0), ColumnType::Text);
        assert_eq!(stmt.column_text(0).unwrap(), WANT);

        assert_eq!(stmt.step().unwrap(), StepResult::Done);
    }

    #[test]
    fn test_stricmp() {
        assert_eq!(stricmp("", ""), Ordering::Equal);
        assert_eq!(stricmp("a", "b"), Ordering::Less);
        assert_eq!(stricmp("b", "a"), Ordering::Greater);
        assert_eq!(stricmp("a", "B"), Ordering::Less);
        assert_eq!(stricmp("B", "a"), Ordering::Greater);
        assert_eq!(stricmp("a", "Aa"), Ordering::Less);
        assert_eq!(stricmp("Aa", "a"), Ordering::Greater);
        assert_eq!(stricmp("aA", "Aa"), Ordering::Equal);
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

    #[test]
    fn test_is_complete() {
        assert!(!is_complete(const_cstr!("select 1")));
        assert!(is_complete(const_cstr!("select 1;")));
        assert!(!is_complete(const_cstr!("create table foo (")));
        // TODO(soon): assert!(!is_complete(const_cstr!("create table foo (;")));
    }
}
