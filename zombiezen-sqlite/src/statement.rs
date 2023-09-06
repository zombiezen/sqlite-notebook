use std::ffi::{c_char, c_int, c_uchar, CStr};
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};
use std::slice;
use std::str::{self, Utf8Error};

use libsqlite3_sys::{
    sqlite3_bind_blob64, sqlite3_bind_double, sqlite3_bind_int64, sqlite3_bind_null,
    sqlite3_bind_parameter_count, sqlite3_bind_parameter_name, sqlite3_bind_text64,
    sqlite3_bind_value, sqlite3_bind_zeroblob64, sqlite3_clear_bindings, sqlite3_column_blob,
    sqlite3_column_bytes, sqlite3_column_count, sqlite3_column_double, sqlite3_column_int64,
    sqlite3_column_name, sqlite3_column_text, sqlite3_column_type, sqlite3_column_value,
    sqlite3_complete, sqlite3_db_handle, sqlite3_finalize, sqlite3_prepare_v2, sqlite3_reset,
    sqlite3_step, sqlite3_stmt, SQLITE_DONE, SQLITE_NOMEM, SQLITE_ROW, SQLITE_UTF8,
};

use crate::*;

impl Conn {
    /// Compile a SQL statement into a byte-code program.
    /// The first return value is the compiled statement, if one was found.
    /// The second return value is the remaining uncompiled source.
    ///
    /// # Example
    ///
    /// ```
    /// # use zombiezen_sqlite::{Connection, OpenFlags};
    /// # use std::ffi::CStr;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let conn = Connection::open(<&CStr>::default(), OpenFlags::default() | OpenFlags::MEMORY)?;
    /// let stmt = conn.prepare("SELECT 2 + 2;").0?.expect("Statement not empty");
    /// # Ok(())
    /// # }
    /// ```
    #[doc(alias("sqlite3_prepare", "sqlite3_prepare_v2"))]
    pub fn prepare<'c, 's>(&'c self, sql: &'s str) -> (Result<Option<Statement<'c>>>, &'s str) {
        let n_byte: c_int = match sql.len().try_into() {
            Ok(n) => n,
            Err(_) => return (Err(ResultCode::TOOBIG.to_result().unwrap_err()), sql),
        };
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
        let tail_start = unsafe {
            let tail_ptr = tail.assume_init();
            debug_assert!(!tail_ptr.is_null());
            tail_ptr.offset_from(z_sql)
        };
        debug_assert!(tail_start >= 0);
        let tail = &sql.split_at(tail_start as usize).1;
        if rc == ResultCode::OK {
            (
                Ok(NonNull::new(unsafe { stmt.assume_init() }).map(|ptr| Statement::new(ptr))),
                tail,
            )
        } else {
            debug_assert!(unsafe { stmt.assume_init() }.is_null());
            return (Err(self.error().unwrap()), tail);
        }
    }
}

/// A single SQL statement that has been compiled into binary form
/// and is ready to be evaluated.
/// Statements are created with [`Conn::prepare`].
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
/// let mut stmt = conn.prepare("values (123);").0?.unwrap();
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
    ptr: *mut sqlite3_stmt,
    has_row: bool,
    conn: PhantomData<&'c Connection>,
}

impl<'c> Statement<'c> {
    #[inline]
    pub(crate) fn new(ptr: NonNull<sqlite3_stmt>) -> Self {
        Statement {
            ptr: ptr.as_ptr(),
            has_row: false,
            conn: PhantomData,
        }
    }

    fn error(&self) -> Option<Error> {
        let db = NonNull::new(unsafe { sqlite3_db_handle(self.ptr) });
        db.and_then(|db| Error::get(db))
    }

    /// Evaluate the statement, stopping at the next row returned.
    /// Once [`Done`][StepResult::Done] is returned,
    /// the statement has finished executing successfully
    /// and `step` should not be called again
    /// without first calling [`reset`][Statement::reset].
    pub fn step(&mut self) -> Result<StepResult> {
        let rc = ResultCode(unsafe { sqlite3_step(self.ptr) });
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
    /// This does not change the bindings:
    /// use [`clear_bindings`][Statement::clear_bindings] to do that.
    pub fn reset(&mut self) -> Result<()> {
        self.has_row = false;
        let rc = ResultCode(unsafe { sqlite3_reset(self.ptr) });
        match rc {
            ResultCode::OK => Ok(()),
            _ => Err(self.error().unwrap()),
        }
    }

    /// Reset all host parameters to `NULL`.
    pub fn clear_bindings(&mut self) {
        unsafe {
            sqlite3_clear_bindings(self.ptr);
        }
    }

    /// Returns the index of the largest (rightmost) parameter.
    /// For all forms except `?NNN`,
    /// this will correspond to the number of unique parameters.
    pub fn bind_parameter_count(&self) -> usize {
        (unsafe { sqlite3_bind_parameter_count(self.ptr) }) as usize
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
            let ptr = sqlite3_bind_parameter_name(self.ptr, i);
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
        let rc = ResultCode(f(self.ptr, i));
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

    /// Sets a host parameter in a statement the given value.
    /// The first host parameter has an index of 1.
    /// This function operates on both protected and unprotected values.
    pub fn bind_value<V: Value + ?Sized>(&mut self, i: usize, v: &V) -> Result<()> {
        self.bind(i, |stmt, i| unsafe {
            sqlite3_bind_value(stmt, i, v.as_ptr())
        })
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
        (unsafe { sqlite3_column_count(self.ptr) }) as usize
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
            let s = sqlite3_column_name(self.ptr, i as c_int);
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
    /// [`column_i64`][Statement::column_i64],
    /// [`column_f64`][Statement::column_f64],
    /// [`column_text`][Statement::column_text],
    /// or [`column_blob`][Statement::column_blob]
    /// on the column, as these can all perform conversions.
    ///
    /// # Panics
    ///
    /// Panics if the statement has not returned a row
    /// or if `i >= self.column_count()`.
    pub fn column_type(&self, i: usize) -> DataType {
        self.check_col(i);
        DataType::from_int(unsafe { sqlite3_column_type(self.ptr, i as c_int) })
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
        unsafe { sqlite3_column_int64(self.ptr, i as c_int) }
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
        unsafe { sqlite3_column_double(self.ptr, i as c_int) }
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
    /// then `column_text` returns a [`TextError`].
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
    /// #     let mut stmt = conn.prepare("values ('Hello, World!');").0?.unwrap();
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
    pub fn column_text(&mut self, i: usize) -> Result<&str, TextError> {
        self.check_col(i);
        let bytes_with_nul = unsafe {
            let ptr = sqlite3_column_text(self.ptr, i as c_int);
            if ptr.is_null() {
                return Ok("");
            }
            let n = sqlite3_column_bytes(self.ptr, i as c_int);
            slice::from_raw_parts(ptr, (n as usize) + 1)
        };
        str::from_utf8(&bytes_with_nul[..bytes_with_nul.len() - 1])
            .map_err(|err| TextError::new(bytes_with_nul, err))
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
            let ptr = sqlite3_column_blob(self.ptr, i as c_int);
            if ptr.is_null() {
                return b"";
            }
            let n = sqlite3_column_bytes(self.ptr, i as c_int);
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
    pub fn column_value<'a>(&'a self, i: usize) -> UnprotectedValue<'a> {
        self.check_col(i);
        unsafe {
            UnprotectedValue::new(NonNull::new(sqlite3_column_value(self.ptr, i as c_int)).unwrap())
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
        if ResultCode(unsafe { sqlite3_finalize(self.ptr) }).is_success() {
            self.ptr = ptr::null_mut(); // sqlite3_finalize specifically no-ops on NULL.
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

/// An error value encountered when a value contains
/// [invalid UTF-8](https://www.sqlite.org/invalidutf.html).
#[derive(Clone, Copy, Debug)]
pub struct TextError<'a> {
    bytes_with_nul: &'a [u8],
    err: Utf8Error,
}

impl<'a> TextError<'a> {
    #[inline]
    pub(crate) fn new(bytes_with_nul: &'a [u8], err: Utf8Error) -> Self {
        assert!(bytes_with_nul.ends_with(b"\x00"));
        TextError {
            bytes_with_nul,
            err,
        }
    }

    /// Returns the slice of bytes that were attempt to convert to a `&str`.
    #[inline]
    pub fn as_bytes(&self) -> &'a [u8] {
        &self.bytes_with_nul[..self.bytes_with_nul.len() - 1]
    }

    /// Returns the slice of bytes as a C-style string
    /// terminated at the first NUL byte.
    #[inline]
    pub fn as_cstr(&self) -> &CStr {
        CStr::from_bytes_until_nul(self.bytes_with_nul)
            .expect("NUL byte was validated in TextError::new")
    }

    /// Fetch a [`Utf8Error`] to get more details about the conversion failure.
    #[inline]
    pub fn utf8_error(&self) -> Utf8Error {
        self.err
    }
}

impl<'a> fmt::Display for TextError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.err, f)
    }
}

impl<'a> std::error::Error for TextError<'a> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.err)
    }
}

/// A subset of [`ResultCode`] returned by [`Statement::step`].
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

/// Reports if the input string appears to be a complete SQL statement.
#[doc(alias = "sqlite3_complete")]
pub fn is_complete(s: impl AsRef<CStr>) -> bool {
    let s = s.as_ref().as_ptr();
    let rc = unsafe { sqlite3_complete(s) };
    match rc {
        0 => false,
        SQLITE_NOMEM => panic!("out of memory"),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use zombiezen_const_cstr::{const_cstr, ConstCStr};

    use super::*;

    const MEMORY: ConstCStr = const_cstr!(":memory:");

    #[test]
    fn test_prepare_empty() {
        let conn = Connection::open(MEMORY, OpenFlags::default()).unwrap();
        let (result, tail) = conn.prepare("");
        assert!(
            result.as_ref().is_ok_and(Option::is_none),
            "Received statement: {result:?}"
        );
        assert_eq!(tail, "");
    }

    #[test]
    fn test_prepare_junk_statement() {
        let conn = Connection::open(MEMORY, OpenFlags::default()).unwrap();
        let (stmt_result, tail) = conn.prepare("values (; select 42;");
        assert!(stmt_result.is_err(), "Received statement: {stmt_result:?}");
        assert_eq!(tail, " select 42;");
    }

    #[test]
    fn test_read_values() {
        let conn = Connection::open(MEMORY, OpenFlags::default()).unwrap();
        let (stmt_result, tail) = conn.prepare(
            "select 123 as \"int\", 'foo' as \"text\", null as \"null\"; \
            -- trailing comment",
        );
        assert_eq!(tail, " -- trailing comment");
        let mut stmt = stmt_result.unwrap().expect("statement is not empty");
        assert_eq!(stmt.column_count(), 3);
        assert_eq!(stmt.column_name(0), Some(String::from("int")));
        assert_eq!(stmt.column_name(1), Some(String::from("text")));
        assert_eq!(stmt.column_name(2), Some(String::from("null")));
        assert_eq!(stmt.column_name(3), None);

        assert_eq!(stmt.step().unwrap(), StepResult::Row);
        assert_eq!(stmt.column_type(0), DataType::Integer);
        assert_eq!(stmt.column_i64(0), 123);
        assert_eq!(stmt.column_type(1), DataType::Text);
        assert_eq!(stmt.column_text(1).unwrap(), "foo");
        assert_eq!(stmt.column_type(2), DataType::Null);
        assert_eq!(stmt.column_value(2).dup().as_mut().r#type(), DataType::Null);

        assert_eq!(stmt.step().unwrap(), StepResult::Done);
    }

    #[test]
    fn test_bind_text() {
        let conn = Connection::open(MEMORY, OpenFlags::default()).unwrap();
        let mut stmt = conn
            .prepare("select ?1;")
            .0
            .unwrap()
            .expect("statement is not empty");
        const WANT: &str = "Hello, World!\n";
        stmt.bind_text(1, WANT).unwrap();

        assert_eq!(stmt.step().unwrap(), StepResult::Row);
        assert_eq!(stmt.column_type(0), DataType::Text);
        assert_eq!(stmt.column_text(0).unwrap(), WANT);

        assert_eq!(stmt.step().unwrap(), StepResult::Done);
    }

    #[test]
    fn test_is_complete() {
        assert!(!is_complete(const_cstr!("select 1")));
        assert!(is_complete(const_cstr!("select 1;")));
        assert!(!is_complete(const_cstr!("create table foo (")));
        // TODO(soon): assert!(!is_complete(const_cstr!("create table foo (;")));
    }
}
