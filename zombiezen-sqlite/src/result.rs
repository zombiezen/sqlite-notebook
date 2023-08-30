use std::ffi::{c_int, CStr};
use std::fmt;
use std::ptr::NonNull;

use libsqlite3_sys::{
    sqlite3, sqlite3_errmsg, sqlite3_error_offset, sqlite3_errstr, sqlite3_extended_errcode,
    SQLITE_ABORT, SQLITE_AUTH, SQLITE_BUSY, SQLITE_CANTOPEN, SQLITE_CONSTRAINT, SQLITE_CORRUPT,
    SQLITE_DONE, SQLITE_EMPTY, SQLITE_ERROR, SQLITE_FORMAT, SQLITE_FULL, SQLITE_INTERNAL,
    SQLITE_INTERRUPT, SQLITE_IOERR, SQLITE_LOCKED, SQLITE_MISMATCH, SQLITE_MISUSE, SQLITE_NOLFS,
    SQLITE_NOMEM, SQLITE_NOTADB, SQLITE_NOTFOUND, SQLITE_NOTICE, SQLITE_OK, SQLITE_PERM,
    SQLITE_PROTOCOL, SQLITE_RANGE, SQLITE_READONLY, SQLITE_ROW, SQLITE_SCHEMA, SQLITE_TOOBIG,
    SQLITE_WARNING,
};

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ResultCode(pub(crate) c_int);

impl ResultCode {
    pub const OK: ResultCode = ResultCode(SQLITE_OK);
    pub const ROW: ResultCode = ResultCode(SQLITE_ROW);
    pub const DONE: ResultCode = ResultCode(SQLITE_DONE);

    pub const ERROR: ResultCode = ResultCode(SQLITE_ERROR);
    pub const INTERNAL: ResultCode = ResultCode(SQLITE_INTERNAL);
    pub const PERM: ResultCode = ResultCode(SQLITE_PERM);
    pub const ABORT: ResultCode = ResultCode(SQLITE_ABORT);
    pub const BUSY: ResultCode = ResultCode(SQLITE_BUSY);
    pub const LOCKED: ResultCode = ResultCode(SQLITE_LOCKED);
    pub const NOMEM: ResultCode = ResultCode(SQLITE_NOMEM);
    pub const READONLY: ResultCode = ResultCode(SQLITE_READONLY);
    pub const INTERRUPT: ResultCode = ResultCode(SQLITE_INTERRUPT);
    pub const IOERR: ResultCode = ResultCode(SQLITE_IOERR);
    pub const CORRUPT: ResultCode = ResultCode(SQLITE_CORRUPT);
    pub const NOTFOUND: ResultCode = ResultCode(SQLITE_NOTFOUND);
    pub const FULL: ResultCode = ResultCode(SQLITE_FULL);
    pub const CANTOPEN: ResultCode = ResultCode(SQLITE_CANTOPEN);
    pub const PROTOCOL: ResultCode = ResultCode(SQLITE_PROTOCOL);
    pub const EMPTY: ResultCode = ResultCode(SQLITE_EMPTY);
    pub const SCHEMA: ResultCode = ResultCode(SQLITE_SCHEMA);
    pub const TOOBIG: ResultCode = ResultCode(SQLITE_TOOBIG);
    pub const CONSTRAINT: ResultCode = ResultCode(SQLITE_CONSTRAINT);
    pub const MISMATCH: ResultCode = ResultCode(SQLITE_MISMATCH);
    pub const MISUSE: ResultCode = ResultCode(SQLITE_MISUSE);
    pub const NOLFS: ResultCode = ResultCode(SQLITE_NOLFS);
    pub const AUTH: ResultCode = ResultCode(SQLITE_AUTH);
    pub const FORMAT: ResultCode = ResultCode(SQLITE_FORMAT);
    pub const RANGE: ResultCode = ResultCode(SQLITE_RANGE);
    pub const NOTADB: ResultCode = ResultCode(SQLITE_NOTADB);
    pub const NOTICE: ResultCode = ResultCode(SQLITE_NOTICE);
    pub const WARNING: ResultCode = ResultCode(SQLITE_WARNING);

    #[inline]
    pub const fn to_primary(self) -> ResultCode {
        ResultCode(self.0 & 0xff)
    }

    #[inline]
    pub const fn is_success(self) -> bool {
        match self.to_primary() {
            ResultCode::OK | ResultCode::ROW | ResultCode::DONE => true,
            _ => false,
        }
    }

    /// Returns the English-language text that describes the result code.
    pub fn message(self) -> &'static str {
        let s = unsafe { CStr::from_ptr(sqlite3_errstr(self.0)) };
        s.to_str().unwrap_or("unknown error")
    }

    /// Converts a result code to a [`Result`].
    /// Successful codes will be a `Ok` of the code itself
    /// and unsuccessful codes will be converted into an [`Error`].
    pub fn to_result(self) -> Result<ResultCode> {
        if self.is_success() {
            Ok(self)
        } else {
            Err(Error {
                result_code: self,
                msg: String::new(),
                error_offset: None,
            })
        }
    }
}

impl From<ResultCode> for Result<ResultCode> {
    fn from(rc: ResultCode) -> Self {
        rc.to_result()
    }
}

impl From<ResultCode> for c_int {
    fn from(rc: ResultCode) -> Self {
        rc.0
    }
}

impl Default for ResultCode {
    fn default() -> Self {
        ResultCode::OK
    }
}

impl fmt::Display for ResultCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// SQLite error.
#[derive(Clone, Debug)]
pub struct Error {
    result_code: ResultCode,
    msg: String,
    error_offset: Option<usize>,
}

impl Error {
    /// Returns a new `Error` with the given error code and message.
    ///
    /// # Panics
    ///
    /// Panics if `result_code` represents success.
    pub fn new(result_code: ResultCode, msg: impl Into<String>) -> Error {
        assert!(!result_code.is_success());
        Error {
            result_code,
            msg: msg.into(),
            error_offset: None,
        }
    }

    /// Extracts error information from the connection.
    pub(crate) fn get(db: NonNull<sqlite3>) -> Option<Self> {
        let result_code = ResultCode(unsafe { sqlite3_extended_errcode(db.as_ptr()) });
        if result_code.is_success() {
            return None;
        }
        let error_offset = unsafe { sqlite3_error_offset(db.as_ptr()) };
        let error_offset = if error_offset < 0 {
            None
        } else {
            Some(error_offset as usize)
        };
        let msg: &CStr = unsafe { CStr::from_ptr(sqlite3_errmsg(db.as_ptr())) };
        let msg = String::from_utf8_lossy(msg.to_bytes()).into_owned();
        Some(Error {
            result_code,
            msg,
            error_offset,
        })
    }

    #[inline]
    pub fn result_code(&self) -> ResultCode {
        self.result_code
    }

    #[inline]
    pub fn error_offset(&self) -> Option<usize> {
        self.error_offset
    }

    pub fn message(&self) -> &str {
        if self.msg.is_empty() {
            self.result_code.message()
        } else {
            &self.msg
        }
    }
}

impl From<&Error> for ResultCode {
    fn from(err: &Error) -> Self {
        err.result_code()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.msg.is_empty() {
            self.result_code.fmt(f)
        } else {
            f.write_str(&self.msg)
        }
    }
}
