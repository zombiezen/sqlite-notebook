use std::ffi::{c_char, c_int, CStr};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};

use bitflags::bitflags;
use libsqlite3_sys::{
    sqlite3, sqlite3_close, sqlite3_db_readonly, sqlite3_open_v2, sqlite3_prepare_v2,
    sqlite3_txn_state, SQLITE_OPEN_CREATE, SQLITE_OPEN_FULLMUTEX, SQLITE_OPEN_MEMORY,
    SQLITE_OPEN_NOMUTEX, SQLITE_OPEN_READONLY, SQLITE_OPEN_READWRITE, SQLITE_OPEN_SHAREDCACHE,
    SQLITE_OPEN_URI, SQLITE_TXN_NONE, SQLITE_TXN_READ, SQLITE_TXN_WRITE,
};
use libsqlite3_sys::{
    sqlite3_db_config, SQLITE_DBCONFIG_DEFENSIVE, SQLITE_DBCONFIG_DQS_DDL, SQLITE_DBCONFIG_DQS_DML,
    SQLITE_DBCONFIG_ENABLE_FKEY, SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER,
    SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, SQLITE_DBCONFIG_ENABLE_QPSG,
    SQLITE_DBCONFIG_ENABLE_TRIGGER, SQLITE_DBCONFIG_ENABLE_VIEW,
    SQLITE_DBCONFIG_LEGACY_ALTER_TABLE, SQLITE_DBCONFIG_LEGACY_FILE_FORMAT,
    SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, SQLITE_DBCONFIG_RESET_DATABASE,
    SQLITE_DBCONFIG_REVERSE_SCANORDER, SQLITE_DBCONFIG_STMT_SCANSTATUS,
    SQLITE_DBCONFIG_TRIGGER_EQP, SQLITE_DBCONFIG_TRUSTED_SCHEMA, SQLITE_DBCONFIG_WRITABLE_SCHEMA,
};

use crate::*;

/// An owned connection.
#[repr(transparent)]
#[derive(Debug)]
pub struct Connection {
    ptr: NonNull<sqlite3>,
}

impl Connection {
    #[inline(always)]
    pub(crate) fn as_ptr(&self) -> *mut sqlite3 {
        self.ptr.as_ptr()
    }

    pub fn open(filename: impl AsRef<CStr>, flags: OpenFlags) -> Result<Connection> {
        let mut db = MaybeUninit::uninit();
        let rc = ResultCode(unsafe {
            sqlite3_open_v2(
                filename.as_ref().as_ptr(),
                db.as_mut_ptr(),
                flags.bits() as c_int,
                ptr::null(),
            )
        });
        let db = match NonNull::new(unsafe { db.assume_init() }) {
            Some(db) => db,
            None => return Err(ResultCode::NOMEM.to_result().unwrap_err()),
        };
        let mut conn = Connection { ptr: db }; // Now will drop properly.
        if rc != ResultCode::OK {
            return Err(conn.as_ref().error().unwrap());
        }
        conn.config(ConfigFlag::DoubleQuotedStringDML, false)?;
        conn.config(ConfigFlag::DoubleQuotedStringDDL, false)?;
        Ok(conn)
    }

    #[inline(always)]
    pub fn as_ref<'c>(&'c self) -> Conn<'c> {
        unsafe { Conn::new(self.ptr) }
    }

    #[inline]
    pub fn prepare<'c, 's>(&'c self, sql: &'s str) -> Result<Option<(Statement<'c>, &'s str)>> {
        self.as_ref().prepare(sql)
    }

    /// Sets a database configuration flag.
    pub fn config(&mut self, flag: ConfigFlag, value: bool) -> Result<()> {
        let rc = ResultCode(unsafe {
            sqlite3_db_config(
                self.as_ptr(),
                flag as c_int,
                value as c_int,
                std::ptr::null_mut::<c_int>(),
            )
        });
        rc.to_result().map(|_| ())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe {
            assert_eq!(
                ResultCode(sqlite3_close(self.ptr.as_ptr() as *mut sqlite3)),
                ResultCode::OK
            );
        }
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

/// A reference to a [`Connection`].
#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct Conn<'c> {
    db: NonNull<sqlite3>,
    phantom: PhantomData<&'c sqlite3>,
}

impl<'c> Conn<'c> {
    #[inline(always)]
    pub(crate) unsafe fn new(db: NonNull<sqlite3>) -> Self {
        Conn {
            db,
            phantom: PhantomData,
        }
    }

    pub(crate) fn error(self) -> Option<Error> {
        Error::get(self.db)
    }

    #[inline]
    pub(crate) fn as_ptr(self) -> *mut sqlite3 {
        self.db.as_ptr()
    }

    pub fn prepare<'s>(self, sql: &'s str) -> Result<Option<(Statement<'c>, &'s str)>> {
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
            let stmt = Statement::new(ptr);
            let tail = sql.split_at(n as usize).1;
            (stmt, tail)
        }))
    }

    /// Reports whether the given schema is attached as read-only.
    /// Returns `None` if the argument does not name a database on the connection.
    pub fn db_readonly(self, schema: &(impl AsRef<CStr> + ?Sized)) -> Option<bool> {
        let result = unsafe { sqlite3_db_readonly(self.as_ptr(), schema.as_ref().as_ptr()) };
        match result {
            -1 => None,
            0 => Some(false),
            1 => Some(true),
            _ => panic!("unhandled result {} from sqlite3_db_readonly", result),
        }
    }

    /// Returns the current transaction state of the given schema.
    /// If no schema is given, then the highest transaction state of any schema is returned.
    pub fn txn_state(
        self,
        schema: Option<&(impl AsRef<CStr> + ?Sized)>,
    ) -> Option<TransactionState> {
        let schema_ptr = schema
            .map(|s| s.as_ref().as_ptr())
            .unwrap_or_else(std::ptr::null);
        let result = unsafe { sqlite3_txn_state(self.as_ptr(), schema_ptr) };
        match result {
            -1 => None,
            SQLITE_TXN_NONE => Some(TransactionState::None),
            SQLITE_TXN_READ => Some(TransactionState::Read),
            SQLITE_TXN_WRITE => Some(TransactionState::Write),
            _ => panic!("unknown transaction state {}", result),
        }
    }

    /// Returns the current value of the given database configuration flag.
    pub fn get_config(self, flag: ConfigFlag) -> Result<bool> {
        unsafe {
            let mut val = MaybeUninit::<c_int>::uninit();
            let rc = ResultCode(sqlite3_db_config(
                self.as_ptr(),
                flag as c_int,
                -1 as c_int,
                val.as_mut_ptr(),
            ));
            rc.to_result().map(|_| val.assume_init() != 0)
        }
    }
}

/// Transaction state of a database file.
#[repr(i32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum TransactionState {
    None = SQLITE_TXN_NONE as c_int,
    Read = SQLITE_TXN_READ as c_int,
    Write = SQLITE_TXN_WRITE as c_int,
}

/// Enumeration of boolean [database connection configuration options].
///
/// [database connection configuration options]: https://www.sqlite.org/c3ref/c_dbconfig_defensive.html
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConfigFlag {
    EnableFKey = SQLITE_DBCONFIG_ENABLE_FKEY as i32,
    EnableTrigger = SQLITE_DBCONFIG_ENABLE_TRIGGER as i32,
    EnableView = SQLITE_DBCONFIG_ENABLE_VIEW as i32,
    FTS3Tokenizer = SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER as i32,
    EnableLoadExtension = SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION as i32,
    NoCheckpointOnClose = SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE as i32,
    EnableQueryPlannerStabilityGuarantee = SQLITE_DBCONFIG_ENABLE_QPSG as i32,
    TriggerExplainQueryPlan = SQLITE_DBCONFIG_TRIGGER_EQP as i32,
    ResetDatabase = SQLITE_DBCONFIG_RESET_DATABASE as i32,
    Defensive = SQLITE_DBCONFIG_DEFENSIVE as i32,
    WritableSchema = SQLITE_DBCONFIG_WRITABLE_SCHEMA as i32,
    LegacyAlterTable = SQLITE_DBCONFIG_LEGACY_ALTER_TABLE as i32,
    DoubleQuotedStringDDL = SQLITE_DBCONFIG_DQS_DDL as i32,
    DoubleQuotedStringDML = SQLITE_DBCONFIG_DQS_DML as i32,
    LegacyFileFormat = SQLITE_DBCONFIG_LEGACY_FILE_FORMAT as i32,
    TrustedSchema = SQLITE_DBCONFIG_TRUSTED_SCHEMA as i32,
    StmtScanStatus = SQLITE_DBCONFIG_STMT_SCANSTATUS as i32,
    ReverseScanOrder = SQLITE_DBCONFIG_REVERSE_SCANORDER as i32,
}
