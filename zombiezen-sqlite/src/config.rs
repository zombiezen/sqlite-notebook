use std::{ffi::c_int, mem::MaybeUninit};

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

use crate::{Conn, Connection, Result, ResultCode};

impl Conn {
    /// Returns the current value of the given database configuration flag.
    pub fn get_config(&self, flag: ConfigFlag) -> Result<bool> {
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

impl Connection {
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
