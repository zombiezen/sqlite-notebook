use std::ffi::{c_int, CStr};

use libsqlite3_sys::{sqlite3_txn_state, SQLITE_TXN_NONE, SQLITE_TXN_READ, SQLITE_TXN_WRITE};

use crate::Conn;

impl Conn {
    /// Returns the current transaction state of the given schema.
    /// If no schema is given, then the highest transaction state of any schema is returned.
    pub fn txn_state(
        &self,
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
}

/// Transaction state of a database file.
#[repr(i32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum TransactionState {
    None = SQLITE_TXN_NONE as c_int,
    Read = SQLITE_TXN_READ as c_int,
    Write = SQLITE_TXN_WRITE as c_int,
}
