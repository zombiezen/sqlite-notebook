use std::ffi::{c_char, c_int, c_void, CStr};
use std::mem;
use std::ptr;

use libsqlite3_sys::{
    sqlite3_free, sqlite3_malloc, sqlite3_set_authorizer, SQLITE_ALTER_TABLE, SQLITE_ANALYZE,
    SQLITE_ATTACH, SQLITE_CREATE_INDEX, SQLITE_CREATE_TABLE, SQLITE_CREATE_TEMP_INDEX,
    SQLITE_CREATE_TEMP_TABLE, SQLITE_CREATE_TEMP_TRIGGER, SQLITE_CREATE_TEMP_VIEW,
    SQLITE_CREATE_TRIGGER, SQLITE_CREATE_VIEW, SQLITE_CREATE_VTABLE, SQLITE_DELETE, SQLITE_DENY,
    SQLITE_DETACH, SQLITE_DROP_INDEX, SQLITE_DROP_TABLE, SQLITE_DROP_TEMP_INDEX,
    SQLITE_DROP_TEMP_TABLE, SQLITE_DROP_TEMP_TRIGGER, SQLITE_DROP_TEMP_VIEW, SQLITE_DROP_TRIGGER,
    SQLITE_DROP_VIEW, SQLITE_DROP_VTABLE, SQLITE_FUNCTION, SQLITE_IGNORE, SQLITE_INSERT, SQLITE_OK,
    SQLITE_PRAGMA, SQLITE_READ, SQLITE_RECURSIVE, SQLITE_REINDEX, SQLITE_SAVEPOINT, SQLITE_SELECT,
    SQLITE_TRANSACTION, SQLITE_UPDATE,
};

use crate::*;

pub(crate) type AuthorizerFn = Box<dyn Fn(AuthAction) -> AuthResult + 'static>;

impl Connection {
    /// Register an authorizer callback,
    /// replacing any previously set callback on the connection.
    /// The authorizer callback is invoked as SQL statements
    /// are being compiled by [`Conn::prepare`].
    #[doc(alias = "sqlite3_set_authorizer")]
    pub fn set_authorizer(&mut self, f: impl Fn(AuthAction) -> AuthResult + 'static) -> Result<()> {
        const BOX_SIZE: c_int = mem::size_of::<AuthorizerFn>() as c_int;
        let f = Box::new(f);
        let user_data = unsafe {
            let user_data = sqlite3_malloc(BOX_SIZE).cast::<AuthorizerFn>();
            ptr::write(user_data, f);
            user_data
        };
        let rc = ResultCode(unsafe {
            sqlite3_set_authorizer(self.as_ptr(), Some(authorizer_callback), user_data.cast())
        });
        if !rc.is_success() {
            return Err(self.as_ref().error().unwrap());
        }
        unsafe {
            free_authorizer(self.authorizer);
        }
        self.authorizer = user_data;
        Ok(())
    }

    /// Disable the authorizer.
    pub fn clear_authorizer(&mut self) -> Result<()> {
        let rc =
            ResultCode(unsafe { sqlite3_set_authorizer(self.as_ptr(), None, ptr::null_mut()) });
        if !rc.is_success() {
            return Err(self.as_ref().error().unwrap());
        }
        unsafe {
            free_authorizer(self.authorizer);
        }
        self.authorizer = ptr::null_mut();
        Ok(())
    }
}

unsafe extern "C" fn authorizer_callback(
    user_data: *mut c_void,
    op: c_int,
    arg1: *const c_char,
    arg2: *const c_char,
    database: *const c_char,
    trigger: *const c_char,
) -> c_int {
    let f = user_data.cast::<AuthorizerFn>().as_ref().unwrap();
    let Some(action) = AuthAction::new(
        op,
        if arg1.is_null() {
            None
        } else {
            Some(CStr::from_ptr(arg1))
        },
        if arg2.is_null() {
            None
        } else {
            Some(CStr::from_ptr(arg2))
        },
        if database.is_null() {
            None
        } else {
            Some(CStr::from_ptr(database))
        },
        if trigger.is_null() {
            None
        } else {
            Some(CStr::from_ptr(trigger))
        },
    ) else {
        return SQLITE_DENY;
    };
    f(action) as c_int
}

unsafe fn free_authorizer(p: *mut AuthorizerFn) {
    if p.is_null() {
        return;
    }
    ptr::drop_in_place(p);
    sqlite3_free(p.cast());
}

/// An action to be authorized.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AuthAction<'a> {
    op: AuthOp<'a>,
    database: Option<&'a CStr>,
    accessor: Option<&'a CStr>,
}

impl<'a> AuthAction<'a> {
    fn new(
        op: c_int,
        arg1: Option<&'a CStr>,
        arg2: Option<&'a CStr>,
        database: Option<&'a CStr>,
        accessor: Option<&'a CStr>,
    ) -> Option<Self> {
        let op = match op {
            SQLITE_CREATE_INDEX => AuthOp::CreateIndex {
                index_name: arg1?,
                table_name: arg2?,
                temp: false,
            },
            SQLITE_CREATE_TABLE => AuthOp::CreateTable {
                table_name: arg1?,
                temp: false,
            },
            SQLITE_CREATE_TEMP_INDEX => AuthOp::CreateIndex {
                index_name: arg1?,
                table_name: arg2?,
                temp: true,
            },
            SQLITE_CREATE_TEMP_TABLE => AuthOp::CreateTable {
                table_name: arg1?,
                temp: true,
            },
            SQLITE_CREATE_TEMP_TRIGGER => AuthOp::CreateTrigger {
                trigger_name: arg1?,
                table_name: arg2?,
                temp: true,
            },
            SQLITE_CREATE_TEMP_VIEW => AuthOp::CreateView {
                view_name: arg1?,
                temp: true,
            },
            SQLITE_CREATE_TRIGGER => AuthOp::CreateTrigger {
                trigger_name: arg1?,
                table_name: arg2?,
                temp: false,
            },
            SQLITE_CREATE_VIEW => AuthOp::CreateView {
                view_name: arg1?,
                temp: false,
            },
            SQLITE_DELETE => AuthOp::Delete { table_name: arg1? },
            SQLITE_DROP_INDEX => AuthOp::DropIndex {
                index_name: arg1?,
                table_name: arg2?,
                temp: false,
            },
            SQLITE_DROP_TABLE => AuthOp::DropTable {
                table_name: arg1?,
                temp: false,
            },
            SQLITE_DROP_TEMP_INDEX => AuthOp::DropIndex {
                index_name: arg1?,
                table_name: arg2?,
                temp: true,
            },
            SQLITE_DROP_TEMP_TABLE => AuthOp::DropTable {
                table_name: arg1?,
                temp: true,
            },
            SQLITE_DROP_TEMP_TRIGGER => AuthOp::DropTrigger {
                trigger_name: arg1?,
                table_name: arg2?,
                temp: true,
            },
            SQLITE_DROP_TEMP_VIEW => AuthOp::DropView {
                view_name: arg1?,
                temp: true,
            },
            SQLITE_DROP_TRIGGER => AuthOp::DropTrigger {
                trigger_name: arg1?,
                table_name: arg2?,
                temp: false,
            },
            SQLITE_DROP_VIEW => AuthOp::DropView {
                view_name: arg1?,
                temp: false,
            },
            SQLITE_INSERT => AuthOp::Insert { table_name: arg1? },
            SQLITE_PRAGMA => AuthOp::Pragma {
                pragma_name: arg1?,
                arg: arg2,
            },
            SQLITE_READ => AuthOp::Read {
                table_name: arg1?,
                column_name: arg2?,
            },
            SQLITE_SELECT => AuthOp::Select,
            SQLITE_TRANSACTION => AuthOp::Transaction { operation: arg1? },
            SQLITE_UPDATE => AuthOp::Update {
                table_name: arg1?,
                column_name: arg2?,
            },
            SQLITE_ATTACH => AuthOp::Attach { filename: arg1? },
            SQLITE_DETACH => AuthOp::Detach {
                database_name: arg1?,
            },
            SQLITE_ALTER_TABLE => AuthOp::AlterTable {
                database_name: arg1?,
                table_name: arg2?,
            },
            SQLITE_REINDEX => AuthOp::Reindex { index_name: arg1? },
            SQLITE_ANALYZE => AuthOp::Analyze { table_name: arg1? },
            SQLITE_CREATE_VTABLE => AuthOp::CreateVTable {
                table_name: arg1?,
                module_name: arg2?,
            },
            SQLITE_DROP_VTABLE => AuthOp::DropVTable {
                table_name: arg1?,
                module_name: arg2?,
            },
            SQLITE_FUNCTION => AuthOp::Function {
                function_name: arg1?,
            },
            SQLITE_SAVEPOINT => AuthOp::Savepoint {
                operation: arg1?,
                savepoint_name: arg2?,
            },
            SQLITE_RECURSIVE => AuthOp::Recursive,
            _ => return None,
        };
        Some(AuthAction {
            op,
            database,
            accessor,
        })
    }

    /// Returns the operation details.
    #[inline]
    pub fn operation(self) -> AuthOp<'a> {
        self.op
    }

    /// Returns the name of the inner-most trigger or view
    /// that is responsible for the access attempt
    /// or `None` if this access attempt is directly from top-level SQL code.
    #[inline]
    pub fn accessor(self) -> Option<&'a CStr> {
        self.accessor
    }

    /// Returns the name of the database (e.g. `main`, `temp`, etc.)
    /// this action affects if applicable.
    #[inline]
    pub fn database(self) -> Option<&'a CStr> {
        self.database
    }
}

/// An enumeration of SQLite statements and authorizable actions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AuthOp<'a> {
    CreateIndex {
        index_name: &'a CStr,
        table_name: &'a CStr,
        temp: bool,
    },
    CreateTable {
        table_name: &'a CStr,
        temp: bool,
    },
    CreateView {
        view_name: &'a CStr,
        temp: bool,
    },
    CreateTrigger {
        trigger_name: &'a CStr,
        table_name: &'a CStr,
        temp: bool,
    },
    Delete {
        table_name: &'a CStr,
    },
    DropIndex {
        index_name: &'a CStr,
        table_name: &'a CStr,
        temp: bool,
    },
    DropTable {
        table_name: &'a CStr,
        temp: bool,
    },
    DropTrigger {
        trigger_name: &'a CStr,
        table_name: &'a CStr,
        temp: bool,
    },
    DropView {
        view_name: &'a CStr,
        temp: bool,
    },
    Insert {
        table_name: &'a CStr,
    },
    Pragma {
        pragma_name: &'a CStr,
        arg: Option<&'a CStr>,
    },
    Read {
        table_name: &'a CStr,
        column_name: &'a CStr,
    },
    Select,
    Transaction {
        operation: &'a CStr,
    },
    Update {
        table_name: &'a CStr,
        column_name: &'a CStr,
    },
    Attach {
        filename: &'a CStr,
    },
    Detach {
        database_name: &'a CStr,
    },
    AlterTable {
        database_name: &'a CStr,
        table_name: &'a CStr,
    },
    Reindex {
        index_name: &'a CStr,
    },
    Analyze {
        table_name: &'a CStr,
    },
    CreateVTable {
        table_name: &'a CStr,
        module_name: &'a CStr,
    },
    DropVTable {
        table_name: &'a CStr,
        module_name: &'a CStr,
    },
    Function {
        function_name: &'a CStr,
    },
    Savepoint {
        operation: &'a CStr,
        savepoint_name: &'a CStr,
    },
    Recursive,
}

impl<'a> AuthOp<'a> {
    /// Returns the name of the index this action affects if any.
    pub fn index_name(self) -> Option<&'a CStr> {
        match self {
            AuthOp::CreateIndex { index_name, .. }
            | AuthOp::DropIndex { index_name, .. }
            | AuthOp::Reindex { index_name, .. } => Some(index_name),
            _ => None,
        }
    }

    /// Returns the name of the table this action affects if any.
    pub fn table_name(self) -> Option<&'a CStr> {
        match self {
            AuthOp::CreateIndex { table_name, .. }
            | AuthOp::CreateTable { table_name, .. }
            | AuthOp::CreateTrigger { table_name, .. }
            | AuthOp::Delete { table_name, .. }
            | AuthOp::DropIndex { table_name, .. }
            | AuthOp::DropTable { table_name, .. }
            | AuthOp::DropTrigger { table_name, .. }
            | AuthOp::Insert { table_name, .. }
            | AuthOp::Read { table_name, .. }
            | AuthOp::Update { table_name, .. }
            | AuthOp::AlterTable { table_name, .. }
            | AuthOp::Analyze { table_name, .. }
            | AuthOp::CreateVTable { table_name, .. }
            | AuthOp::DropVTable { table_name, .. } => Some(table_name),
            _ => None,
        }
    }

    /// Returns the name of the trigger this action affects if any.
    pub fn trigger_name(self) -> Option<&'a CStr> {
        match self {
            AuthOp::CreateTrigger { trigger_name, .. }
            | AuthOp::DropTrigger { trigger_name, .. } => Some(trigger_name),
            _ => None,
        }
    }

    /// Returns the name of the view this action affects if any.
    pub fn view_name(self) -> Option<&'a CStr> {
        match self {
            AuthOp::CreateView { view_name, .. } | AuthOp::DropView { view_name, .. } => {
                Some(view_name)
            }
            _ => None,
        }
    }

    /// Returns the name of the column this action affects if any.
    pub fn column_name(self) -> Option<&'a CStr> {
        match self {
            AuthOp::Read { column_name, .. } | AuthOp::Update { column_name, .. } => {
                Some(column_name)
            }
            _ => None,
        }
    }

    /// Returns one of `"BEGIN"`, `"COMMIT"`, `"RELEASE"`, or `"ROLLBACK"`
    /// for a transaction or savepoint statement.
    pub fn operation(self) -> Option<&'a CStr> {
        match self {
            AuthOp::Transaction { operation, .. } | AuthOp::Savepoint { operation, .. } => {
                Some(operation)
            }
            _ => None,
        }
    }

    /// Returns the name of the virtual table module this action affects if any.
    pub fn module_name(self) -> Option<&'a CStr> {
        match self {
            AuthOp::CreateVTable { module_name, .. } | AuthOp::DropVTable { module_name, .. } => {
                Some(module_name)
            }
            _ => None,
        }
    }
}

/// The result of an authorization callback.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AuthResult {
    /// Allow the statement to be compiled.
    Ok = SQLITE_OK as i32,
    /// Abort the SQL statement with an error.
    Deny = SQLITE_DENY as i32,
    /// Don't allow access, but don't generate an error.
    Ignore = SQLITE_IGNORE as i32,
}

impl AuthResult {
    /// Reports whether the result is [`AuthorizerResult::Ok`].
    #[inline]
    pub const fn is_ok(self) -> bool {
        match self {
            Self::Ok => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_authorizer() {
        let mut db =
            Connection::open(<&CStr>::default(), OpenFlags::default() | OpenFlags::MEMORY).unwrap();
        db.set_authorizer(|action| match action.operation() {
            AuthOp::CreateTable { .. } => AuthResult::Deny,
            _ => AuthResult::Ok,
        })
        .unwrap();
        db.prepare("SELECT 2 + 2;").0.unwrap();
        let result = db.prepare("CREATE TABLE foo(bar);").0;
        assert!(
            result
                .as_ref()
                .is_err_and(|err| err.result_code() == ResultCode::AUTH),
            "Authorization denial seemed to not take effect. Got: {:?}",
            &result
        );
    }
}
