use std::any::Any;
use std::ffi::{c_char, c_int, c_uchar, c_void, CStr};
use std::marker::PhantomData;
use std::mem;
use std::ptr::{self, NonNull};
use std::slice;
use std::str;

use crate::{bytearray, ColumnTextError, ColumnType, Conn, Connection, Result, ResultCode};

use bitflags::bitflags;
use libsqlite3_sys::{
    sqlite3_context, sqlite3_context_db_handle, sqlite3_create_function_v2, sqlite3_free,
    sqlite3_get_auxdata, sqlite3_malloc, sqlite3_result_error, sqlite3_result_error_code,
    sqlite3_result_error_toobig, sqlite3_result_int64, sqlite3_result_text64, sqlite3_result_value,
    sqlite3_set_auxdata, sqlite3_user_data, sqlite3_value, sqlite3_value_bytes, sqlite3_value_dup,
    sqlite3_value_free, sqlite3_value_text, sqlite3_value_type, SQLITE_DETERMINISTIC,
    SQLITE_DIRECTONLY, SQLITE_NULL, SQLITE_UTF8,
};

type ScalarFn = Box<dyn Fn(Context, &mut dyn ExactSizeIterator<Item = ProtectedValue>) + 'static>;

impl Connection {
    pub fn create_scalar_function(
        &mut self,
        name: &(impl AsRef<CStr> + ?Sized),
        n_arg: Option<u8>,
        flags: FunctionFlags,
        f: impl Fn(Context, &mut dyn ExactSizeIterator<Item = ProtectedValue>) + 'static,
    ) -> Result<()> {
        const BOX_SIZE: c_int = mem::size_of::<ScalarFn>() as c_int;
        let f = Box::new(f);
        let rc = ResultCode(unsafe {
            let app = sqlite3_malloc(BOX_SIZE) as *mut ScalarFn;
            ptr::write(app, f);
            sqlite3_create_function_v2(
                self.0.as_ptr(),
                name.as_ref().as_ptr(),
                n_arg.map_or(-1, |n| n as c_int),
                SQLITE_UTF8 | flags.bits(),
                app as *mut c_void,
                Some(scalar_callback),
                None,
                None,
                Some(destroy_scalar),
            )
        });
        if rc.is_success() {
            Ok(())
        } else {
            Err(self.as_ref().error().unwrap())
        }
    }
}

unsafe extern "C" fn scalar_callback(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let mut ctx = Context {
        ctx: NonNull::new(ctx).unwrap(),
        phantom_ref: PhantomData,
    };
    let app = match NonNull::new(ctx.user_data()) {
        Some(ptr) => ptr.cast::<ScalarFn>(),
        None => {
            ctx.result_error(ResultCode::ERROR, "null context");
            return;
        }
    };
    let arg_slice = slice::from_raw_parts_mut(argv, argc as usize);
    let mut arg_iter = arg_slice.iter().copied().map(|ptr| ProtectedValue {
        ptr: NonNull::new(ptr).unwrap(),
        phantom_ref: PhantomData,
    });
    let f = app.as_ref();
    f(ctx, &mut arg_iter);
}

unsafe extern "C" fn destroy_scalar(app: *mut c_void) {
    {
        let app = NonNull::new(app).unwrap().cast::<ScalarFn>();
        ptr::drop_in_place(app.as_ptr());
    }
    sqlite3_free(app);
}

#[repr(transparent)]
#[derive(Debug)]
pub struct Context<'a> {
    ctx: NonNull<sqlite3_context>,
    phantom_ref: PhantomData<&'a mut sqlite3_context>,
}

impl<'a> Context<'a> {
    #[inline]
    fn as_ptr(&self) -> *mut sqlite3_context {
        self.ctx.as_ptr()
    }

    fn user_data(&self) -> *mut c_void {
        unsafe { sqlite3_user_data(self.as_ptr()) }
    }

    pub fn db_handle<'b>(&'b self) -> Conn<'b> {
        Conn {
            db: NonNull::new(unsafe { sqlite3_context_db_handle(self.as_ptr()) }).unwrap(),
            phantom: PhantomData,
        }
    }

    pub fn result_error(&mut self, code: ResultCode, s: &str) {
        assert!(!code.is_success());
        let n = match c_int::try_from(s.len()) {
            Ok(n) => n,
            Err(_) => {
                unsafe {
                    sqlite3_result_error_toobig(self.as_ptr());
                }
                return;
            }
        };
        unsafe {
            sqlite3_result_error(self.as_ptr(), s.as_bytes().as_ptr() as *const i8, n);
            sqlite3_result_error_code(self.as_ptr(), code.into());
        }
    }

    pub fn result_value<V: Value + ?Sized>(&mut self, value: &V) {
        unsafe {
            sqlite3_result_value(self.as_ptr(), value.as_ptr());
        }
    }

    pub fn result_i64(&mut self, v: i64) {
        unsafe { sqlite3_result_int64(self.as_ptr(), v) }
    }

    pub fn result_text(&mut self, v: impl Into<String>) {
        let (ptr, n) = bytearray::new(v.into().into_bytes());
        unsafe {
            sqlite3_result_text64(
                self.as_ptr(),
                ptr.cast::<c_char>().as_ptr(),
                n as u64,
                Some(bytearray::destroy),
                SQLITE_UTF8 as c_uchar,
            )
        }
    }

    pub fn get_auxdata(&self, arg: usize) -> Option<&dyn Any> {
        let arg = c_int::try_from(arg).ok()?;
        let b: &Box<dyn Any> =
            unsafe { (sqlite3_get_auxdata(self.as_ptr(), arg) as *mut Box<dyn Any>).as_ref() }?;
        Some(b.as_ref())
    }

    /// Associates metadata with a non-aggregate SQL function's argument value.
    /// If the same value is passed to multiple invocations of the same SQL function during query execution,
    /// under some circumstances the associated metadata may be preserved.
    /// An example of where this might be useful is in a regular-expression matching function.
    /// The compiled version of the regular expression
    /// can be stored as metadata associated with the pattern string.
    /// Then as long as the pattern string remains the same,
    /// the compiled regular expression can be reused on multiple invocations of the same function.
    pub fn set_auxdata(&mut self, arg: usize, data: Box<dyn Any>) {
        let Ok(arg) = c_int::try_from(arg) else {
            // If the index is out of bounds, then just drop the data.
            return;
        };
        let b: Box<Box<dyn Any>> = Box::new(data); // Need an extra box to hold the thick pointer.
        let ptr = Box::into_raw(b);
        unsafe {
            sqlite3_set_auxdata(
                self.as_ptr(),
                arg as c_int,
                ptr as *mut c_void,
                Some(destroy_auxdata),
            );
        }
    }
}

unsafe extern "C" fn destroy_auxdata(ptr: *mut c_void) {
    let b: Box<Box<dyn Any>> = Box::from_raw(ptr as *mut Box<dyn Any>);
    drop(b);
}

/// A reference to a SQLite value.
#[derive(Clone, Copy, Debug)]
pub struct UnprotectedValue<'a> {
    ptr: NonNull<sqlite3_value>,
    phantom_ref: PhantomData<&'a sqlite3_value>,
}

impl<'a> UnprotectedValue<'a> {
    #[inline]
    pub(crate) fn new(ptr: NonNull<sqlite3_value>) -> Self {
        UnprotectedValue {
            ptr,
            phantom_ref: PhantomData,
        }
    }
}

impl<'a> Value for UnprotectedValue<'a> {
    fn as_ptr(&self) -> *mut sqlite3_value {
        self.ptr.as_ptr()
    }
}

unsafe impl<'a> Send for UnprotectedValue<'a> {}
unsafe impl<'a> Sync for UnprotectedValue<'a> {}

/// A mutex-protected reference to a SQLite value.
#[repr(transparent)]
#[derive(Debug)]
pub struct ProtectedValue<'a> {
    ptr: NonNull<sqlite3_value>,
    phantom_ref: PhantomData<&'a mut sqlite3_value>,
}

impl<'a> ProtectedValue<'a> {
    /// Reports whether the value represents `NULL`.
    #[inline]
    pub fn is_null(&self) -> bool {
        (unsafe { sqlite3_value_type(self.as_ptr()) }) == SQLITE_NULL
    }

    /// Returns the type of the value.
    pub fn r#type(&self) -> ColumnType {
        ColumnType::from_int(unsafe { sqlite3_value_type(self.ptr.as_ptr()) }).unwrap()
    }

    pub fn text<'b>(&'b mut self) -> Result<&'b str, ColumnTextError<'b>> {
        let bytes = unsafe {
            let ptr = sqlite3_value_text(self.ptr.as_ptr());
            if ptr.is_null() {
                return Ok("");
            }
            let n = sqlite3_value_bytes(self.ptr.as_ptr());
            slice::from_raw_parts(ptr, n as usize)
        };
        str::from_utf8(bytes).map_err(|err| ColumnTextError { bytes, err })
    }
}

impl<'a> Value for ProtectedValue<'a> {
    fn as_ptr(&self) -> *mut sqlite3_value {
        self.ptr.as_ptr()
    }
}

/// An owned SQLite value.
#[repr(transparent)]
#[derive(Debug)]
pub struct DupValue {
    ptr: NonNull<sqlite3_value>,
}

impl DupValue {
    /// Borrows the value as a protected value.
    pub fn as_mut<'a>(&'a mut self) -> ProtectedValue<'a> {
        ProtectedValue {
            ptr: self.ptr,
            phantom_ref: PhantomData,
        }
    }
}

impl Value for DupValue {
    fn as_ptr(&self) -> *mut sqlite3_value {
        self.ptr.as_ptr()
    }
}

impl Clone for DupValue {
    fn clone(&self) -> Self {
        self.dup()
    }
}

impl Drop for DupValue {
    fn drop(&mut self) {
        unsafe { sqlite3_value_free(self.ptr.as_ptr()) }
    }
}

/// Trait that is one of [`UnprotectedValue`], [`ProtectedValue`], or [`DupValue`].
/// This trait is sealed and cannot be implemented for types outside this crate.
pub trait Value: private::Sealed {
    /// Make an owned copy of the value.
    fn dup(&self) -> DupValue {
        DupValue {
            ptr: NonNull::new(unsafe { sqlite3_value_dup(self.as_ptr()) }).expect("out of memory"),
        }
    }

    #[doc(hidden)]
    fn as_ptr(&self) -> *mut sqlite3_value;
}

mod private {
    pub trait Sealed {}

    impl<'a> Sealed for super::UnprotectedValue<'a> {}
    impl<'a> Sealed for super::ProtectedValue<'a> {}
    impl Sealed for super::DupValue {}
}

bitflags! {
    #[repr(transparent)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct FunctionFlags: c_int {
        const DIRECTONLY = SQLITE_DIRECTONLY;
        const DETERMINISTIC = SQLITE_DETERMINISTIC;
    }
}

impl Default for FunctionFlags {
    fn default() -> Self {
        FunctionFlags::DIRECTONLY
    }
}

#[cfg(test)]
mod tests {
    use zombiezen_const_cstr::{const_cstr, ConstCStr};

    use crate::*;

    const MEMORY: ConstCStr = const_cstr!(":memory:");

    #[test]
    fn test_scalar_function() {
        let mut conn = Connection::open(MEMORY, OpenFlags::default()).unwrap();
        conn.create_scalar_function(
            const_cstr!("noop").as_cstr(),
            Some(1),
            FunctionFlags::default() | FunctionFlags::DETERMINISTIC,
            |mut ctx, args| {
                let arg = args.next().unwrap();
                ctx.result_value(&arg);
            },
        )
        .unwrap();
        let (mut stmt, _) = conn
            .prepare("select noop(123);")
            .unwrap()
            .expect("statement is not empty");
        assert_eq!(stmt.step().unwrap(), StepResult::Row);
        assert_eq!(stmt.column_type(0), ColumnType::Integer);
        assert_eq!(stmt.column_i64(0), 123);

        assert_eq!(stmt.step().unwrap(), StepResult::Done);
    }

    #[test]
    fn test_scalar_function_text() {
        let mut conn = Connection::open(MEMORY, OpenFlags::default()).unwrap();
        conn.create_scalar_function(
            const_cstr!("my_text").as_cstr(),
            Some(1),
            FunctionFlags::default() | FunctionFlags::DETERMINISTIC,
            |mut ctx, args| {
                let mut arg = args.next().unwrap();
                ctx.result_text(arg.text().unwrap_or(""));
            },
        )
        .unwrap();
        let (mut stmt, _) = conn
            .prepare("select my_text(123);")
            .unwrap()
            .expect("statement is not empty");
        assert_eq!(stmt.step().unwrap(), StepResult::Row);
        assert_eq!(stmt.column_type(0), ColumnType::Text);
        assert_eq!(stmt.column_text(0).unwrap(), "123");

        assert_eq!(stmt.step().unwrap(), StepResult::Done);
    }
}
