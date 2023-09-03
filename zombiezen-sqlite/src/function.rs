use std::any::Any;
use std::ffi::{c_char, c_int, c_uchar, c_void, CStr};
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::pin::Pin;
use std::ptr::{self, NonNull};
use std::slice;
use std::str;

use crate::{
    bytearray, ColumnTextError, ColumnType, Conn, PhantomUnsend, PhantomUnsync, Result, ResultCode,
};

use bitflags::bitflags;
use libsqlite3_sys::{
    sqlite3_context, sqlite3_context_db_handle, sqlite3_create_function_v2, sqlite3_free,
    sqlite3_get_auxdata, sqlite3_malloc, sqlite3_result_error, sqlite3_result_error_code,
    sqlite3_result_error_toobig, sqlite3_result_int64, sqlite3_result_text64, sqlite3_result_value,
    sqlite3_set_auxdata, sqlite3_user_data, sqlite3_value, sqlite3_value_bytes, sqlite3_value_dup,
    sqlite3_value_free, sqlite3_value_text, sqlite3_value_type, SQLITE_DETERMINISTIC,
    SQLITE_DIRECTONLY, SQLITE_NULL, SQLITE_UTF8,
};

type ScalarFn = Box<
    dyn Fn(Pin<&mut Context>, &mut dyn ExactSizeIterator<Item = Pin<&mut Value<Protected>>>)
        + 'static,
>;

impl Conn {
    pub fn create_scalar_function(
        &self,
        name: &(impl AsRef<CStr> + ?Sized),
        n_arg: Option<u8>,
        flags: FunctionFlags,
        f: impl Fn(Pin<&mut Context>, &mut dyn ExactSizeIterator<Item = Pin<&mut Value<Protected>>>)
            + 'static,
    ) -> Result<()> {
        const BOX_SIZE: c_int = mem::size_of::<ScalarFn>() as c_int;
        let f = Box::new(f);
        let rc = ResultCode(unsafe {
            let app = sqlite3_malloc(BOX_SIZE) as *mut ScalarFn;
            ptr::write(app, f);
            sqlite3_create_function_v2(
                self.as_ptr(),
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
            Err(self.error().unwrap())
        }
    }
}

unsafe extern "C" fn scalar_callback(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let mut ctx = Pin::new((ctx as *mut Context).as_mut().unwrap());
    let app = match NonNull::new(ctx.as_mut().user_data()) {
        Some(ptr) => ptr.cast::<ScalarFn>(),
        None => {
            ctx.result_error(ResultCode::ERROR, "null context");
            return;
        }
    };
    let arg_slice = slice::from_raw_parts_mut(argv, argc as usize);
    let mut arg_iter = arg_slice
        .iter()
        .copied()
        .map(|ptr| mem::transmute::<*mut sqlite3_value, Pin<&mut Value<Protected>>>(ptr));
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
pub struct Context {
    ctx: sqlite3_context,
    unsend: PhantomUnsend,
    unsync: PhantomUnsync,
}

impl Context {
    #[inline]
    fn as_ptr(&self) -> *mut sqlite3_context {
        ptr::addr_of!(self.ctx) as *mut sqlite3_context
    }

    fn user_data(self: Pin<&mut Self>) -> *mut c_void {
        unsafe { sqlite3_user_data(self.as_ptr()) }
    }

    pub fn db_handle(&self) -> &Conn {
        unsafe { &*(sqlite3_context_db_handle(self.as_ptr()) as *mut Conn) }
    }

    pub fn result_error(mut self: Pin<&mut Self>, code: ResultCode, s: &str) {
        assert!(!code.is_success());
        let n = match c_int::try_from(s.len()) {
            Ok(n) => n,
            Err(_) => {
                unsafe {
                    sqlite3_result_error_toobig(self.as_mut().as_ptr());
                }
                return;
            }
        };
        unsafe {
            sqlite3_result_error(
                self.as_mut().as_ptr(),
                s.as_bytes().as_ptr() as *const i8,
                n,
            );
            sqlite3_result_error_code(self.as_mut().as_ptr(), code.into());
        }
    }

    pub fn result_value<P: ValueProtection>(self: Pin<&mut Self>, value: &Value<P>) {
        unsafe {
            sqlite3_result_value(
                self.as_ptr(),
                // sqlite3_result_value does not actually modify referenced value,
                // but it isn't marked as const.
                &value.value as *const sqlite3_value as *mut sqlite3_value,
            );
        }
    }

    pub fn result_i64(self: Pin<&mut Self>, v: i64) {
        unsafe { sqlite3_result_int64(self.as_ptr(), v) }
    }

    pub fn result_text(self: Pin<&mut Self>, v: impl Into<String>) {
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
    pub fn set_auxdata(self: Pin<&mut Self>, arg: usize, data: Box<dyn Any>) {
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

#[repr(transparent)]
#[derive(Debug)]
pub struct Value<P: ValueProtection> {
    value: sqlite3_value,
    protection: PhantomData<P>,
}

impl<P: ValueProtection> Value<P> {
    pub fn dup(&self) -> DupValue {
        DupValue {
            dup_value: NonNull::new(unsafe {
                sqlite3_value_dup(&self.value as *const sqlite3_value) as *mut Value<Protected>
            })
            .expect("out of memory"),
        }
    }
}

impl Value<Protected> {
    #[inline]
    unsafe fn as_const_ptr(&self) -> *const sqlite3_value {
        ptr::addr_of!(self.value)
    }

    #[inline]
    fn as_ptr(mut self: Pin<&mut Self>) -> *mut sqlite3_value {
        ptr::addr_of_mut!(self.value)
    }

    /// Reports whether the value represents `NULL`.
    #[inline]
    pub fn is_null(&self) -> bool {
        (unsafe { sqlite3_value_type(self.as_const_ptr() as *mut sqlite3_value) }) == SQLITE_NULL
    }

    /// Returns the type of the value.
    pub fn r#type(&self) -> ColumnType {
        ColumnType::from_int(unsafe {
            sqlite3_value_type(self.as_const_ptr() as *mut sqlite3_value)
        })
        .unwrap()
    }

    pub fn text<'a>(mut self: Pin<&'a mut Self>) -> Result<&'a str, ColumnTextError> {
        let bytes = unsafe {
            let ptr = sqlite3_value_text(self.as_mut().as_ptr());
            if ptr.is_null() {
                return Ok("");
            }
            let n = sqlite3_value_bytes(self.as_mut().as_ptr());
            slice::from_raw_parts(ptr, n as usize)
        };
        str::from_utf8(bytes).map_err(|err| ColumnTextError { bytes, err })
    }
}

/// An owned SQLite value.
#[repr(transparent)]
#[derive(Debug)]
pub struct DupValue {
    dup_value: NonNull<Value<Protected>>,
}

impl DupValue {
    pub fn as_mut(&mut self) -> Pin<&mut Value<Protected>> {
        Pin::new(unsafe { self.dup_value.as_mut() })
    }
}

impl Deref for DupValue {
    type Target = Value<Protected>;

    fn deref(&self) -> &Value<Protected> {
        unsafe { self.dup_value.as_ref() }
    }
}

impl Clone for DupValue {
    fn clone(&self) -> Self {
        self.dup()
    }
}

impl Drop for DupValue {
    fn drop(&mut self) {
        unsafe { sqlite3_value_free(self.as_mut().as_ptr()) }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Unprotected {}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Protected {
    unsend: PhantomUnsend,
}

/// Trait that is either [`Unprotected`] or [`Protected`].
/// This trait is sealed and cannot be implemented for types outside this crate.
pub trait ValueProtection: private::Sealed {}

impl ValueProtection for super::Unprotected {}
impl ValueProtection for super::Protected {}

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

mod private {
    pub trait Sealed {}

    impl Sealed for super::Unprotected {}
    impl Sealed for super::Protected {}
}

#[cfg(test)]
mod tests {
    use zombiezen_const_cstr::{const_cstr, ConstCStr};

    use crate::*;

    const MEMORY: ConstCStr = const_cstr!(":memory:");

    #[test]
    fn test_scalar_function() {
        let conn = Connection::open(MEMORY, OpenFlags::default()).unwrap();
        conn.create_scalar_function(
            const_cstr!("noop").as_cstr(),
            Some(1),
            FunctionFlags::default() | FunctionFlags::DETERMINISTIC,
            |ctx, args| {
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
        let conn = Connection::open(MEMORY, OpenFlags::default()).unwrap();
        conn.create_scalar_function(
            const_cstr!("my_text").as_cstr(),
            Some(1),
            FunctionFlags::default() | FunctionFlags::DETERMINISTIC,
            |ctx, args| {
                let arg = args.next().unwrap();
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
