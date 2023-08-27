use std::ffi::{c_int, c_void, CStr};
use std::marker::PhantomData;
use std::ptr::NonNull;

use errno::{errno, Errno};
use libzmq_sys::{
    zmq_ctx_new, zmq_ctx_set, zmq_ctx_term, zmq_socket, ZMQ_BLOCKY, ZMQ_IO_THREADS, ZMQ_IPV6,
};

use super::{Socket, SocketType};

#[derive(Debug)]
#[repr(transparent)]
pub struct Context(NonNull<c_void>);

impl Context {
    #[inline]
    pub fn new() -> Result<Context, Errno> {
        Self::builder().context()
    }

    #[inline]
    pub fn builder() -> ContextBuilder {
        ContextBuilder::new()
    }

    fn set(&mut self, option_name: c_int, option_value: c_int) -> Result<(), Errno> {
        let rc = unsafe { zmq_ctx_set(self.0.as_ptr(), option_name, option_value) };
        if rc == 0 {
            Ok(())
        } else {
            Err(errno())
        }
    }

    pub fn socket(&self, typ: SocketType) -> Result<Socket, Errno> {
        match NonNull::new(unsafe { zmq_socket(self.0.as_ptr(), typ as c_int) }) {
            Some(ptr) => Ok(Socket {
                ptr,
                context: PhantomData,
            }),
            None => Err(errno::errno()),
        }
    }

    /// Returns a [`SocketType::Rep`] socket that binds to zero or more endpoints.
    pub fn new_rep<I, S>(&self, endpoints: I) -> Result<Socket, Errno>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<CStr>,
    {
        self.new_bind(SocketType::Rep, endpoints)
    }

    /// Returns a [`SocketType::Pub`] socket that binds to zero or more endpoints.
    pub fn new_pub<I, S>(&self, endpoints: I) -> Result<Socket, Errno>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<CStr>,
    {
        self.new_bind(SocketType::Pub, endpoints)
    }

    /// Returns a [`SocketType::Router`] socket that binds to zero or more endpoints.
    pub fn new_router<I, S>(&self, endpoints: I) -> Result<Socket, Errno>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<CStr>,
    {
        self.new_bind(SocketType::Router, endpoints)
    }

    fn new_bind<I, S>(&self, typ: SocketType, endpoints: I) -> Result<Socket, Errno>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<CStr>,
    {
        let mut s = self.socket(typ)?;
        for endpoint in endpoints {
            s.bind(endpoint)?;
        }
        Ok(s)
    }
}

unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe {
            zmq_ctx_term(self.0.as_ptr());
        }
    }
}

#[derive(Clone, Debug)]
pub struct ContextBuilder {
    io_threads: c_int,
}

impl ContextBuilder {
    #[inline]
    pub fn new() -> ContextBuilder {
        ContextBuilder { io_threads: 1 }
    }

    pub fn io_threads(&mut self, io_threads: u32) -> &mut ContextBuilder {
        self.io_threads = c_int::try_from(io_threads).expect("I/O thread count exceeds C int");
        self
    }

    pub fn context(&self) -> Result<Context, Errno> {
        match NonNull::new(unsafe { zmq_ctx_new() }) {
            None => Err(errno::errno()),
            Some(ctx) => {
                let mut ctx = Context(ctx);
                ctx.set(ZMQ_IPV6 as c_int, 1)?;
                ctx.set(ZMQ_BLOCKY as c_int, 0)?;
                ctx.set(ZMQ_IO_THREADS as c_int, self.io_threads)?;
                Ok(ctx)
            }
        }
    }
}

impl Default for ContextBuilder {
    fn default() -> Self {
        Self::new()
    }
}
