use std::cmp::min;
use std::ffi::{c_int, c_void, CStr};
use std::marker::PhantomData;
use std::mem::{self, MaybeUninit};
use std::ptr::NonNull;

use bitflags::bitflags;
use errno::{errno, Errno};
use libc::size_t;
use libzmq_sys::{
    zmq_bind, zmq_close, zmq_connect, zmq_getsockopt, zmq_recv, zmq_send, ZMQ_DONTWAIT, ZMQ_PUB,
    ZMQ_RCVMORE, ZMQ_REP, ZMQ_REQ, ZMQ_ROUTER, ZMQ_SNDMORE, ZMQ_SUB,
};
use tracing::{field, trace_span};

use super::Context;

#[repr(isize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SocketType {
    /// A publisher that distributes data.
    Pub = ZMQ_PUB as isize,
    /// A subscriber that subscribes to data distributed by a publisher.
    Sub = ZMQ_SUB as isize,
    /// A client to send requests to and receive replies from a service.
    Req = ZMQ_REQ as isize,
    /// A service to receive requests from and send replies to a client.
    Rep = ZMQ_REP as isize,
    /// An advanced request/reply socket.
    /// When receiving messages, a `Router` socket shall prepend a message part
    /// containing the identity of the originating peer to the message
    /// before passing it to the application.
    /// When sending messages, a `Router` socket shall remove the first part of the message
    /// and use it to determine the identity of the peer the message shall be routed to.
    /// If the peer does not exist anymore the mssage shall be silently discarded.
    Router = ZMQ_ROUTER as isize,
}

#[derive(Debug)]
#[repr(transparent)]
pub struct Socket<'c> {
    pub(crate) ptr: NonNull<c_void>,
    pub(crate) context: PhantomData<&'c Context>,
}

impl<'c> Socket<'c> {
    pub fn bind(&mut self, endpoint: impl AsRef<CStr>) -> Result<(), Errno> {
        let endpoint = endpoint.as_ref();
        let rc = unsafe { zmq_bind(self.ptr.as_ptr(), endpoint.as_ptr()) };
        if rc == 0 {
            Ok(())
        } else {
            Err(errno())
        }
    }

    pub fn connect(&mut self, endpoint: impl AsRef<CStr>) -> Result<(), Errno> {
        let endpoint = endpoint.as_ref();
        let rc = unsafe { zmq_connect(self.ptr.as_ptr(), endpoint.as_ptr()) };
        if rc == 0 {
            Ok(())
        } else {
            Err(errno())
        }
    }

    /// Receives a message part from the socket and copies it into `buf`.
    pub fn recv(&mut self, buf: &mut [u8], flags: RecvFlags) -> Result<RecvResult, Errno> {
        let span = trace_span!(
            "zmq_recv",
            buf_len = buf.len(),
            part_len = field::Empty,
            more = field::Empty,
            ok = field::Empty,
            errno = field::Empty,
        );
        let _enter = span.enter();

        let n = unsafe {
            zmq_recv(
                self.ptr.as_ptr(),
                buf.as_mut_ptr() as *mut c_void,
                buf.len(),
                flags.bits() as c_int,
            )
        };
        if n < 0 {
            span.record("ok", false);
            let e = errno();
            span.record("errno", e.0);
            return Err(e);
        }
        span.record("ok", true);
        span.record("part_len", n);
        let more = {
            let mut more = MaybeUninit::<c_int>::uninit();
            let mut option_len = mem::size_of::<c_int>() as size_t;
            unsafe {
                zmq_getsockopt(
                    self.ptr.as_ptr() as *mut c_void,
                    ZMQ_RCVMORE as c_int,
                    more.as_mut_ptr() as *mut c_void,
                    &mut option_len,
                );
                more.assume_init() != 0
            }
        };
        span.record("more", more);
        Ok(RecvResult {
            n: n as usize,
            buf_len: buf.len(),
            more,
        })
    }

    /// Receive a message from the socket and copies it into `buf`.
    /// It returns a slice of `buf` for each part.
    /// If buf was not large enough to receive the entire message,
    /// `recv_message` will return a vector with the number of parts in the message,
    /// but the parts at the end may be truncated or empty.
    pub fn recv_message<'a>(
        &mut self,
        mut buf: &'a mut [u8],
        flags: RecvFlags,
    ) -> Result<Vec<&'a [u8]>, Errno> {
        let span = trace_span!("zmq_recv_message");
        let _enter = span.enter();

        let mut first_error: Option<Errno> = None;
        let mut msg = Vec::new();
        loop {
            match self.recv(buf, flags) {
                Err(Errno(libc::EINTR)) => { /* Retry */ }
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
                Ok(result) => {
                    let (head, tail) = buf.split_at_mut(result.buf_len());
                    msg.push(head as &[u8]);
                    buf = tail;
                    if !result.has_more() {
                        return Ok(msg);
                    }
                }
            }
        }
    }

    /// Queue a message part on the socket.
    /// Once `send` returns, the message has not necessarily been transmitted to the endpoint,
    /// but it is present in 0MQ's queue.
    pub fn send(&mut self, buf: &[u8], flags: SendFlags) -> Result<(), Errno> {
        let span = trace_span!(
            "zmq_send",
            len = buf.len(),
            more = flags.contains(SendFlags::SNDMORE),
            ok = field::Empty,
            errno = field::Empty,
        );
        let _enter = span.enter();

        let rc = unsafe {
            zmq_send(
                self.ptr.as_ptr(),
                buf.as_ptr() as *const c_void,
                buf.len(),
                flags.bits() as c_int,
            )
        };
        if rc >= 0 {
            span.record("ok", true);
            Ok(())
        } else {
            span.record("ok", false);
            let e = errno();
            span.record("errno", e.0);
            Err(e)
        }
    }

    /// Queue a multi-part message on the socket,
    /// returning the first error encountered.
    /// Once `send_message` returns, the message has not necessarily been transmitted to the endpoint,
    /// but it is present in 0MQ's queue.
    pub fn send_message<M, P>(&mut self, msg: M, flags: SendFlags) -> Result<(), Errno>
    where
        M: IntoIterator<Item = P>,
        P: AsRef<[u8]>,
    {
        let span = trace_span!("zmq_send_message");
        let _enter = span.enter();

        let mut iter = msg.into_iter().peekable();
        loop {
            let part = match iter.next() {
                Some(part) => part,
                None => return Ok(()),
            };
            let mut part_flags = flags;
            part_flags.set(SendFlags::SNDMORE, iter.peek().is_some());
            'send: loop {
                match self.send(part.as_ref(), part_flags) {
                    Ok(()) => break 'send,
                    Err(Errno(libc::EINTR)) => { /* Retry */ }
                    Err(e) => return Err(e),
                }
            }
        }
    }
}

impl<'a> Drop for Socket<'a> {
    fn drop(&mut self) {
        unsafe {
            zmq_close(self.ptr.as_ptr());
        }
    }
}

bitflags! {
    #[repr(transparent)]
    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct RecvFlags: i32 {
        const DONTWAIT = ZMQ_DONTWAIT as i32;
    }
}

#[derive(Clone, Debug)]
pub struct RecvResult {
    n: usize,
    buf_len: usize,
    more: bool,
}

impl RecvResult {
    /// Returns the number of bytes received.
    #[inline]
    pub fn part_len(&self) -> usize {
        self.n
    }

    /// Returns the number of bytes written to the buffer.
    #[inline]
    pub fn buf_len(&self) -> usize {
        min(self.n, self.buf_len)
    }

    /// Reports whether the message part was longer than the buffer provided.
    #[inline]
    pub fn is_truncated(&self) -> bool {
        self.n > self.buf_len
    }

    /// Reports whether there are subsequent parts in a multi-part message
    /// that can be obtained through more calls to [`Socket::recv`].
    #[inline]
    pub fn has_more(&self) -> bool {
        self.more
    }
}

bitflags! {
    #[repr(transparent)]
    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct SendFlags: i32 {
        const DONTWAIT = ZMQ_DONTWAIT as i32;
        /// Indicates that the message part is appended to a multi-part message
        /// that will be delivered atomically
        /// once the last message part is sent with `SNDMORE` not set.
        const SNDMORE = ZMQ_SNDMORE as i32;
    }
}
