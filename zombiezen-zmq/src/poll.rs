use std::ffi::{c_int, c_long, c_short};
use std::os::fd::{AsRawFd, BorrowedFd};
use std::ptr;
use std::time::Duration;

use bitflags::bitflags;
use errno::{errno, Errno};
use libzmq_sys::{zmq_poll, zmq_pollitem_t, ZMQ_POLLERR, ZMQ_POLLIN, ZMQ_POLLOUT, ZMQ_POLLPRI};

use super::Socket;

/// Waits until any poll items have events are available
/// or the timeout elapses, whichever comes first.
pub fn poll<'a, 'c: 'a>(
    timeout: Option<Duration>,
    items: impl IntoIterator<Item = PollItem<'a, 'c>>,
) -> Result<Vec<(usize, Events)>, Errno> {
    let mut items: Vec<zmq_pollitem_t> = items.into_iter().map(|item| item.into_c()).collect();
    let timeout: c_long = match timeout {
        Some(d) => d.as_millis().try_into().map_err(|_| Errno(libc::EINVAL))?,
        None => -1,
    };
    let rc = unsafe { zmq_poll(items.as_mut_ptr(), items.len() as c_int, timeout) };
    if rc < 0 {
        return Err(errno());
    }
    Ok(items
        .into_iter()
        .enumerate()
        .filter_map(|(i, item)| {
            let events = Events::from_bits_truncate(item.revents);
            if events.is_empty() {
                None
            } else {
                Some((i, events))
            }
        })
        .collect())
}

#[derive(Debug)]
pub struct PollItem<'a, 'c> {
    target: PollTarget<'a, 'c>,
    events: Events,
}

impl<'a, 'c> PollItem<'a, 'c> {
    pub fn new(target: impl Into<PollTarget<'a, 'c>>, events: Events) -> Self {
        PollItem {
            target: target.into(),
            events,
        }
    }

    fn into_c(self) -> zmq_pollitem_t {
        let mut c = zmq_pollitem_t {
            socket: ptr::null_mut(),
            fd: 0,
            events: self.events.bits() as c_short,
            revents: 0,
        };
        match &self.target {
            PollTarget::Fd(fd) => c.fd = fd.as_raw_fd(),
            PollTarget::Socket(s) => c.socket = s.ptr.as_ptr(),
        }
        c
    }
}

#[derive(Debug)]
pub enum PollTarget<'a, 'c> {
    Socket(&'a mut Socket<'c>),
    Fd(BorrowedFd<'a>),
}

impl<'a, 'c> From<&'a mut Socket<'c>> for PollTarget<'a, 'c> {
    fn from(s: &'a mut Socket<'c>) -> Self {
        PollTarget::Socket(s)
    }
}

impl<'a, 'c> From<BorrowedFd<'a>> for PollTarget<'a, 'c> {
    fn from(fd: BorrowedFd<'a>) -> Self {
        PollTarget::Fd(fd)
    }
}

bitflags! {
    #[repr(transparent)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
    pub struct Events: i16 {
        const IN = ZMQ_POLLIN as i16;
        const OUT = ZMQ_POLLOUT as i16;
        const ERR = ZMQ_POLLERR as i16;
        const PRI = ZMQ_POLLPRI as i16;
    }
}
