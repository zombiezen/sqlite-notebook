mod context;
mod poll;
mod socket;

use std::ffi::c_int;
use std::mem::MaybeUninit;

use libzmq_sys::zmq_version;

pub use self::context::*;
pub use self::poll::*;
pub use self::socket::*;

pub use errno::Errno;
pub use libc::EINTR;

pub fn version() -> (i32, i32, i32) {
    let mut major = MaybeUninit::<c_int>::uninit();
    let mut minor = MaybeUninit::<c_int>::uninit();
    let mut patch = MaybeUninit::<c_int>::uninit();
    unsafe {
        zmq_version(major.as_mut_ptr(), minor.as_mut_ptr(), patch.as_mut_ptr());
        (
            major.assume_init(),
            minor.assume_init(),
            patch.assume_init(),
        )
    }
}
