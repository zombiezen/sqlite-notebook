use std::ffi::{c_char, c_int, CStr};

use libsqlite3_sys::sqlite3_snprintf;
use zombiezen_const_cstr::const_cstr;

/// Converts the string into a single-quoted SQL string literal,
/// writing the zero-terminated result into `buf`.
pub fn quote(buf: &mut [u8], s: &(impl AsRef<CStr> + ?Sized)) {
    let n = buf.len().try_into().unwrap_or(c_int::MAX);
    unsafe {
        sqlite3_snprintf(
            n,
            buf.as_mut_ptr() as *mut c_char,
            const_cstr!("%Q").as_ptr(),
            s.as_ref().as_ptr(),
        );
    }
}

/// Converts the string into a double-quoted SQL string literal,
/// writing the zero-terminated result into `buf`.
pub fn quote_id(buf: &mut [u8], s: &(impl AsRef<CStr> + ?Sized)) {
    match buf.len() {
        0 => return,
        1 => {
            buf[0] = 0;
            return;
        }
        2 => {
            buf[0] = b'"';
            buf[1] = 0;
            return;
        }
        _ => {}
    }
    buf[0] = b'"';
    let n = (buf.len() - 2).try_into().unwrap_or(c_int::MAX);
    unsafe {
        sqlite3_snprintf(
            n,
            (&mut buf[1..]).as_mut_ptr() as *mut c_char,
            const_cstr!("%w").as_ptr(),
            s.as_ref().as_ptr(),
        );
    }
    let nul = buf
        .iter()
        .position(|&b| b == 0)
        .expect("sqlite3_snprintf did not write a NUL");
    buf[nul] = b'"';
    buf[nul + 1] = 0;
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use super::*;

    fn do_quote<F, S>(f: F, s: &S) -> CString
    where
        F: FnOnce(&mut [u8], &S),
        S: AsRef<CStr> + ?Sized,
    {
        let mut buf = vec![0; 4096];
        f(&mut buf, s);
        let nul = buf
            .iter()
            .position(|&b| b == 0)
            .expect("Expect to have zero byte");
        buf.truncate(nul + 1);
        CString::from_vec_with_nul(buf).unwrap()
    }

    #[test]
    fn test_quote() {
        assert_eq!(
            do_quote(quote, const_cstr!("").as_cstr()),
            CString::new("''").unwrap()
        );
        assert_eq!(
            do_quote(quote, const_cstr!("abc").as_cstr()),
            CString::new("'abc'").unwrap()
        );
        assert_eq!(
            do_quote(quote, const_cstr!("qu'ote").as_cstr()),
            CString::new("'qu''ote'").unwrap()
        );
    }

    #[test]
    fn test_quote_id() {
        assert_eq!(
            do_quote(quote_id, const_cstr!("").as_cstr()),
            CString::new("\"\"").unwrap()
        );
        assert_eq!(
            do_quote(quote_id, const_cstr!("abc").as_cstr()),
            CString::new("\"abc\"").unwrap()
        );
        assert_eq!(
            do_quote(quote_id, const_cstr!("qu'ote").as_cstr()),
            CString::new("\"qu'ote\"").unwrap()
        );
        assert_eq!(
            do_quote(quote_id, const_cstr!("qu\"ote").as_cstr()),
            CString::new("\"qu\"\"ote\"").unwrap()
        );
    }
}
