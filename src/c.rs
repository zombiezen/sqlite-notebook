use std::ffi::{CString, OsStr};

pub(crate) fn cstring_until_first_nul<T>(t: T) -> CString
where
    T: Into<Vec<u8>>,
{
    CString::new(t).unwrap_or_else(|err| {
        let i = err.nul_position();
        let mut buf = err.into_vec();
        buf.truncate(i + 1);
        unsafe { CString::from_vec_with_nul_unchecked(buf) }
    })
}

#[cfg(unix)]
pub(crate) fn cstr_from_osstr(p: &(impl AsRef<OsStr> + ?Sized)) -> CString {
    use std::os::unix::ffi::OsStrExt;
    cstring_until_first_nul(p.as_ref().as_bytes())
}

#[cfg(not(unix))]
pub(crate) fn cstr_from_osstr(p: &(impl AsRef<OsStr> + ?Sized)) -> CString {
    cstring_until_first_nul(
        p.as_ref()
            .to_str()
            .expect("cannot convert invalid UTF-8 to a C string")
            .as_bytes(),
    )
}
