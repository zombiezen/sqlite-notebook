use std::cmp::{min, Ordering};
use std::ffi::{c_char, c_int, c_uint, CStr};
use std::str;

use libsqlite3_sys::{sqlite3_strglob, sqlite3_strlike, sqlite3_strnicmp};

/// Compare the contents of two strings
/// using the same definition of case independence that SQLite uses internally
/// when comparing identifiers.
pub fn stricmp(left: impl AsRef<str>, right: impl AsRef<str>) -> Ordering {
    let left = left.as_ref();
    let right = right.as_ref();
    let common_length = min(left.len(), right.len());

    let result = unsafe {
        sqlite3_strnicmp(
            left.as_ptr() as *const c_char,
            right.as_ptr() as *const c_char,
            c_int::try_from(common_length).expect("strings too large"),
        )
    };
    if result > 0 {
        Ordering::Greater
    } else if result < 0 {
        Ordering::Less
    } else {
        left.len().cmp(&right.len())
    }
}

/// Reports whether `s` matches the [`LIKE`] pattern `glob`
/// with the given escape character.
/// For `s LIKE glob` without the `ESCAPE` clause,
/// use 0 for `escape_char`.
/// As with the `LIKE` operator, the `strlike` function is case insensitive -
/// equivalent upper and lower case ASCII characters match one another.
/// The `strlike` function matches Unicode characters,
/// though only ASCII characters are case folded.
///
/// [`LIKE`]: https://www.sqlite.org/lang_expr.html#like
pub fn strlike(glob: impl AsRef<CStr>, s: impl AsRef<CStr>, escape_char: char) -> bool {
    (unsafe {
        sqlite3_strlike(
            glob.as_ref().as_ptr(),
            s.as_ref().as_ptr(),
            escape_char as c_uint,
        )
    }) == 0
}

/// Reports whether `s` matches the [`GLOB`] pattern `glob`.
/// This function is case-sensitive.
///
/// [`GLOB`]: https://www.sqlite.org/lang_expr.html#glob
pub fn strglob(glob: impl AsRef<CStr>, s: impl AsRef<CStr>) -> bool {
    (unsafe { sqlite3_strglob(glob.as_ref().as_ptr(), s.as_ref().as_ptr()) }) == 0
}

#[cfg(test)]
mod tests {
    use zombiezen_const_cstr::const_cstr;

    use super::*;

    #[test]
    fn test_stricmp() {
        assert_eq!(stricmp("", ""), Ordering::Equal);
        assert_eq!(stricmp("a", "b"), Ordering::Less);
        assert_eq!(stricmp("b", "a"), Ordering::Greater);
        assert_eq!(stricmp("a", "B"), Ordering::Less);
        assert_eq!(stricmp("B", "a"), Ordering::Greater);
        assert_eq!(stricmp("a", "Aa"), Ordering::Less);
        assert_eq!(stricmp("Aa", "a"), Ordering::Greater);
        assert_eq!(stricmp("aA", "Aa"), Ordering::Equal);
    }

    #[test]
    fn test_strlike() {
        assert!(strlike(const_cstr!("foo"), const_cstr!("foo"), '\x00'));
        assert!(strlike(const_cstr!("%oob%"), const_cstr!("foobar"), '\x00'));
        assert!(strlike(const_cstr!("a\\%b"), const_cstr!("a\\b"), '\x00'));
        assert!(!strlike(const_cstr!("a\\%b"), const_cstr!("a\\b"), '\\'));
        assert!(strlike(const_cstr!("a\\%b"), const_cstr!("a%b"), '\\'));
        assert!(!strlike(
            const_cstr!("%bork%"),
            const_cstr!("foobar"),
            '\x00'
        ));
    }
}
