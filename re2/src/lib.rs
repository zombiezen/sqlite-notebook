/**
Rust bindings to [RE2].

```rust
use re2::RE2;
# pub fn main() -> Result<(), Box<dyn std::error::Error>> {
let valid_id = RE2::new(r#"^[a-z]+\[[0-9]+\]$"#)?;
assert!(valid_id.easy_match("adam[23]", None));
assert!(valid_id.easy_match("eve[7]", None));
assert!(!valid_id.easy_match("Job[48]", None));
assert!(!valid_id.easy_match("snakey", None));
# Ok(())
# }
```

[RE2]: https://github.com/google/re2
*/
use std::cmp::min;
use std::ffi::{c_char, c_int};
use std::mem::MaybeUninit;
use std::ops::Range;
use std::ptr::{self, NonNull};
use std::str::FromStr;

use libcre2_sys::*;

mod error;
mod options;

pub use error::*;
pub use options::*;

/// A compiled regular expression.
#[derive(Debug)]
#[repr(transparent)]
pub struct RE2 {
    re: NonNull<cre2_regexp_t>,
}

impl RE2 {
    /// Build and return a new regular expression object representing the pattern.
    pub fn new(pattern: &(impl AsRef<str> + ?Sized)) -> Result<Self, Error> {
        RE2Options::default().build(pattern)
    }

    /// Return the number of capturing groups (parenthetical subexpressions) in the pattern.
    pub fn num_capturing_groups(&self) -> usize {
        unsafe { cre2_num_capturing_groups(self.re.as_ptr()) as usize }
    }

    /// Match a substring of the text referenced by `text` against the regular expression,
    /// reporting if the text matched.
    /// `pos` restricts the matching to the slice of the text.
    /// If `matches` is not `None`, then it will be filled with the ranges of captured subgroups,
    /// with the first element being set to the range of the entire match.
    /// Groups that are absent are set to `None`.
    pub fn r#match(
        &self,
        text: &str,
        pos: Range<usize>,
        anchor: Anchor,
        matches: Option<&mut [Option<Range<usize>>]>,
    ) -> bool {
        let n = c_int::try_from(text.len()).expect("Text too large");
        match matches {
            None => unsafe {
                cre2_match(
                    self.re.as_ptr(),
                    text.as_ptr() as *const c_char,
                    n,
                    pos.start as c_int,
                    pos.end as c_int,
                    anchor as cre2_anchor_t,
                    ptr::null_mut(),
                    0,
                ) != 0
            },
            Some(matches) => unsafe {
                let nmatch = min(1 + self.num_capturing_groups(), matches.len());
                let mut c_matches = vec![MaybeUninit::<cre2_string_t>::uninit(); nmatch];
                let text_ptr = text.as_ptr() as *const c_char;
                let result = cre2_match(
                    self.re.as_ptr(),
                    text_ptr,
                    n,
                    pos.start as c_int,
                    pos.end as c_int,
                    anchor as cre2_anchor_t,
                    c_matches.as_mut_ptr() as *mut cre2_string_t,
                    nmatch as c_int,
                ) != 0;
                if result {
                    for (i, m) in c_matches.into_iter().enumerate() {
                        let m = m.assume_init();
                        matches[i] = if !m.data.is_null() {
                            let start = usize::try_from(m.data.offset_from(text_ptr)).unwrap();
                            Some(start..(start + usize::try_from(m.length).unwrap()))
                        } else {
                            None
                        };
                    }
                } else {
                    for i in 0..nmatch {
                        matches[i] = None;
                    }
                }
                result
            },
        }
    }

    /// Match a substring of the text referenced by `text` against the regular expression,
    /// reporting if the text matched.
    /// If `matches` is not `None`, then it will be filled with the ranges of captured subgroups,
    /// with the first element being set to the range of the entire match.
    /// Groups that are absent are set to `None`.
    ///
    /// This is a convenience method for the common case of calling `match`
    /// with [`Anchor::Unanchored`] and the entire text.
    #[inline]
    pub fn easy_match(&self, text: &str, matches: Option<&mut [Option<Range<usize>>]>) -> bool {
        self.r#match(text, 0..text.len(), Anchor::Unanchored, matches)
    }
}

impl FromStr for RE2 {
    type Err = Error;

    fn from_str(pattern: &str) -> Result<Self, Self::Err> {
        Self::new(pattern)
    }
}

impl Drop for RE2 {
    fn drop(&mut self) {
        unsafe {
            cre2_delete(self.re.as_ptr());
        }
    }
}

/// Anchor point of [`RE2::match`].
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Anchor {
    /// No anchoring.
    Unanchored = cre2_anchor_t_CRE2_UNANCHORED as i32,
    /// Anchor at start only.
    AnchorStart = cre2_anchor_t_CRE2_ANCHOR_START as i32,
    /// Anchor at start and end.
    AnchorBoth = cre2_anchor_t_CRE2_ANCHOR_BOTH as i32,
}

#[cfg(test)]
mod tests {
    use std::array;

    use super::*;

    #[test]
    fn cre_matching_example() {
        let rex = RE2Options::new()
            .set_posix_syntax(true)
            .build("(ciao) (hello)")
            .unwrap();
        let text = "ciao hello";
        let mut matches: [_; 3] = array::from_fn(|_| None);
        assert!(rex.r#match(text, 0..text.len(), Anchor::Unanchored, Some(&mut matches)));
        assert_eq!(matches[0], Some(0..text.len()));
        assert_eq!(matches[1], Some(0..4));
        assert_eq!(matches[2], Some(5..10));
    }

    #[test]
    fn not_matching() {
        let rex = RE2Options::new()
            .set_posix_syntax(true)
            .build("(ciao) (hello)")
            .unwrap();
        let text = "bleh";
        let mut matches: [_; 3] = array::from_fn(|_| Some(42..123));
        assert!(!rex.r#match(text, 0..text.len(), Anchor::Unanchored, Some(&mut matches)));
        assert_eq!(matches[0], None);
        assert_eq!(matches[1], None);
        assert_eq!(matches[2], None);
    }

    #[test]
    fn empty_matches() {
        let rex = RE2Options::new()
            .set_posix_syntax(true)
            .build("()(foo)?(hello)")
            .unwrap();
        let text = "hello";
        let mut matches: [_; 4] = array::from_fn(|_| None);
        assert!(rex.r#match(text, 0..text.len(), Anchor::Unanchored, Some(&mut matches)));
        assert_eq!(matches[0], Some(0..5));
        assert_eq!(matches[1], Some(0..0));
        assert_eq!(matches[2], None);
        assert_eq!(matches[3], Some(0..5));
    }

    #[test]
    fn error_pattern() {
        let err = RE2::new("(").unwrap_err();
        assert_eq!(err.code(), ErrorCode::MissingParen);
    }
}
