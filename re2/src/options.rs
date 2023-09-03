use std::{
    ffi::{c_char, c_int},
    ptr::{self, NonNull},
};

use lazy_static::lazy_static;
use libcre2_sys::*;

use crate::{Error, RE2};

/// Regular expression configuration options.
#[derive(Debug)]
#[repr(transparent)]
pub struct RE2Options {
    pub(crate) opt: *mut cre2_options_t,
}

impl RE2Options {
    /// Create a new set of default options.
    pub fn new() -> Self {
        RE2Options {
            opt: ptr::null_mut(),
        }
    }

    fn as_ptr(&self) -> *const cre2_options_t {
        if !self.opt.is_null() {
            self.opt as *const cre2_options_t
        } else {
            DEFAULT_OPTIONS.as_ptr()
        }
    }

    fn init(&mut self) -> NonNull<cre2_options_t> {
        match NonNull::new(self.opt) {
            Some(opts) => opts,
            None => {
                let ptr =
                    NonNull::new(unsafe { cre2_opt_new() }).expect("Options failed to allocate");
                unsafe {
                    cre2_opt_set_log_errors(ptr.as_ptr(), 0);
                }
                self.opt = ptr.as_ptr();
                ptr
            }
        }
    }

    /// Reports whether regexps built with this set of options
    /// are restricted to POSIX egrep syntax.
    pub fn posix_syntax(&self) -> bool {
        (unsafe { cre2_opt_posix_syntax(self.as_ptr() as *mut cre2_options_t) }) != 0
    }

    /// Restrict regexps to POSIX egrep syntax.
    pub fn set_posix_syntax(&mut self, enabled: bool) -> &mut Self {
        unsafe {
            cre2_opt_set_posix_syntax(self.init().as_ptr(), enabled as c_int);
        }
        self
    }

    /// Reports whether regexps built with this set of options
    /// will search for the longest match, not the first match.
    pub fn longest_match(&self) -> bool {
        (unsafe { cre2_opt_longest_match(self.as_ptr() as *mut cre2_options_t) }) != 0
    }

    /// Search for longest match, not first match.
    pub fn set_longest_match(&mut self, enabled: bool) -> &mut Self {
        unsafe {
            cre2_opt_set_longest_match(self.init().as_ptr(), enabled as c_int);
        }
        self
    }

    /// Reports whether regexps built with this set of options
    /// will interpret the pattern string as literal, not as regular expression.
    pub fn literal(&self) -> bool {
        (unsafe { cre2_opt_literal(self.as_ptr() as *mut cre2_options_t) }) != 0
    }

    /// Interpret the pattern string as literal, not as regular expression.
    pub fn set_literal(&mut self, enabled: bool) -> &mut Self {
        unsafe {
            cre2_opt_set_literal(self.init().as_ptr(), enabled as c_int);
        }
        self
    }

    /// Reports whether regexps built with this set of options
    /// will not match a newline character,
    /// even if it is in the regular expression pattern.
    pub fn never_nl(&self) -> bool {
        (unsafe { cre2_opt_never_nl(self.as_ptr() as *mut cre2_options_t) }) != 0
    }

    /// Never match a newline character,
    /// even if it is in the regular expression pattern.
    pub fn set_never_nl(&mut self, enabled: bool) -> &mut Self {
        unsafe {
            cre2_opt_set_never_nl(self.init().as_ptr(), enabled as c_int);
        }
        self
    }

    /// Reports whether regexps built with this set of options
    /// will interpret dots in the pattern as matching everything,
    /// including new lines.
    pub fn dot_nl(&self) -> bool {
        (unsafe { cre2_opt_dot_nl(self.as_ptr() as *mut cre2_options_t) }) != 0
    }

    /// The dot matches everything, including the new line.
    pub fn set_dot_nl(&mut self, enabled: bool) -> &mut Self {
        unsafe {
            cre2_opt_set_dot_nl(self.init().as_ptr(), enabled as c_int);
        }
        self
    }

    /// Reports whether regexps built with this set of options
    /// will parse all the parentheses as non-capturing.
    pub fn never_capture(&self) -> bool {
        (unsafe { cre2_opt_never_capture(self.as_ptr() as *mut cre2_options_t) }) != 0
    }

    /// Parse all the parentheses as non–capturing.
    pub fn set_never_capture(&mut self, enabled: bool) -> &mut Self {
        unsafe {
            cre2_opt_set_never_capture(self.init().as_ptr(), enabled as c_int);
        }
        self
    }

    /// Reports whether regexps built with this set of options
    /// are case-sensitive.
    pub fn case_sensitive(&self) -> bool {
        (unsafe { cre2_opt_case_sensitive(self.as_ptr() as *mut cre2_options_t) }) != 0
    }

    /// Match is case–sensitive;
    /// the regular expression pattern can override this setting with `(?i)`
    /// unless configured in POSIX syntax mode.
    pub fn set_case_sensitive(&mut self, enabled: bool) -> &mut Self {
        unsafe {
            cre2_opt_set_case_sensitive(self.init().as_ptr(), enabled as c_int);
        }
        self
    }

    /// Reports whether regexps built with this set of options
    /// allow Perl's `\d`, `\s`, `\w`, `\D`, `\S`, `\W`.
    pub fn perl_classes(&self) -> bool {
        (unsafe { cre2_opt_perl_classes(self.as_ptr() as *mut cre2_options_t) }) != 0
    }

    /// Allow Perl's `\d`, `\s`, `\w`, `\D`, `\S`, `\W`.
    pub fn set_perl_classes(&mut self, enabled: bool) -> &mut Self {
        unsafe {
            cre2_opt_set_perl_classes(self.init().as_ptr(), enabled as c_int);
        }
        self
    }

    /// Reports whether regexps built with this set of options
    /// allow Perl's `\b` and `\B`.
    pub fn word_boundary(&self) -> bool {
        (unsafe { cre2_opt_word_boundary(self.as_ptr() as *mut cre2_options_t) }) != 0
    }

    /// Allow Perl's `\b` and `\B`.
    pub fn set_word_boundary(&mut self, enabled: bool) -> &mut Self {
        unsafe {
            cre2_opt_set_word_boundary(self.init().as_ptr(), enabled as c_int);
        }
        self
    }

    /// Reports whether regexps built with this set of options
    /// will only match `^` and `$` at the beginning and end of the text, respectively.
    pub fn one_line(&self) -> bool {
        (unsafe { cre2_opt_one_line(self.as_ptr() as *mut cre2_options_t) }) != 0
    }

    /// The patterns `^` and `$` only match at the beginning and end of the text.
    pub fn set_one_line(&mut self, enabled: bool) -> &mut Self {
        unsafe {
            cre2_opt_set_one_line(self.init().as_ptr(), enabled as c_int);
        }
        self
    }

    /// Returns the maximum amount of memory in bytes
    /// that can be used to hold the compiled form of regular expressions
    /// built with this set of options
    /// and their cached DFA graphs.
    pub fn max_mem(&self) -> i64 {
        unsafe { cre2_opt_max_mem(self.as_ptr() as *mut cre2_options_t) }
    }

    /// Set the maximum amount of memory in bytes
    /// that can be used to hold the compiled form of the regular expressions
    /// and their cached DFA graphs.
    pub fn set_max_mem(&mut self, value: i64) -> &mut Self {
        unsafe {
            cre2_opt_set_max_mem(self.init().as_ptr(), value);
        }
        self
    }

    /// Build and return a new regular expression object representing the pattern.
    pub fn build(&self, pattern: &(impl AsRef<str> + ?Sized)) -> Result<RE2, Error> {
        let pattern = pattern.as_ref();
        let re = NonNull::new(unsafe {
            cre2_new(
                pattern.as_ptr() as *const c_char,
                pattern.len().try_into().expect("Pattern is too large"),
                self.as_ptr(),
            )
        })
        .expect("Failed to allocate regular expression");
        let re = RE2 { re };
        Error::from_re(re.re)?;
        Ok(re)
    }
}

impl Default for RE2Options {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for RE2Options {
    fn clone(&self) -> Self {
        let mut clone = RE2Options::new();
        clone.clone_from(self);
        clone
    }

    fn clone_from(&mut self, source: &Self) {
        self.set_posix_syntax(source.posix_syntax())
            .set_longest_match(source.longest_match())
            .set_literal(source.literal())
            .set_never_nl(source.never_nl())
            .set_dot_nl(source.dot_nl())
            .set_never_capture(source.never_capture())
            .set_case_sensitive(source.case_sensitive())
            .set_perl_classes(source.perl_classes())
            .set_word_boundary(source.word_boundary())
            .set_one_line(source.one_line())
            .set_max_mem(source.max_mem());
    }
}

impl Drop for RE2Options {
    fn drop(&mut self) {
        if !self.opt.is_null() {
            unsafe {
                cre2_opt_delete(self.opt);
            }
            self.opt = ptr::null_mut();
        }
    }
}

lazy_static! {
    static ref DEFAULT_OPTIONS: ConstOptions = {
        let opt = NonNull::new(unsafe { cre2_opt_new() }).expect("Options failed to allocate");
        unsafe {
            cre2_opt_set_log_errors(opt.as_ptr(), 0);
        }
        ConstOptions(opt)
    };
}

#[derive(Clone, Copy, Debug)]
struct ConstOptions(NonNull<cre2_options_t>);

impl ConstOptions {
    fn as_ptr(self) -> *const cre2_options_t {
        self.0.as_ptr() as *const cre2_options_t
    }
}

unsafe impl Send for ConstOptions {}
unsafe impl Sync for ConstOptions {}

#[cfg(test)]
mod tests {
    use crate::RE2Options;

    #[test]
    fn memory_is_positive() {
        let default_max_mem = RE2Options::new().max_mem();
        assert!(default_max_mem > 0);
    }
}
