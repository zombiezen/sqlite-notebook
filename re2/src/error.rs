use std::ffi::{c_int, CStr};
use std::fmt;
use std::mem::MaybeUninit;
use std::ptr::NonNull;

use libcre2_sys::*;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

#[derive(Clone, Debug)]
pub struct Error {
    code: ErrorCode,
    message: String,
    arg: String,
}

impl Error {
    pub(crate) fn from_re(re: NonNull<cre2_regexp_t>) -> Result<(), Error> {
        let code = {
            let code = unsafe { cre2_error_code(re.as_ptr()) };
            if code == cre2_error_code_t_CRE2_NO_ERROR as c_int {
                return Ok(());
            }
            ErrorCode::from_i32(code as i32).expect("Unknown RE2 error code")
        };
        let message = {
            let cstr = unsafe { CStr::from_ptr(cre2_error_string(re.as_ptr())) };
            String::from_utf8_lossy(cstr.to_bytes()).into_owned()
        };
        let arg = {
            let mut arg_struct = MaybeUninit::<cre2_string_t>::uninit();
            let arg_bytes: &[u8] = unsafe {
                cre2_error_arg(re.as_ptr(), arg_struct.as_mut_ptr());
                let arg = arg_struct.assume_init();
                std::slice::from_raw_parts(arg.data as *const u8, arg.length as usize)
            };
            String::from_utf8_lossy(arg_bytes).into_owned()
        };
        Err(Error { code, message, arg })
    }

    /// Returns the error's code.
    #[inline]
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns the error's message.
    #[inline]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the offending portion of the pattern.
    #[inline]
    pub fn arg(&self) -> &str {
        &self.arg
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for Error {}

/// Error codes.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, FromPrimitive)]
pub enum ErrorCode {
    /// Unexpected error.
    Internal = cre2_error_code_t_CRE2_ERROR_INTERNAL as i32,
    /// Bad escape sequence.
    BadEscape = cre2_error_code_t_CRE2_ERROR_BAD_ESCAPE as i32,
    /// Bad character class.
    BadCharClass = cre2_error_code_t_CRE2_ERROR_BAD_CHAR_CLASS as i32,
    /// Bad character class range.
    BadCharRange = cre2_error_code_t_CRE2_ERROR_BAD_CHAR_RANGE as i32,
    /// Missing closing ].
    MissingBracket = cre2_error_code_t_CRE2_ERROR_MISSING_BRACKET as i32,
    /// Missing closing ).
    MissingParen = cre2_error_code_t_CRE2_ERROR_MISSING_PAREN as i32,
    /// Trailing \ at end of regexp.
    TrailingBackslash = cre2_error_code_t_CRE2_ERROR_TRAILING_BACKSLASH as i32,
    /// Repeat argument missing, e.g. "*".
    RepeatArgument = cre2_error_code_t_CRE2_ERROR_REPEAT_ARGUMENT as i32,
    /// Bad repetition argument.
    RepeatSize = cre2_error_code_t_CRE2_ERROR_REPEAT_SIZE as i32,
    /// Bad repetition operator.
    RepeatOp = cre2_error_code_t_CRE2_ERROR_REPEAT_OP as i32,
    /// Bad perl operator.
    BadPerlOp = cre2_error_code_t_CRE2_ERROR_BAD_PERL_OP as i32,
    /// Invalid UTF-8 in regexp.
    BadUtf8 = cre2_error_code_t_CRE2_ERROR_BAD_UTF8 as i32,
    /// Bad named capture group.
    BadNamedCapture = cre2_error_code_t_CRE2_ERROR_BAD_NAMED_CAPTURE as i32,
    /// Pattern too large (compile failed).
    PatternTooLarge = cre2_error_code_t_CRE2_ERROR_PATTERN_TOO_LARGE as i32,
}
