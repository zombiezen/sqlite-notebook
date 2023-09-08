use std::fmt::{self, Write};

/// Helper struct for escaping strings as SQL.
pub struct Quote<'a> {
    str: &'a str,
    typ: QuoteType,
    with_quotes: bool,
}

impl<'a> Quote<'a> {
    /// Returns an object that implements [`Display`][std::fmt::Display]
    /// for printing the string as a single-quoted SQL string literal.
    #[inline]
    pub fn as_text(str: &'a (impl AsRef<str> + ?Sized)) -> Self {
        Self {
            str: str.as_ref(),
            typ: QuoteType::Text,
            with_quotes: true,
        }
    }

    /// Returns an object that implements [`Display`][std::fmt::Display]
    /// for printing the string as a double-quoted SQL identifier literal.
    #[inline]
    pub fn as_id(str: &'a (impl AsRef<str> + ?Sized)) -> Self {
        Self {
            str: str.as_ref(),
            typ: QuoteType::Identifier,
            with_quotes: true,
        }
    }

    /// Returns an object that implements [`Display`][std::fmt::Display]
    /// for printing the string as the contents of single-quoted SQL string literal.
    #[inline]
    pub fn as_text_without_quotes(str: &'a (impl AsRef<str> + ?Sized)) -> Self {
        Self {
            str: str.as_ref(),
            typ: QuoteType::Text,
            with_quotes: false,
        }
    }

    /// Returns an object that implements [`Display`][std::fmt::Display]
    /// for printing the string as the contents of a double-quoted SQL identifier literal.
    #[inline]
    pub fn as_id_without_quotes(str: &'a (impl AsRef<str> + ?Sized)) -> Self {
        Self {
            str: str.as_ref(),
            typ: QuoteType::Identifier,
            with_quotes: false,
        }
    }
}

impl<'a> fmt::Debug for Quote<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.str, f)
    }
}

impl<'a> fmt::Display for Quote<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let quote_char = self.typ.char();
        if self.with_quotes {
            f.write_char(quote_char)?;
        }
        if !self.str.contains(quote_char) {
            f.write_str(self.str)?;
        } else {
            for c in self.str.chars() {
                f.write_char(c)?;
                if c == quote_char {
                    f.write_char(c)?;
                }
            }
        }
        if self.with_quotes {
            f.write_char(quote_char)?;
        }
        Ok(())
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum QuoteType {
    Text,
    Identifier,
}

impl QuoteType {
    const fn char(self) -> char {
        match self {
            Self::Text => '\'',
            Self::Identifier => '"',
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_text() {
        assert_eq!(Quote::as_text("").to_string(), String::from("''"));
        assert_eq!(Quote::as_text("abc").to_string(), String::from("'abc'"));
        assert_eq!(
            Quote::as_text("qu'ote").to_string(),
            String::from("'qu''ote'")
        );
    }

    #[test]
    fn test_quote_id() {
        assert_eq!(Quote::as_id("").to_string(), String::from(r#""""#));
        assert_eq!(Quote::as_id("abc").to_string(), String::from(r#""abc""#));
        assert_eq!(
            Quote::as_id("qu'ote").to_string(),
            String::from(r#""qu'ote""#)
        );
        assert_eq!(
            Quote::as_id("qu\"ote").to_string(),
            String::from(r#""qu""ote""#)
        );
    }
}
