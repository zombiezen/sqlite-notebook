use std::fmt;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

/// A source file position.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Position<P = PathBuf> {
    file_name: Option<P>,
    lineno: NonZeroUsize,
    column: usize,
}

impl<P> Position<P> {
    pub(crate) fn advance(&mut self, s: &str) {
        for c in s.chars() {
            // TODO(soon): Tabs.
            match c {
                '\n' => {
                    self.lineno = self.lineno.saturating_add(1);
                    self.column = 1;
                }
                _ => {
                    if self.column != 0 {
                        self.column += 1;
                    }
                }
            }
        }
    }

    /// Returns a new position with the function applied to the file name.
    pub(crate) fn map_file_name<F, Q>(self, f: F) -> Position<Q>
    where
        F: FnOnce(P) -> Q,
    {
        Position {
            file_name: self.file_name.map(f),
            lineno: self.lineno,
            column: self.column,
        }
    }

    /// Returns the 1-based line number.
    #[inline]
    #[allow(unused)]
    pub(crate) const fn lineno(&self) -> NonZeroUsize {
        self.lineno
    }

    /// Returns the 1-based column number if present.
    #[inline]
    pub(crate) const fn column(&self) -> Option<NonZeroUsize> {
        NonZeroUsize::new(self.column)
    }

    pub(crate) fn clear_column(&mut self) {
        self.column = 0;
    }
}

impl<P: AsRef<Path>> Position<P> {
    /// Returns a new position.
    #[inline]
    #[cfg(test)]
    pub(crate) const fn new(
        file_name: Option<P>,
        lineno: NonZeroUsize,
        column: Option<NonZeroUsize>,
    ) -> Self {
        Position {
            file_name,
            lineno,
            column: match column {
                Some(c) => c.get(),
                None => 0,
            },
        }
    }
    /// Returns the first position for a given file.
    #[inline]
    pub(crate) const fn start_of(file_name: Option<P>) -> Self {
        Position {
            file_name,
            lineno: unsafe { NonZeroUsize::new_unchecked(1) },
            column: 1,
        }
    }

    /// Returns a reference to the file name.
    #[inline]
    pub(crate) fn file_name(&self) -> Option<&Path> {
        self.file_name.as_ref().map(AsRef::as_ref)
    }
}

impl<P: AsRef<Path>> fmt::Display for Position<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(fname) = self.file_name() {
            write!(f, "{}:", fname.display())?;
        }
        write!(f, "{}", self.lineno)?;
        if let Some(col) = self.column() {
            write!(f, ":{}", col)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_empty() {
        let mut pos = Position::<PathBuf>::start_of(None);
        pos.advance("");
        assert_eq!(
            pos,
            Position::new(None, NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1))
        );
    }

    #[test]
    fn advance_word() {
        let mut pos = Position::<PathBuf>::start_of(None);
        pos.advance("abc");
        assert_eq!(
            pos,
            Position::new(None, NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(4))
        );
    }

    #[test]
    fn advance_line() {
        let mut pos = Position::<PathBuf>::start_of(None);
        pos.advance("abc\nx");
        assert_eq!(
            pos,
            Position::new(None, NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(2))
        );
    }

    #[test]
    fn advance_without_starting_col() {
        let mut pos = Position::<PathBuf>::new(None, NonZeroUsize::new(42).unwrap(), None);
        pos.advance("abc");
        assert_eq!(
            pos,
            Position::new(None, NonZeroUsize::new(42).unwrap(), None)
        );
        pos.advance("abc\nx");
        assert_eq!(
            pos,
            Position::new(None, NonZeroUsize::new(43).unwrap(), NonZeroUsize::new(2))
        );
    }
}
