use std::fmt::Write;
use std::iter::{FusedIterator, Peekable};

use anyhow::Result;
use zombiezen_sqlite::{Connection, StepResult};

use super::*;

pub(super) fn process_dot_command(
    conn: &mut Connection,
    line: &str,
    lineno: usize,
    result: &mut SQLiteOutputBuilder,
) -> Result<(), zombiezen_sqlite::Error> {
    let line = match DotCommandLine::from_str(line) {
        Ok(line) => line,
        Err(err) => {
            let err_str = err.to_string();
            let _ = writeln!(&mut result.stderr, "line {lineno}: {}", &err_str);
            return Ok(());
        }
    };
    match line.name {
        "help" => {
            let _ = writeln!(&mut result.stdout, "This is help.");
        }
        "s" | "schema" => {
            let (mut stmt, _) = conn.prepare("SELECT sql FROM sqlite_master;")?.unwrap();
            loop {
                match stmt.step()? {
                    StepResult::Done => break,
                    StepResult::Row => {
                        let sql = stmt.column_text(0).map_or_else(
                            |err| String::from_utf8_lossy(err.as_bytes()),
                            |s| s.into(),
                        );
                        let _ = writeln!(&mut result.plain, "{};", &sql);
                        let _ = writeln!(&mut result.html, "<pre><code>{};</code></pre>", &sql);
                    }
                }
            }
        }
        _ => {
            let _ = writeln!(
                &mut result.stderr,
                "line {lineno}: unknown dot command {}",
                &line.name
            );
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DotCommandLine<'a> {
    name: &'a str,
    args: Vec<Cow<'a, str>>,
}

impl<'a> DotCommandLine<'a> {
    fn from_str(mut line: &'a str) -> Result<Self> {
        line = match line.strip_prefix(".") {
            Some(line) => line,
            None => {
                return Err(anyhow!(
                    "parse dot command: {:?} does not begin with '.'",
                    line
                ))
            }
        };
        line = line.trim_start();

        if line.is_empty() {
            return Err(anyhow!("parse dot command: line is blank"));
        }
        let name = match line.find(char::is_whitespace) {
            None => {
                return Ok(DotCommandLine {
                    name: line,
                    args: Vec::new(),
                })
            }
            Some(command_end) => {
                let (name, tail) = line.split_at(command_end);
                line = tail;
                name
            }
        };

        let mut args = Vec::new();
        loop {
            line = line.trim_start();
            match line.bytes().next() {
                None => return Ok(DotCommandLine { name, args }),
                Some(b'\'') => {
                    line = &line[1..];
                    let (arg, tail) = cut(line, "'");
                    args.push(arg.into());
                    match tail {
                        None => return Ok(DotCommandLine { name, args }),
                        Some(tail) => line = tail,
                    }
                }
                Some(b'"') => {
                    let (arg, tail) = unquote_c_string(line)?;
                    args.push(arg);
                    line = tail;
                }
                _ => match line.find(char::is_whitespace) {
                    None => {
                        args.push(line.into());
                        return Ok(DotCommandLine { name, args });
                    }
                    Some(arg_end) => {
                        let (arg, tail) = line.split_at(arg_end);
                        args.push(arg.into());
                        line = tail;
                    }
                },
            }
        }
    }
}

fn unquote_c_string<'a>(s: &'a str) -> Result<(Cow<'a, str>, &'a str)> {
    let s = s.strip_prefix('"').expect("C string must start with '\"'");

    let (arg, tail) = cut(s, "\"");
    if arg.find('\\').is_none() {
        return Ok((arg.into(), tail.unwrap_or("")));
    }
    let mut chars = s.chars();
    let mut iter = chars.by_ref().peekable();
    let mut arg = String::new();
    loop {
        match iter.next() {
            None => return Ok((arg.into(), "")),
            Some('"') => return Ok((arg.into(), chars.as_str())),
            Some('\\') => match iter.next() {
                None | Some('\\') => arg.push('\\'),
                Some('a') => arg.push('\x07'),
                Some('b') => arg.push('\x08'),
                Some('t') => arg.push('\t'),
                Some('n') => arg.push('\n'),
                Some('v') => arg.push('\x0b'),
                Some('f') => arg.push('\x0c'),
                Some('r') => arg.push('\r'),
                Some('x') => {
                    let mut hex_val = 0u8;
                    for digit in take_while_map(2, &mut iter, |c| c.to_digit(16)) {
                        hex_val = (hex_val << 4) | (digit as u8);
                    }
                    let c: char = hex_val.into();
                    if !c.is_ascii() {
                        return Err(anyhow!("parse dot command: non-ASCII hex escape found"));
                    }
                    arg.push(c);
                }
                Some(c @ '0'..='7') => {
                    let mut oct_val = c.to_digit(8).unwrap() as u8;
                    for digit in take_while_map(2, &mut iter, |c| c.to_digit(8)) {
                        oct_val = (oct_val << 3) | (digit as u8);
                    }
                    let c: char = oct_val.into();
                    if !c.is_ascii() {
                        return Err(anyhow!("parse dot command: non-ASCII octal escape found"));
                    }
                    arg.push(c);
                }
                Some(c) => arg.push(c),
            },
            Some(c) => arg.push(c),
        }
    }
}

/// The implementation of [`take_while_map`].
struct TakeWhileMap<'a, I: Iterator, F> {
    iter: &'a mut Peekable<I>,
    filter_map: F,
    take: usize,
}

/// Returns an iterator that takes at most n items
/// from an underlying `Peekable` iterator,
/// stopping once the `filter_map` function returns `None`.
fn take_while_map<'a, I, F, T>(
    n: usize,
    iter: &'a mut Peekable<I>,
    filter_map: F,
) -> TakeWhileMap<'a, I, F>
where
    I: Iterator,
    F: FnMut(&<I as Iterator>::Item) -> Option<T>,
{
    TakeWhileMap {
        iter,
        filter_map,
        take: n,
    }
}

impl<'a, T, I, F> Iterator for TakeWhileMap<'a, I, F>
where
    I: Iterator,
    F: FnMut(&<I as Iterator>::Item) -> Option<T>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.take == 0 {
            return None;
        }
        match self.iter.peek() {
            Some(x) => match (self.filter_map)(x) {
                Some(y) => {
                    let x = self.iter.next();
                    debug_assert!(x.is_some());
                    self.take -= 1;
                    Some(y)
                }
                None => {
                    self.take = 0;
                    None
                }
            },
            None => {
                self.take = 0;
                None
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.take))
    }
}

impl<'a, T, I, F> FusedIterator for TakeWhileMap<'a, I, F>
where
    I: Iterator,
    F: FnMut(&<I as Iterator>::Item) -> Option<T>,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dot_command_line() {
        assert!(DotCommandLine::from_str("").is_err());
        assert!(DotCommandLine::from_str(".").is_err());
        assert!(DotCommandLine::from_str(".  ").is_err());
        assert!(DotCommandLine::from_str(" .foo").is_err());

        assert_eq!(
            DotCommandLine::from_str(".foo").unwrap(),
            DotCommandLine {
                name: "foo",
                args: vec![],
            }
        );
        assert_eq!(
            DotCommandLine::from_str(".foo 1 2 3").unwrap(),
            DotCommandLine {
                name: "foo",
                args: vec!["1".into(), "2".into(), "3".into()],
            }
        );
        assert_eq!(
            DotCommandLine::from_str(".foo '1 2' 3").unwrap(),
            DotCommandLine {
                name: "foo",
                args: vec!["1 2".into(), "3".into()],
            }
        );
        assert_eq!(
            DotCommandLine::from_str(".foo \"1 2\" 3").unwrap(),
            DotCommandLine {
                name: "foo",
                args: vec!["1 2".into(), "3".into()],
            }
        );
        assert_eq!(
            DotCommandLine::from_str(".foo \"1\\n2\" 3").unwrap(),
            DotCommandLine {
                name: "foo",
                args: vec!["1\n2".into(), "3".into()],
            }
        );
        assert_eq!(
            DotCommandLine::from_str(".foo \"1\\\"2\" 3").unwrap(),
            DotCommandLine {
                name: "foo",
                args: vec!["1\"2".into(), "3".into()],
            }
        );
        assert_eq!(
            DotCommandLine::from_str(".foo \"\\x").unwrap(),
            DotCommandLine {
                name: "foo",
                args: vec!["\x00".into()],
            }
        );
        assert_eq!(
            DotCommandLine::from_str(".foo \"\\x123").unwrap(),
            DotCommandLine {
                name: "foo",
                args: vec!["\x123".into()],
            }
        );
        assert_eq!(
            DotCommandLine::from_str(".foo \"\\1237").unwrap(),
            DotCommandLine {
                name: "foo",
                args: vec!["\x537".into()],
            }
        );
    }
}
