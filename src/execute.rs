use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{self, Write};
use std::iter;
use std::mem;

use anyhow::{anyhow, Result};
use csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
use serde::{ser::SerializeMap, Serialize, Serializer};
use serde_json::json;
use tracing::{debug_span, field};
use zombiezen_sqlite::{Connection, StepResult};
use zombiezen_zmq::Socket;

use crate::{reply, wire};

pub(crate) fn execute<S, K, P>(
    session_id: &str,
    auth: &wire::Authentication<S, K>,
    execution_counter: &mut i32,
    conn: &mut Connection,
    message: &wire::Message<P>,
    io_pub_socket: &mut Socket,
) -> Result<wire::ExecuteReply, wire::ErrorReply<'static>>
where
    S: AsRef<str>,
    K: AsRef<[u8]>,
    P: AsRef<[u8]>,
{
    let span = debug_span!(
        "execute",
        code = field::Empty,
        execution_count = field::Empty,
        silent = field::Empty,
        store_history = field::Empty,
    );

    let req_content: wire::ExecuteRequest =
        message
            .deserialize_content()
            .map_err(|err| wire::ErrorReply {
                exception_value: format!("parse request: {}", err).into(),
                ..wire::ErrorReply::new("ValueError")
            })?;
    span.record("code", &req_content.code);
    span.record("silent", req_content.silent);
    span.record("store_history", req_content.store_history);
    let _enter = span.enter();

    reply(
        session_id,
        auth,
        io_pub_socket,
        "status",
        iter::empty::<Vec<u8>>(),
        message.raw_header(),
        &json!({
            "execution_state": "busy",
        }),
    )
    .map_err(|err| wire::ErrorReply {
        exception_value: err.to_string().into(),
        ..wire::ErrorReply::new("IOError")
    })?;

    let execution_count = *execution_counter + 1;
    span.record("execution_count", execution_count);
    if !req_content.silent && req_content.store_history {
        *execution_counter += 1;
    }
    reply(
        session_id,
        auth,
        io_pub_socket,
        "execute_input",
        iter::empty::<Vec<u8>>(),
        message.raw_header(),
        &json!({
            "code": &req_content.code,
            "execution_count": execution_count,
        }),
    )
    .map_err(|err| wire::ErrorReply {
        exception_value: err.to_string().into(),
        execution_count: Some(execution_count),
        ..wire::ErrorReply::new("IOError")
    })?;

    match run_code(conn, &req_content.code) {
        Ok(mut data) => {
            if data.is_empty() {
                let _ = reply(
                    session_id,
                    auth,
                    io_pub_socket,
                    "clear_output",
                    iter::empty::<Vec<u8>>(),
                    message.raw_header(),
                    &json!({
                        "wait": false,
                    }),
                );
            }
            if !data.stdout.is_empty() {
                let _ = reply(
                    session_id,
                    auth,
                    io_pub_socket,
                    "stream",
                    iter::empty::<Vec<u8>>(),
                    message.raw_header(),
                    &json!({
                        "name": "stdout",
                        "text": mem::take(&mut data.stdout),
                    }),
                );
            }
            if !data.stderr.is_empty() {
                let _ = reply(
                    session_id,
                    auth,
                    io_pub_socket,
                    "stream",
                    iter::empty::<Vec<u8>>(),
                    message.raw_header(),
                    &json!({
                        "name": "stderr",
                        "text": mem::take(&mut data.stderr),
                    }),
                );
            }
            reply(
                session_id,
                auth,
                io_pub_socket,
                "execute_result",
                iter::empty::<Vec<u8>>(),
                message.raw_header(),
                &json!({
                    "execution_count": execution_count,
                    "data": data,
                    "metadata": {},
                }),
            )
            .map_err(|err| wire::ErrorReply {
                exception_value: err.to_string().into(),
                execution_count: Some(execution_count),
                ..wire::ErrorReply::new("IOError")
            })?;

            Ok(wire::ExecuteReply {
                execution_count,
                user_expressions: HashMap::new(),
            })
        }
        Err((lineno, col, err)) => {
            let err_string = err.to_string();
            let traceback = format!("{}:{}: {}", lineno, col, &err_string);
            let err_reply = wire::ErrorReply {
                execution_count: Some(execution_count),
                traceback: vec![traceback.into()],
                exception_value: err_string.into(),
                ..wire::ErrorReply::new(format!("{:?}", err.result_code()))
            };
            let _ = reply(
                session_id,
                auth,
                io_pub_socket,
                "error",
                iter::empty::<Vec<u8>>(),
                message.raw_header(),
                &json!({
                    "ename": err_reply.exception_name.clone(),
                    "evalue": err_reply.exception_value.clone(),
                    "traceback": err_reply.traceback.clone(),
                }),
            );

            Err(err_reply)
        }
    }
}

fn run_code(
    conn: &mut Connection,
    mut code: &str,
) -> Result<SQLiteOutput, (usize, usize, zombiezen_sqlite::Error)> {
    let mut result = SQLiteOutputBuilder::new();
    let orig_code = code;
    let map_err = |err: zombiezen_sqlite::Error| {
        let offset = orig_code.len() - code.len() + err.error_offset().unwrap_or(0);
        let (lineno, col) = str_position(orig_code, offset);
        (lineno, col, err)
    };
    let mut first_select = true;
    while !code.is_empty() {
        // Check for special directives.
        let (line, tail) = match code.find('\n') {
            Some(eol) => (&code[..eol], &code[eol + 1..]),
            None => (code, ""),
        };
        match code.bytes().next() {
            Some(b'.') => {
                let (lineno, _) = str_position(orig_code, orig_code.len() - code.len());
                process_dot_command(conn, line, lineno, &mut result).map_err(map_err)?;
                code = tail;
                continue;
            }
            Some(b'#') => {
                code = tail;
                continue;
            }
            _ => {
                if line.chars().all(char::is_whitespace) {
                    code = tail;
                    continue;
                }
            }
        }

        // It's SQL!
        let (mut stmt, tail) = match conn.prepare(code).map_err(map_err)? {
            Some(x) => x,
            None => break,
        };

        let column_count = stmt.column_count();
        if column_count > 0 {
            result.html.push_str("<table>\n<thead><tr>");
            for i in 0..column_count {
                let name = stmt.column_name(i).unwrap();
                let _ = write!(
                    &mut result.html,
                    "<th scope=\"col\">{}</th>",
                    EscapeHtml(&name)
                );
            }
            result.html.push_str("</tr></thead>\n<tbody>\n");

            if !first_select {
                result.plain.push_str("\n");
            }
        }
        loop {
            match stmt.step().map_err(map_err)? {
                StepResult::Done => break,
                StepResult::Row => {
                    result.html.push_str("<tr>");
                    for i in 0..column_count {
                        let val = stmt
                            .column_text(i)
                            .expect("column should be in range")
                            .map_or_else(
                                |err| String::from_utf8_lossy(err.as_bytes()),
                                |s| s.into(),
                            );

                        let _ = write!(&mut result.html, "<td>{}</td>", EscapeHtml(&val));

                        if i > 0 {
                            result.plain.push_str("|");
                        }
                        result.plain.push_str(&val);
                    }
                    result.html.push_str("</tr>\n");
                    result.plain.push_str("\n");
                }
            }
        }
        if column_count > 0 {
            result.html.push_str("</tbody></table>\n");
            first_select = false;
        }
        code = tail;
    }
    Ok(result.finish())
}

fn process_dot_command(
    conn: &mut Connection,
    line: &str,
    lineno: usize,
    result: &mut SQLiteOutputBuilder,
) -> Result<(), zombiezen_sqlite::Error> {
    let line = match DotCommandLine::from_str(line) {
        Ok(line) => line,
        Err(err) => {
            let err_str = err.to_string();
            let _ = writeln!(&mut result.stderr, "line {}: {}", lineno, &err_str);
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
                        let sql = stmt.column_text(0).unwrap().map_or_else(
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
                "line {}: unknown dot command {}",
                lineno, &line.name
            );
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct SQLiteOutput {
    plain: String,
    html: String,
    csv: String,

    stdout: String,
    stderr: String,
}

impl SQLiteOutput {
    fn is_empty(&self) -> bool {
        self.plain.is_empty() && self.html.is_empty() && self.csv.is_empty()
    }
}

impl Serialize for SQLiteOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.is_empty() {
            return serializer.serialize_map(Some(0))?.end();
        }
        let mut serializer = serializer.serialize_map(Some(3))?;
        serializer.serialize_entry("text/plain", &self.plain)?;
        serializer.serialize_entry("text/html", &self.html)?;
        serializer.serialize_entry("text/csv", &self.csv)?;
        serializer.end()
    }
}

#[derive(Debug)]
struct SQLiteOutputBuilder {
    plain: String,
    html: String,
    csv: CsvWriter<Vec<u8>>,

    stdout: String,
    stderr: String,
}

impl SQLiteOutputBuilder {
    fn new() -> Self {
        SQLiteOutputBuilder {
            plain: String::new(),
            html: String::new(),
            csv: CsvWriterBuilder::new()
                .flexible(true)
                .from_writer(Vec::new()),
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    fn finish(self) -> SQLiteOutput {
        let csv_bytes = self
            .csv
            .into_inner()
            .expect("writing to String should never fail");
        let csv = String::from_utf8(csv_bytes)
            .unwrap_or_else(|err| String::from_utf8_lossy(err.as_bytes()).into_owned());
        SQLiteOutput {
            plain: self.plain,
            html: self.html,
            csv,
            stdout: self.stdout,
            stderr: self.stderr,
        }
    }
}

impl Default for SQLiteOutputBuilder {
    fn default() -> Self {
        Self::new()
    }
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
                    match line.find('\'') {
                        None => {
                            args.push(line.into());
                            return Ok(DotCommandLine { name, args });
                        }
                        Some(arg_end) => {
                            let (arg, tail) = line.split_at(arg_end);
                            args.push(arg.into());
                            line = &tail[1..];
                        }
                    }
                }
                // TODO(soon): Double-quotes.
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

fn str_position(s: &str, off: usize) -> (usize, usize) {
    let mut lineno = 1;
    let mut col = 1;
    for c in s[..off].chars() {
        // TODO(soon): Tabs.
        match c {
            '\n' => {
                lineno += 1;
                col = 1;
            }
            _ => {
                col += 1;
            }
        }
    }
    (lineno, col)
}

#[derive(Clone, Copy, Debug)]
struct EscapeHtml<'a>(&'a str);

impl<'a> fmt::Display for EscapeHtml<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, c) in self.0.char_indices() {
            match c {
                '&' => f.write_str("&amp;")?,
                '<' => f.write_str("&lt;")?,
                '>' => f.write_str("&gt;")?,
                '\'' => f.write_str("&#39;")?,
                '"' => f.write_str("&#34;")?,
                _ => f.write_str(&self.0[i..i + c.len_utf8()])?,
            }
        }
        Ok(())
    }
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
    }
}
