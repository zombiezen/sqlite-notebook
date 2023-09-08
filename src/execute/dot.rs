use std::fmt::Write;
use std::iter::{FusedIterator, Peekable};
use std::str;
use std::{env, fs};

use anyhow::Result;
use csv::{ReaderBuilder as CsvReaderBuilder, StringRecord};
use tracing::debug;
use zombiezen_const_cstr::const_cstr;
use zombiezen_sqlite::{Conn, Connection, Quote, ResultCode, ResultExt, TransactionState};

use crate::{c::cstring_until_first_nul, open_conn};

use super::*;

pub(super) fn process_dot_command<'a>(
    conn: &mut Connection,
    position: Position<Cow<'a, Path>>,
    line: &str,
    result: &mut SQLiteOutputBuilder,
) -> Result<(), (Position<Cow<'a, Path>>, zombiezen_sqlite::Error)> {
    let line = match DotCommandLine::from_str(line) {
        Ok(line) => line,
        Err(err) => {
            return Err((
                position,
                zombiezen_sqlite::Error::new(ResultCode::ERROR, err.to_string()),
            ));
        }
    };
    match line.name {
        "help" => {
            if line.args.len() > 1 {
                return Err((
                    position,
                    zombiezen_sqlite::Error::new(
                        ResultCode::MISUSE,
                        "usage: .help ?-all? ?PATTERN?".to_string(),
                    ),
                ));
            }
            display_help(&mut result.stdout, line.args.get(0).map(AsRef::as_ref));
        }
        "import" => {
            if let Err(mut err) = import(conn, &line.args, &mut result.stderr) {
                err.clear_error_offset();
                return Err((position, err));
            }
        }
        "open" => {
            // TODO(someday): Other flags.
            if line.args.len() != 1 {
                return Err((
                    position,
                    zombiezen_sqlite::Error::new(
                        ResultCode::MISUSE,
                        "usage: .open FILE".to_string(),
                    ),
                ));
            }
            let cwd = env::current_dir().unwrap_or_else(|_| "<error>".into());
            debug!(
                path = line.args[0].as_ref(),
                cwd = cwd.display().to_string(),
                "Opening new database"
            );
            *conn = match open_conn(&cstring_until_first_nul(line.args[0].as_bytes())) {
                Ok(conn) => conn,
                Err(mut err) => {
                    err.clear_error_offset();
                    return Err((position, err));
                }
            };
        }
        "read" => {
            if line.args.len() != 1 {
                return Err((
                    position,
                    zombiezen_sqlite::Error::new(
                        ResultCode::MISUSE,
                        "usage: .read FILE".to_string(),
                    ),
                ));
            }
            let cwd = env::current_dir().unwrap_or_else(|_| "<error>".into());
            debug!(
                path = line.args[0].as_ref(),
                cwd = cwd.display().to_string(),
                "Reading SQL file"
            );
            let path: &Path = line.args[0].as_ref().as_ref();
            let contents = fs::read(path).map_err(|err| {
                (
                    position,
                    zombiezen_sqlite::Error::new(ResultCode::IOERR, err.to_string()),
                )
            })?;
            let contents = String::from_utf8_lossy(&contents);
            run_code(conn, Some(path), &contents, result).map_err(|(position, err)| {
                (position.map_file_name(|f| Cow::Owned(f.into_owned())), err)
            })?;
        }
        "databases" => {
            let databases = {
                let mut stmt = match conn.prepare("PRAGMA database_list").0 {
                    Ok(Some(stmt)) => stmt,
                    Ok(_) => unreachable!("Statement not empty"),
                    Err(mut err) => {
                        err.clear_error_offset();
                        return Err((position, err));
                    }
                };
                let mut databases = Vec::<(String, String)>::new();
                loop {
                    match stmt.step() {
                        Ok(StepResult::Done) => break,
                        Ok(StepResult::Row) => {
                            databases.push((
                                stmt.column_text(1).to_string_lossy().into_owned(),
                                stmt.column_text(2).to_string_lossy().into_owned(),
                            ));
                        }
                        Err(mut err) => {
                            err.clear_error_offset();
                            return Err((position, err));
                        }
                    }
                }
                databases
            };
            for (schema, file) in databases {
                debug!(schema = schema, file = file, "Returning database data");
                let c_schema = cstring_until_first_nul(schema);
                let rdonly = conn
                    .db_readonly(&c_schema)
                    .expect("database returned from list is now absent");
                let txn = conn
                    .txn_state(Some(&c_schema))
                    .expect("database returned from list is now absent");
                let _ = writeln!(
                    &mut result.stdout,
                    "{schema}: {file} {rdonly}{txn}",
                    schema =
                        str::from_utf8(c_schema.as_bytes()).expect("was already UTF-8 encoded"),
                    file = if file.is_empty() { "\"\"" } else { &file },
                    rdonly = if rdonly { "r/o" } else { "r/w" },
                    txn = match txn {
                        TransactionState::None => "",
                        TransactionState::Read => " read-txn",
                        TransactionState::Write => " write-txn",
                    },
                );
            }
        }
        "parameter" => match line.args.get(0).map(AsRef::as_ref) {
            Some("clear") if line.args.len() == 1 => {
                if let Err(mut err) = parameter::clear(conn) {
                    err.clear_error_offset();
                    return Err((position, err));
                }
            }
            Some("list") if line.args.len() == 1 => {
                if let Err(mut err) = parameter::list(result, conn) {
                    err.clear_error_offset();
                    return Err((position, err));
                }
            }
            Some("init") if line.args.len() == 1 => {
                if let Err(mut err) = parameter::init(conn) {
                    err.clear_error_offset();
                    return Err((position, err));
                }
            }
            Some("set") if line.args.len() == 3 => {
                if let Err(mut err) = parameter::set(conn, &line.args[1], &line.args[2]) {
                    err.clear_error_offset();
                    return Err((position, err));
                }
            }
            Some("unset") if line.args.len() == 2 => {
                if let Err(mut err) = parameter::unset(conn, &line.args[1]) {
                    err.clear_error_offset();
                    return Err((position, err));
                }
            }
            _ => {
                let mut msg = String::new();
                display_help(&mut msg, Some("parameter"));
                return Err((
                    position,
                    zombiezen_sqlite::Error::new(ResultCode::MISUSE, msg),
                ));
            }
        },
        "schema" => {
            if line.args.len() >= 2 {
                return Err((
                    position,
                    zombiezen_sqlite::Error::new(
                        ResultCode::MISUSE,
                        "usage: .schema ?LIKE-PATTERN?".to_string(),
                    ),
                ));
            }

            if let Some(pattern) = line.args.get(0) {
                let cpattern = cstring_until_first_nul(pattern.as_ref().to_owned());
                let is_schema =
                    zombiezen_sqlite::strlike(&cpattern, const_cstr!("sqlite_master"), '\\')
                        || zombiezen_sqlite::strlike(&cpattern, const_cstr!("sqlite_schema"), '\\')
                        || zombiezen_sqlite::strlike(
                            &cpattern,
                            const_cstr!("sqlite_temp_master"),
                            '\\',
                        )
                        || zombiezen_sqlite::strlike(
                            &cpattern,
                            const_cstr!("sqlite_temp_schema"),
                            '\\',
                        );
                if is_schema {
                    // TODO(soon): Use the name that actually matched.
                    // Don't worry, the _actual CLI_ has the same bug.
                    display_schema(result, &format!(include_str!("sqlite_schema.sql"), pattern));
                }
            }

            let schema_query = {
                let mut database_list_stmt =
                    match conn.prepare("SELECT name FROM pragma_database_list").0 {
                        Ok(Some(stmt)) => stmt,
                        Ok(None) => unreachable!("Statement not empty"),
                        Err(mut err) => {
                            err.clear_error_offset();
                            return Err((position, err));
                        }
                    };
                let mut div = "(";
                let mut schema_query = String::from("SELECT sql FROM");
                let mut schema_num = 1usize;
                loop {
                    match database_list_stmt.step() {
                        Ok(StepResult::Done) => break,
                        Ok(StepResult::Row) => {
                            let db = database_list_stmt.column_text(0).to_string_lossy();
                            schema_query.push_str(div);
                            div = " UNION ALL ";
                            schema_query.push_str("SELECT shell_add_schema(sql,");
                            if db != "main" {
                                write!(&mut schema_query, "{}", Quote::as_text(&db)).unwrap();
                            } else {
                                schema_query.push_str("NULL");
                            }
                            schema_query.push_str(",name) AS sql, type, tbl_name, name, rowid,");
                            write!(&mut schema_query, "{schema_num} AS snum, ").unwrap();
                            schema_num += 1;
                            write!(
                                &mut schema_query,
                                "{db} AS sname ",
                                db = Quote::as_text(&db)
                            )
                            .unwrap();
                            write!(
                                &mut schema_query,
                                "FROM {db}.sqlite_schema",
                                db = Quote::as_id(&db)
                            )
                            .unwrap();
                        }
                        Err(mut err) => {
                            err.clear_error_offset();
                            return Err((position, err));
                        }
                    }
                }

                schema_query.push_str(") WHERE ");
                if let Some(pattern) = line.args.get(0) {
                    if pattern.contains('.') {
                        schema_query.push_str("lower(printf('%s.%s',sname,tbl_name))")
                    } else {
                        schema_query.push_str("lower(tbl_name)")
                    }
                    let is_glob = pattern.contains(&['*', '?', '[']);
                    write!(
                        &mut schema_query,
                        " {operator} {pattern} AND ",
                        operator = if is_glob { "GLOB" } else { "LIKE" },
                        pattern = Quote::as_text(pattern)
                    )
                    .unwrap();
                }
                schema_query.push_str("sql IS NOT NULL ORDER BY snum, rowid");
                schema_query
            };
            debug!("Schema query: {schema_query}");

            let mut stmt = match conn.prepare(&schema_query).0 {
                Ok(Some(stmt)) => stmt,
                Ok(None) => unreachable!("Statement not empty"),
                Err(mut err) => {
                    err.clear_error_offset();
                    return Err((position, err));
                }
            };
            loop {
                match stmt.step() {
                    Ok(StepResult::Done) => break,
                    Ok(StepResult::Row) => {
                        let sql = stmt.column_text(0).to_string_lossy();
                        display_schema(result, &sql);
                    }
                    Err(mut err) => {
                        err.clear_error_offset();
                        return Err((position, err));
                    }
                }
            }
        }
        _ => {
            return Err((
                position,
                zombiezen_sqlite::Error::new(
                    ResultCode::ERROR,
                    format!("unknown dot command {}", &line.name),
                ),
            ));
        }
    }
    Ok(())
}

fn import(
    conn: &Conn,
    args: &[impl AsRef<str>],
    stderr: &mut String,
) -> zombiezen_sqlite::Result<()> {
    // Initial lines to skip.
    let mut filename = None::<&str>;
    let mut table = None::<&str>;
    let mut schema = None::<&str>;
    let mut skip = 0usize;
    {
        let mut arg_iter = args.iter().map(AsRef::as_ref);
        while let Some(mut arg) = arg_iter.next() {
            if arg.starts_with("--") {
                arg = &arg[1..];
            }
            match arg {
                "-skip" => {
                    let Some(value) = arg_iter.next().and_then(|v| v.parse().ok()) else {
                        let mut msg = String::new();
                        msg.push_str("Missing or invalid argument for -skip.\n");
                        display_help(&mut msg, Some("import"));
                        return Err(zombiezen_sqlite::Error::new(ResultCode::MISUSE, msg));
                    };
                    skip = value;
                }
                "-schema" => {
                    let Some(value) = arg_iter.next() else {
                        let mut msg = String::new();
                        msg.push_str("Missing argument for -schema.\n");
                        display_help(&mut msg, Some("import"));
                        return Err(zombiezen_sqlite::Error::new(ResultCode::MISUSE, msg));
                    };
                    schema = Some(value);
                }
                "-csv" => {
                    // No-op. Compatibility with SQLite shell.
                }
                _ if arg.starts_with("-") => {
                    let mut msg = String::new();
                    writeln!(&mut msg, "Unknown option \"{arg}\". Usage:").unwrap();
                    display_help(&mut msg, Some("import"));
                    return Err(zombiezen_sqlite::Error::new(ResultCode::MISUSE, msg));
                }
                _ => {
                    if filename.is_none() {
                        filename = Some(arg);
                    } else if table.is_none() {
                        table = Some(arg);
                    } else {
                        let mut msg = String::new();
                        writeln!(&mut msg, "Extra argument: \"{arg}\". Usage:").unwrap();
                        display_help(&mut msg, Some("import"));
                        return Err(zombiezen_sqlite::Error::new(ResultCode::MISUSE, msg));
                    }
                }
            }
        }
    }
    let Some(filename) = filename else {
        let mut msg = String::new();
        msg.push_str("Missing FILE argument. Usage:\n");
        display_help(&mut msg, Some("import"));
        return Err(zombiezen_sqlite::Error::new(ResultCode::MISUSE, msg));
    };
    let Some(table) = table else {
        let mut msg = String::new();
        msg.push_str("Missing TABLE argument. Usage:\n");
        display_help(&mut msg, Some("import"));
        return Err(zombiezen_sqlite::Error::new(ResultCode::MISUSE, msg));
    };

    let mut file = CsvReaderBuilder::new()
        .has_headers(false)
        .from_path(filename)
        .map_err(|err| zombiezen_sqlite::Error::new(ResultCode::IOERR, err.to_string()))?;
    let mut row = StringRecord::new();
    for _ in 0..skip {
        file.read_record(&mut row)
            .map_err(|err| zombiezen_sqlite::Error::new(ResultCode::IOERR, err.to_string()))?;
    }

    let full_table_name = match schema {
        Some(schema) => format!(
            "{schema}.{table}",
            schema = Quote::as_id(&schema),
            table = Quote::as_id(&table)
        ),
        None => Quote::as_id(&table).to_string(),
    };
    let column_count = {
        match conn.prepare(&format!("SELECT * FROM {full_table_name}")).0 {
            Ok(Some(stmt)) => stmt.column_count(),
            Ok(_) => unreachable!(),
            Err(err) if err.message().starts_with("no such table: ") => {
                let has_row = file.read_record(&mut row).map_err(|err| {
                    zombiezen_sqlite::Error::new(ResultCode::IOERR, err.to_string())
                })?;
                if !has_row {
                    return Err(zombiezen_sqlite::Error::new(
                        ResultCode::ERROR,
                        format!("{filename}: empty file"),
                    ));
                }
                // TODO(someday): Rename columns as appropriate.
                let mut definition = String::new();
                writeln!(&mut definition, "CREATE TABLE {full_table_name} (").unwrap();
                for (i, header) in row.iter().enumerate() {
                    writeln!(
                        &mut definition,
                        "{header} TEXT{comma}",
                        header = Quote::as_id(header),
                        comma = if i < row.len() - 1 { "," } else { "" }
                    )
                    .unwrap();
                }
                definition.push_str(")");
                conn.prepare(&definition)
                    .0
                    .map_err(|mut err| {
                        err.clear_error_offset();
                        err
                    })?
                    .unwrap()
                    .step()
                    .map_err(|mut err| {
                        err.clear_error_offset();
                        err
                    })?;
                row.len()
            }
            Err(mut err) => {
                err.clear_error_offset();
                return Err(err);
            }
        }
    };
    let mut insert_stmt = {
        let mut sql = format!("INSERT INTO {full_table_name} VALUES(?");
        for _ in 1..column_count {
            sql.push_str(",?");
        }
        sql.push_str(")");
        conn.prepare(&sql)
            .0
            .map_err(|mut err| {
                err.clear_error_offset();
                err
            })?
            .unwrap()
    };
    let need_commit = conn.get_autocommit();
    if need_commit {
        exec(conn, "BEGIN")?;
    }
    loop {
        let has_row = file
            .read_record(&mut row)
            .map_err(|err| zombiezen_sqlite::Error::new(ResultCode::IOERR, err.to_string()))?;
        if !has_row {
            break;
        }
        // TODO(soon): Permit more or less columns.
        for (i, val) in row.iter().enumerate() {
            insert_stmt.bind_text(1 + i, val)?;
        }
        let _ = insert_stmt.step(); // Captured by reset below:
        if let Err(err) = insert_stmt.reset() {
            writeln!(
                stderr,
                "{filename}:{lineno}: INSERT failed: {err}",
                lineno = file.position().line()
            )
            .unwrap();
        }
    }
    drop(insert_stmt);
    if need_commit {
        exec(conn, "COMMIT")?;
    }
    Ok(())
}

fn display_schema(result: &mut SQLiteOutputBuilder, sql: &str) {
    let _ = writeln!(&mut result.plain, "{}", &sql);
    let _ = writeln!(&mut result.html, "<pre><code>{}</code></pre>", &sql);
}

const HELP: &str = include_str!("help.txt");

fn display_help(result: &mut String, pattern: Option<&str>) {
    match pattern {
        Some("-a") | Some("-all") | Some("--all") => result.push_str(HELP),
        None => {
            for line in HELP.lines().filter(|l| l.starts_with(".")) {
                result.push_str(line);
                result.push_str("\n");
            }
        }
        Some(pattern) => {
            // Seek documented commands for which the pattern is an exact prefix.
            {
                let mut iter = HELP.lines();
                let mut details = None;
                let mut n = 0usize;
                let glob_pattern = cstring_until_first_nul(format!(".{}*", pattern));
                while let Some(line) = iter.next() {
                    if zombiezen_sqlite::strglob(&glob_pattern, cstring_until_first_nul(line)) {
                        result.push_str(line);
                        result.push_str("\n");
                        n += 1;
                        if n == 1 {
                            details = Some(iter.clone().take_while(|line| !line.starts_with(".")));
                        } else {
                            details = None;
                        }
                    }
                }
                if let Some(details) = details {
                    for line in details {
                        result.push_str(line);
                        result.push_str("\n");
                    }
                }
                if n > 0 {
                    return;
                }
            }

            // Look for documented commands that contain the pattern anywhere.
            // Show complete text of all documented commands that match.
            let like_pattern = cstring_until_first_nul(format!("%{}%", pattern));
            let mut iter = HELP.lines().peekable();
            while let Some(line) = iter.next() {
                if line.starts_with(".")
                    && zombiezen_sqlite::strlike(
                        &like_pattern,
                        cstring_until_first_nul(&line[1..]),
                        '\x00',
                    )
                {
                    result.push_str(line);
                    result.push_str("\n");
                    while let Some(line) = iter.by_ref().next_if(|line| !line.starts_with(".")) {
                        result.push_str(line);
                        result.push_str("\n");
                    }
                }
            }
        }
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
    use std::ffi::CStr;
    use std::io::Write;

    use tempfile::NamedTempFile;
    use zombiezen_sqlite::OpenFlags;

    use super::*;

    #[test]
    fn test_import() {
        let db =
            Connection::open(<&CStr>::default(), OpenFlags::default() | OpenFlags::MEMORY).unwrap();
        let mut csv_file = NamedTempFile::new().unwrap();
        csv_file.write(include_bytes!("import_test.csv")).unwrap();

        let mut stderr = String::new();
        import(
            &db,
            &[csv_file.path().to_str().unwrap(), "beatles"],
            &mut stderr,
        )
        .unwrap();

        let mut stmt = db
            .prepare("SELECT first_name, last_name, birth_year FROM beatles ORDER BY last_name")
            .0
            .unwrap()
            .unwrap();
        let mut results = Vec::<(String, String, i64)>::new();
        while stmt.step().unwrap().has_row() {
            let first_name = stmt.column_text(0).unwrap().to_string();
            let last_name = stmt.column_text(1).unwrap().to_string();
            let birth_year = stmt.column_i64(2);
            results.push((first_name, last_name, birth_year));
        }
        assert_eq!(
            results,
            vec![
                (String::from("George"), String::from("Harrison"), 1943),
                (String::from("John"), String::from("Lennon"), 1940),
                (String::from("Paul"), String::from("McCartney"), 1942),
                (String::from("Ringo"), String::from("Starr"), 1940),
            ],
        );
    }

    #[test]
    fn test_import_existing() {
        let db =
            Connection::open(<&CStr>::default(), OpenFlags::default() | OpenFlags::MEMORY).unwrap();
        let mut csv_file = NamedTempFile::new().unwrap();
        csv_file.write(include_bytes!("import_test.csv")).unwrap();
        exec(
            &db,
            "CREATE TABLE beatles (first_name, last_name, birth_year INTEGER)",
        )
        .unwrap();

        let mut stderr = String::new();
        import(
            &db,
            &["-skip", "1", csv_file.path().to_str().unwrap(), "beatles"],
            &mut stderr,
        )
        .unwrap();

        let mut stmt = db
            .prepare("SELECT first_name, last_name, birth_year FROM beatles ORDER BY last_name")
            .0
            .unwrap()
            .unwrap();
        let mut results = Vec::<(String, String, i64)>::new();
        while stmt.step().unwrap().has_row() {
            let first_name = stmt.column_text(0).unwrap().to_string();
            let last_name = stmt.column_text(1).unwrap().to_string();
            let birth_year = stmt.column_i64(2);
            results.push((first_name, last_name, birth_year));
        }
        assert_eq!(
            results,
            vec![
                (String::from("George"), String::from("Harrison"), 1943),
                (String::from("John"), String::from("Lennon"), 1940),
                (String::from("Paul"), String::from("McCartney"), 1942),
                (String::from("Ringo"), String::from("Starr"), 1940),
            ],
        );
    }

    #[test]
    fn test_display_help_prefix() {
        let mut output = String::new();
        display_help(&mut output, Some("rea"));
        assert!(
            output.starts_with(".read "),
            "{:?} should start with .read",
            &output
        );
        assert!(
            output.ends_with("\n"),
            "{:?} should end with newline",
            &output
        );
        assert_eq!(
            output.chars().filter(|&c| c == '\n').count(),
            1,
            "{:?} should only be one line",
            &output
        );
    }

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
