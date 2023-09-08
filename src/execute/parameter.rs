use std::ffi::CStr;
use std::fmt::Write;
use std::mem;

use tracing::{debug, debug_span};
use zombiezen_const_cstr::const_cstr;
use zombiezen_sqlite::column_metadata::TableColumnMetadataFlags;
use zombiezen_sqlite::{ConfigFlag, Conn, Connection, ResultExt, Statement, StepResult};

use super::{exec, EscapeHtml, SQLiteOutputBuilder};

/// Bind parameters from values in the `temp.sqlite_parameters` table.
pub(super) fn bind_prepared_stmt(
    conn: &Conn,
    stmt: &mut Statement,
) -> zombiezen_sqlite::Result<()> {
    let n = stmt.bind_parameter_count();
    let span = debug_span!("parameter::bind_prepared_stmt", n = n);
    let _enter = span.enter();

    if n == 0 {
        return Ok(());
    }
    let metadata_result = conn.table_column_metadata(
        Some(const_cstr!("TEMP").as_cstr()),
        const_cstr!("sqlite_parameters").as_cstr(),
        const_cstr!("key").as_cstr(),
        TableColumnMetadataFlags::empty(),
    );
    if metadata_result.is_err() {
        // sqlite_parameters table does not exist.
        for i in 1..=n {
            stmt.bind_null(i)?;
        }
        return Ok(());
    }
    let mut param_stmt = conn
        .prepare("SELECT value FROM temp.sqlite_parameters WHERE key=?1")
        .0
        .map_err(|mut err| {
            err.clear_error_offset();
            err
        })?
        .unwrap();
    let num_buf = &mut Vec::<u8>::with_capacity(30);
    for i in 1..=n {
        let var_name = stmt
            .bind_parameter_name(i)
            .unwrap_or_else(|| {
                num_buf.clear();
                {
                    let mut num_buf_string =
                        unsafe { String::from_utf8_unchecked(mem::take(num_buf)) };
                    write!(&mut num_buf_string, "?{}", i).unwrap();
                    *num_buf = num_buf_string.into_bytes();
                }
                num_buf.push(0);
                unsafe { CStr::from_bytes_with_nul_unchecked(&num_buf) }
            })
            .to_string_lossy();
        debug!(i = i, var_name = var_name.as_ref(), "Found parameter");
        param_stmt.bind_text(1, var_name)?;
        match param_stmt.step() {
            Ok(StepResult::Row) => stmt.bind_value(i, &param_stmt.column_value(0))?,
            _ => stmt.bind_null(i)?,
        }
        let _ = param_stmt.reset();
    }
    Ok(())
}

pub(super) fn init(conn: &mut Connection) -> zombiezen_sqlite::Result<()> {
    let span = debug_span!("parameter::init");
    let _enter = span.enter();

    let defensive_mode = conn.get_config(ConfigFlag::Defensive)?;
    let writable_schema = conn.get_config(ConfigFlag::WritableSchema)?;

    conn.config(ConfigFlag::Defensive, false)?;
    conn.config(ConfigFlag::WritableSchema, true)?;
    let stmt_result = exec(conn, include_str!("parameters.sql"));
    let _ = conn.config(ConfigFlag::Defensive, defensive_mode);
    let _ = conn.config(ConfigFlag::WritableSchema, writable_schema);
    stmt_result
}

pub(super) fn set(conn: &mut Connection, key: &str, value: &str) -> zombiezen_sqlite::Result<()> {
    init(conn)?;
    debug!(key = key, value = value, "Setting parameter...");
    let mut stmt = match conn
        .prepare(&format!(
            "REPLACE INTO temp.sqlite_parameters(key,value) \
         VALUES (?1, {value});"
        ))
        .0
    {
        Ok(Some(stmt)) => stmt,
        Ok(None) => unreachable!("Statement not empty"),
        Err(_) => {
            // Value might not be SQL syntax.
            // Fallback to text.
            debug!(value = value, "Falling back to text for parameter value");
            let mut stmt = conn
                .prepare("REPLACE INTO temp.sqlite_parameters(key,value) VALUES (?1, ?2);")
                .0?
                .unwrap();
            stmt.bind_text(2, value)?;
            stmt
        }
    };
    stmt.bind_text(1, key)?;
    stmt.step()?;
    stmt.finalize()?;
    Ok(())
}

pub(super) fn unset(conn: &Conn, key: &str) -> zombiezen_sqlite::Result<()> {
    let Ok(Some(mut stmt)) = conn.prepare("DELETE FROM temp.sqlite_parameters WHERE key = ?").0 else {
        // Table may not exist. Ignore error.
        return Ok(());
    };
    stmt.bind_text(1, key)?;
    let _ = stmt.step();
    Ok(())
}

pub(super) fn clear(conn: &Conn) -> zombiezen_sqlite::Result<()> {
    exec(conn, "DROP TABLE IF EXISTS temp.sqlite_parameters;")
}

pub(super) fn list(result: &mut SQLiteOutputBuilder, conn: &Conn) -> zombiezen_sqlite::Result<()> {
    let column_width = match conn
        .prepare("SELECT max(length(key)) FROM temp.sqlite_parameters;")
        .0
    {
        Ok(Some(mut stmt)) => match stmt.step() {
            Ok(StepResult::Row) => stmt.column_i64(0).clamp(0, 40) as usize,
            _ => 0usize,
        },
        Ok(None) => unreachable!("Statement not empty"),
        Err(err) => {
            debug!(
                error = err.to_string(),
                "Ignoring sqlite_parameters error: table probably does not exist."
            );
            return Ok(());
        }
    };
    if column_width == 0 {
        return Ok(());
    }
    result.html.push_str(
        "<figure><figcaption>Parameters</figcaption>\n\
                                <table>\n\
                                <thead><tr>\
                                <th scope=\"col\">Parameter</th>\
                                <th scope=\"col\">Value</th>\
                                </tr></thead>\n\
                                <tbody>\n",
    );
    let mut stmt = match conn
        .prepare("SELECT key, quote(value) FROM temp.sqlite_parameters;")
        .0
    {
        Ok(Some(stmt)) => stmt,
        Ok(None) => unreachable!("Statement not empty"),
        Err(mut err) => {
            err.clear_error_offset();
            return Err(err);
        }
    };
    while stmt.step().is_ok_and(StepResult::has_row) {
        {
            let key = stmt.column_text(0).to_string_lossy();
            write!(&mut result.plain, "{key:<column_width$} ").unwrap();
            write!(
                &mut result.html,
                "<tr><th scope=\"row\"><code>{key}</code></th>",
                key = EscapeHtml(&key),
            )
            .unwrap();
            result.csv.write_field(key.as_bytes()).unwrap();
        }

        let value = stmt.column_text(1).to_string_lossy();
        writeln!(&mut result.plain, "{value}").unwrap();
        writeln!(
            &mut result.html,
            "<td><code>{value}</code></td></tr>",
            value = EscapeHtml(&value),
        )
        .unwrap();
        result.csv.write_field(value.as_bytes()).unwrap();
        result.csv.write_record(None::<&[u8]>).unwrap();
    }
    result.html.push_str("</tbody></table></figure>\n");
    Ok(())
}
