use std::collections::HashMap;
use std::ffi::CString;
use std::fmt;
use std::fs;
use std::iter;
use std::path::PathBuf;
use std::process::exit;

use anyhow::Result;
use clap::Parser as ClapParser;
use rand::{thread_rng, RngCore};
use serde::{de::Visitor, Deserialize, Deserializer, Serialize};
use serde_json::json;
use tempdir::TempDir;
use tracing::{debug, debug_span, error, field, info, trace_span, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use uuid::Builder as UuidBuilder;
use zombiezen_sqlite::{Connection, OpenFlags, StepResult};
use zombiezen_zmq::{
    Context as ZeroMQContext, Errno, Events, PollItem, RecvFlags, SendFlags, Socket,
};

mod wire;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct KernelConfig {
    transport: String,
    ip: String,
    control_port: u16,
    shell_port: u16,
    #[serde(rename = "iopub_port")]
    io_pub_port: u16,
    stdin_port: u16,
    #[serde(rename = "hb_port")]
    heartbeat_port: u16,

    signature_scheme: String,
    #[serde(deserialize_with = "deserialize_key")]
    key: Vec<u8>,
}

fn deserialize_key<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    struct AsBytes {}

    impl<'de> Visitor<'de> for AsBytes {
        type Value = Vec<u8>;

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v.to_string().into_bytes())
        }

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            write!(formatter, "a string")
        }
    }

    deserializer.deserialize_str(AsBytes {})
}

impl KernelConfig {
    fn as_auth(&self) -> wire::Authentication<&str, &[u8]> {
        wire::Authentication {
            signature_scheme: &self.signature_scheme,
            key: &self.key,
        }
    }
}

#[derive(Clone, Debug, ClapParser)]
struct Args {
    /// Path to a JSON file with the Jupyter kernel connection information.
    #[arg(id = "FILE")]
    connection_file: PathBuf,

    #[arg(long)]
    debug: bool,
}

fn main() {
    let args = Args::parse();
    let mut sink_builder = tracing_subscriber::fmt().with_span_events(FmtSpan::CLOSE);
    if args.debug {
        sink_builder = sink_builder.with_max_level(Level::DEBUG);
    }
    sink_builder.init();

    match run(args) {
        Ok(()) => {
            info!("Shutdown complete.");
        }
        Err(err) => {
            error!("Exiting: {}", err);
            exit(1)
        }
    }
}

fn run(args: Args) -> Result<()> {
    let session_id = {
        let mut random_bytes = [0u8; 16];
        thread_rng().fill_bytes(&mut random_bytes);
        let id = UuidBuilder::from_random_bytes(random_bytes).into_uuid();
        id.as_hyphenated().to_string()
    };
    debug!(session_id = session_id, "Generated session ID");
    let (_database_directory, mut conn) = {
        let span = debug_span!("sqlite3_open", path = field::Empty);
        let _enter = span.enter();
        let dir = TempDir::new("sqlite-notebook")?;
        let db_path = dir.path().join("database.sqlite");
        span.record("path", db_path.to_string_lossy().as_ref());
        let conn = Connection::open(
            // TODO(now): There should be better ways to do this.
            CString::new(db_path.to_str().unwrap())?,
            OpenFlags::default(),
        )?;
        (dir, conn)
    };

    let connection_file_data = fs::read(&args.connection_file)?;
    debug!(
        path = &args.connection_file.to_string_lossy().as_ref(),
        data = String::from_utf8_lossy(&connection_file_data).as_ref(),
        "Connection file read"
    );
    let config: KernelConfig = serde_json::from_slice(&connection_file_data)?;
    debug!(
        transport = &config.transport,
        ip = &config.ip,
        hb_port = config.heartbeat_port,
        iopub_port = config.io_pub_port,
        control_port = config.control_port,
        stdin_port = config.stdin_port,
        shell_port = config.shell_port,
        signature_scheme = &config.signature_scheme,
        "Configuration parsed"
    );

    let ctx = ZeroMQContext::new()?;
    let mut heartbeat_socket = ctx.new_rep(iter::once(CString::new(format!(
        "{}://{}:{}",
        config.transport, config.ip, config.heartbeat_port
    ))?))?;
    let mut io_pub_socket = ctx.new_pub(iter::once(CString::new(format!(
        "{}://{}:{}",
        config.transport, config.ip, config.io_pub_port
    ))?))?;
    let mut control_socket = ctx.new_router(iter::once(CString::new(format!(
        "{}://{}:{}",
        config.transport, config.ip, config.control_port
    ))?))?;
    let mut stdin_socket = ctx.new_router(iter::once(CString::new(format!(
        "{}://{}:{}",
        config.transport, config.ip, config.stdin_port
    ))?))?;
    let mut shell_socket = ctx.new_router(iter::once(CString::new(format!(
        "{}://{}:{}",
        config.transport, config.ip, config.shell_port
    ))?))?;

    let zmq_version = {
        let (major, minor, patch) = zombiezen_zmq::version();
        format!("{}.{}.{}", major, minor, patch)
    };
    info!(
        sqlite.version = zombiezen_sqlite::version(),
        zeromq.version = &zmq_version,
        "Kernel ready",
    );
    let mut execution_counter = 0i32;
    loop {
        let result = zombiezen_zmq::poll(
            None,
            [
                PollItem::new(&mut heartbeat_socket, Events::IN),
                PollItem::new(&mut io_pub_socket, Events::IN),
                PollItem::new(&mut control_socket, Events::IN),
                PollItem::new(&mut stdin_socket, Events::IN),
                PollItem::new(&mut shell_socket, Events::IN),
            ],
        );
        let socket_indices: Vec<usize> = match result {
            Ok(events) => events.into_iter().map(|(i, _)| i).collect(),
            Err(Errno(zombiezen_zmq::EINTR)) => continue,
            Err(err) => return Err(err.into()),
        };

        for socket_idx in socket_indices {
            match socket_idx {
                0 => handle_heartbeat(&mut heartbeat_socket)?,
                1 => drain_socket(&mut io_pub_socket),
                2 => {
                    if !handle_control(&config.as_auth(), &mut control_socket)? {
                        info!("Shutting down...");
                        return Ok(());
                    }
                }
                3 => drain_socket(&mut stdin_socket),
                4 => handle_shell(
                    &session_id,
                    &config.as_auth(),
                    &mut execution_counter,
                    &mut conn,
                    &mut shell_socket,
                    &mut io_pub_socket,
                )?,
                _ => unreachable!("unknown index {}", socket_idx),
            }
        }
    }
}

fn handle_shell<S, K>(
    session_id: &str,
    auth: &wire::Authentication<S, K>,
    execution_counter: &mut i32,
    conn: &mut Connection,
    shell_socket: &mut Socket,
    io_pub_socket: &mut Socket,
) -> Result<()>
where
    S: AsRef<str>,
    K: AsRef<[u8]>,
{
    let span = debug_span!("handle_shell", msg_type = field::Empty);
    let _enter = span.enter();

    let mut buf = [0u8; 65536]; // 64 KiB
    let message = shell_socket.recv_message(&mut buf, RecvFlags::default())?;
    let message = wire::Message::deserialize(auth, message)?;
    let hdr = message.parse_header()?;
    span.record("msg_type", &hdr.r#type);

    match hdr.r#type.as_str() {
        "execute_request" => {
            match execute(
                session_id,
                auth,
                execution_counter,
                conn,
                &message,
                io_pub_socket,
            ) {
                Ok(response) => reply(
                    session_id,
                    auth,
                    shell_socket,
                    "execute_reply",
                    message.identities().iter().copied(),
                    message.raw_header(),
                    &response,
                )?,
                Err(err) => reply(
                    session_id,
                    auth,
                    shell_socket,
                    "execute_reply",
                    message.identities().iter().copied(),
                    message.raw_header(),
                    &err,
                )?,
            }
        }
        "kernel_info_request" => {
            let version = zombiezen_sqlite::version();
            let banner = format!(
                "SQLite version {}\nEnter \".help\" for usage hints.",
                version
            );
            reply(
                session_id,
                auth,
                shell_socket,
                "is_complete_reply",
                message.identities().iter().copied(),
                message.raw_header(),
                &json!({
                    "protocol_version": wire::PROTOCOL_VERSION,

                    "implementation": "sqlite-notebook",
                    "implementation_version": "0.0.1",

                    "language_info": {
                        "name": "sql",
                        "version": version,
                        "mimetype": "application/sql",
                        "file_extension": ".sql",
                    },

                    "banner": &banner,
                }),
            )?;
        }
        "is_complete_request" => {
            let message_content = message.deserialize_content::<wire::IsCompleteRequest>();
            let result = message_content
                .map_err(|err| wire::ErrorReply {
                    exception_value: err.to_string().into(),
                    ..wire::ErrorReply::new("ValueError")
                })
                .and_then(|request| is_complete(&request));
            match result {
                Ok(response) => reply(
                    session_id,
                    auth,
                    shell_socket,
                    "is_complete_reply",
                    message.identities().iter().copied(),
                    message.raw_header(),
                    &response,
                )?,
                Err(err) => reply(
                    session_id,
                    auth,
                    shell_socket,
                    "is_complete_reply",
                    message.identities().iter().copied(),
                    message.raw_header(),
                    &err,
                )?,
            }
        }
        _ => {
            if let Some(base) = hdr.r#type.strip_suffix("_request") {
                reply(
                    session_id,
                    auth,
                    shell_socket,
                    format!("{}_reply", base),
                    message.identities().iter().copied(),
                    message.raw_header(),
                    &wire::ErrorReply::new("NotImplementedError"),
                )?;
            }
        }
    }
    reply(
        session_id,
        auth,
        io_pub_socket,
        "status",
        iter::empty::<Vec<u8>>(),
        message.raw_header(),
        &json!({
            "execution_state": "idle",
        }),
    )?;
    Ok(())
}

fn execute<S, K, P>(
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
        Ok((plain, html)) => {
            let data = if plain.is_empty() && html.is_empty() {
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
                json!({})
            } else {
                json!({"text/plain": plain, "text/html": html})
            };
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
        Err((base_offset, err)) => {
            let err_string = err.to_string();
            let traceback = if let Some(off) = err.error_offset() {
                let (lineno, col) = str_position(&req_content.code, base_offset + off);
                format!("{}:{}: {}", lineno, col, &err_string)
            } else {
                err_string.clone()
            };
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
) -> Result<(String, String), (usize, zombiezen_sqlite::Error)> {
    let mut plain_result = String::new();
    let mut html_result = String::new();
    let orig_size = code.len();
    let map_err = |err| (orig_size - code.len(), err);
    let mut first_select = true;
    while !code.is_empty() {
        let (mut stmt, tail) = match conn.prepare(code).map_err(map_err)? {
            Some(x) => x,
            None => break,
        };

        let column_count = stmt.column_count();
        if column_count > 0 {
            html_result.push_str("<table>\n<thead><tr>");
            for i in 0..column_count {
                let name = stmt.column_name(i).unwrap();
                html_result.push_str("<th scope=\"col\">");
                escape_html(&mut html_result, &name);
                html_result.push_str("</th>");
            }
            html_result.push_str("</tr></thead>\n<tbody>\n");

            if !first_select {
                plain_result.push_str("\n");
            }
        }
        loop {
            match stmt.step().map_err(map_err)? {
                StepResult::Done => break,
                StepResult::Row => {
                    html_result.push_str("<tr>");
                    for i in 0..column_count {
                        let val = String::from_utf8_lossy(
                            stmt.column_blob(i).expect("column should be in range"),
                        );

                        html_result.push_str("<td>");
                        escape_html(&mut html_result, &val);
                        html_result.push_str("</td>");

                        if i > 0 {
                            plain_result.push_str("|");
                        }
                        plain_result.push_str(&val);
                    }
                    html_result.push_str("</tr>\n");
                    plain_result.push_str("\n");
                }
            }
        }
        if column_count > 0 {
            html_result.push_str("</tbody></table>\n");
            first_select = false;
        }
        code = tail;
    }
    Ok((plain_result, html_result))
}

fn is_complete(
    request: &wire::IsCompleteRequest,
) -> Result<wire::IsCompleteReply, wire::ErrorReply<'static>> {
    let code = request.code.as_bytes();
    let mut buf = Vec::with_capacity(code.len() + 2);
    buf.extend(code);
    if !code.ends_with(b";") {
        buf.push(b';');
    }
    buf.push(0);
    let code = match CString::from_vec_with_nul(buf) {
        Ok(code) => code,
        Err(_) => {
            return Ok(wire::IsCompleteReply {
                status: wire::IsCompleteStatus::Invalid,
                indent: None,
            });
        }
    };
    if zombiezen_sqlite::is_complete(&code) {
        Ok(wire::IsCompleteReply {
            status: wire::IsCompleteStatus::Complete,
            indent: None,
        })
    } else {
        Ok(wire::IsCompleteReply {
            status: wire::IsCompleteStatus::Incomplete,
            indent: Some("  ".to_string()),
        })
    }
}

fn reply<S, K>(
    session_id: &str,
    auth: &wire::Authentication<S, K>,
    socket: &mut Socket,
    r#type: impl Into<String>,
    identities: impl IntoIterator<Item = impl Into<Vec<u8>>>,
    parent_header: &[u8],
    content: &(impl Serialize + ?Sized),
) -> Result<()>
where
    S: AsRef<str>,
    K: AsRef<[u8]>,
{
    let mut response = wire::Message::new(thread_rng(), r#type, session_id, content)?;
    response.set_identities(identities);
    response.set_parent_header(parent_header);
    let response = response.serialize(auth)?;
    socket.send_message(response, SendFlags::default())?;
    Ok(())
}

fn handle_heartbeat(socket: &mut Socket) -> Result<()> {
    let span = trace_span!("heartbeat");
    let _enter = span.enter();

    let mut buf = [0u8; 4096];
    let result = socket.recv(&mut buf, RecvFlags::default())?;
    if result.is_truncated() {
        eprintln!("Heartbeat truncated");
        return Ok(());
    }
    socket.send(
        &buf[..result.buf_len()],
        if result.has_more() {
            SendFlags::SNDMORE
        } else {
            SendFlags::default()
        },
    )?;
    Ok(())
}

fn handle_control<S, K>(auth: &wire::Authentication<S, K>, socket: &mut Socket) -> Result<bool>
where
    S: AsRef<str>,
    K: AsRef<[u8]>,
{
    let span = debug_span!("control", msg_type = field::Empty);
    let _enter = span.enter();

    let mut buf = [0u8; 4096];
    let message = socket.recv_message(&mut buf, RecvFlags::default())?;
    let message = match wire::Message::deserialize(auth, message) {
        Ok(message) => message,
        Err(_) => return Ok(true),
    };
    match message.parse_header() {
        Ok(hdr) => {
            span.record("msg_type", &hdr.r#type);
            Ok(&hdr.r#type != "shutdown_request")
        }
        Err(_) => return Ok(true),
    }
}

fn drain_socket(socket: &mut Socket) {
    let mut buf = [0u8; 4096];
    let _ = socket.recv(&mut buf, RecvFlags::default());
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

fn escape_html(dst: &mut String, s: &str) {
    dst.reserve(s.len());
    for c in s.chars() {
        match c {
            '&' => dst.push_str("&amp;"),
            '<' => dst.push_str("&lt;"),
            '>' => dst.push_str("&gt;"),
            '\'' => dst.push_str("&#39;"),
            '"' => dst.push_str("&#34;"),
            _ => dst.push(c),
        }
    }
}
