use std::ffi::{CStr, CString};
use std::fmt;
use std::fs;
use std::iter;
use std::path::PathBuf;
use std::process::exit;
use std::rc::Rc;

use anyhow::Result;
use clap::Parser as ClapParser;
use rand::{thread_rng, RngCore};
use re2::RE2;
use serde::{de::Visitor, Deserialize, Deserializer, Serialize};
use serde_json::json;
use tempdir::TempDir;
use tracing::warn;
use tracing::{debug, debug_span, error, field, info, trace_span, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use uuid::Builder as UuidBuilder;
use zombiezen_const_cstr::const_cstr;
use zombiezen_sqlite::{
    stricmp, Connection, Context, FunctionFlags, OpenFlags, ProtectedValue, ResultCode, ResultExt,
};
use zombiezen_zmq::{
    Context as ZeroMQContext, Errno, Events, PollItem, RecvFlags, SendFlags, Socket,
};

use crate::c::cstr_from_osstr;

mod c;
mod execute;
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
        let dir = TempDir::new("sqlite-notebook")?;
        let db_path = dir.path().join("database.sqlite");
        let conn = open_conn(&cstr_from_osstr(&db_path))?;
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

fn open_conn(db_path: &(impl AsRef<CStr> + ?Sized)) -> zombiezen_sqlite::Result<Connection> {
    let db_path = db_path.as_ref();
    let span = debug_span!("sqlite3_open", path = db_path.to_string_lossy().as_ref());
    let _enter = span.enter();

    let mut conn = Connection::open(db_path, OpenFlags::default())?;
    conn.create_scalar_function(
        const_cstr!("regexp").as_cstr(),
        Some(2),
        FunctionFlags::DETERMINISTIC,
        regexp_func,
    )?;
    conn.create_scalar_function(
        const_cstr!("shell_add_schema").as_cstr(),
        Some(3),
        FunctionFlags::empty(),
        |mut ctx, args| {
            let mut input_value = args.next().unwrap();
            let input = match input_value.to_text() {
                Ok(s) => s,
                Err(_) => {
                    ctx.result_error(ResultCode::ERROR, "invalid UTF-8");
                    return;
                }
            };
            let mut schema_value = args.next().unwrap();
            let schema = match schema_value.to_text() {
                Ok(s) => s,
                Err(_) => {
                    ctx.result_error(ResultCode::ERROR, "invalid UTF-8");
                    return;
                }
            };
            let mut name_value = args.next().unwrap();
            let name = match name_value.to_text() {
                Ok(s) => s,
                Err(_) => {
                    ctx.result_error(ResultCode::ERROR, "invalid UTF-8");
                    return;
                }
            };
            if let Some(s) = shell_add_schema_name(input, schema, name) {
                ctx.result_text(s);
            } else {
                ctx.result_value(&input_value);
            }
        },
    )?;
    Ok(conn)
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
            match execute::execute(
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

fn is_complete(
    request: &wire::IsCompleteRequest,
) -> Result<wire::IsCompleteReply, wire::ErrorReply<'static>> {
    let span = debug_span!("is_complete", status = field::Empty);
    let _enter = span.enter();

    let status = scan(request.code.as_str());
    span.record("status", format!("{:?}", status));
    Ok(wire::IsCompleteReply {
        status,
        indent: if status == wire::IsCompleteStatus::Incomplete {
            Some(String::new())
        } else {
            None
        },
    })
}

fn scan(code: &str) -> wire::IsCompleteStatus {
    let mut term = None::<char>;
    let mut iter = code.chars().peekable();
    let mut paren_level = 0usize;
    let mut ends_in_semicolon = true;
    let mut can_start_dot_command = true;
    loop {
        let Some(c) = iter.next() else {
            break;
        };
        match term {
            None => match c {
                '-' => {
                    if iter.next_if(|&next| next == '-').is_some() {
                        // Consume the line here and now.
                        // Unlike the others, having an EOF before hitting the end of line
                        // still results in a complete statement.
                        iter.find(|&next| next == '\n');
                        can_start_dot_command = true;
                    } else {
                        can_start_dot_command = false;
                    }
                }
                '.' | '#' if can_start_dot_command => {
                    // Consume the line here and now.
                    // Unlike the others, having an EOF before hitting the end of line
                    // still results in a complete statement.
                    iter.find(|&next| next == '\n');
                }
                '/' => {
                    if iter.next_if(|&next| next == '*').is_some() {
                        term = Some('*');
                    } else {
                        ends_in_semicolon = false;
                    }
                    can_start_dot_command = false;
                }
                '[' => {
                    term = Some(']');
                    ends_in_semicolon = false;
                    can_start_dot_command = false;
                }
                '`' | '\'' | '"' => {
                    term = Some(c);
                    ends_in_semicolon = false;
                    can_start_dot_command = false;
                }
                '(' => {
                    paren_level += 1;
                    ends_in_semicolon = false;
                    can_start_dot_command = false;
                }
                ')' => {
                    paren_level = paren_level.saturating_sub(1);
                    ends_in_semicolon = false;
                    can_start_dot_command = false;
                }
                ';' => {
                    ends_in_semicolon = true;
                    can_start_dot_command = false;
                }
                '\n' => {
                    can_start_dot_command = true;
                }
                _ => {
                    can_start_dot_command = false;
                    if !c.is_whitespace() {
                        ends_in_semicolon = false;
                    }
                }
            },
            Some(t) if c == t => match t {
                '*' => {
                    if !iter.next_if(|&next| next == '/').is_some() {
                        continue;
                    }
                    term = None;
                }
                '`' | '\'' | '"' => {
                    if iter.next_if(|&next| next == t).is_some() {
                        // Swallow doubled end-delimiter.
                        continue;
                    }
                    term = None;
                }
                ']' => {
                    term = None;
                }
                _ => unreachable!(),
            },
            Some(_) => {
                // Ignore non-terminating character of state.
            }
        }
    }
    if term.is_none() && paren_level == 0 && ends_in_semicolon {
        wire::IsCompleteStatus::Complete
    } else {
        wire::IsCompleteStatus::Incomplete
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
        warn!("Heartbeat truncated");
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

fn shell_add_schema_name(input: &str, schema: &str, _name: &str) -> Option<String> {
    const START: &str = "CREATE ";
    const PREFIXES: &[&str] = &[
        "TABLE",
        "INDEX",
        "UNIQUE INDEX",
        "VIEW",
        "TRIGGER",
        "VIRTUAL TABLE",
    ];

    let input = strip_iprefix(input, START)?;
    for prefix in PREFIXES {
        if let Some(tail) = strip_iprefix(input, prefix) {
            let Some(tail) = tail.strip_prefix(" ") else {
                continue;
            };
            let mut buf = String::new();
            buf.push_str(START);
            buf.push_str(prefix);
            buf.push_str(" ");
            if !schema.is_empty() {
                // TODO(someday): Quote.
                buf.push_str(schema);
                buf.push_str(".");
            }
            // TODO(someday): Rename if needed.
            buf.push_str(tail);
            return Some(buf);
        }
    }
    None
}

fn regexp_func(mut ctx: Context, args: &mut dyn ExactSizeIterator<Item = ProtectedValue>) {
    let mut pattern_value = args.next().unwrap();
    let mut text = args.next().unwrap();
    if text.is_null() {
        return;
    }
    let text = text.to_text().to_string_lossy();

    let re = match ctx
        .get_auxdata(0)
        .and_then(|data| data.downcast_ref::<Rc<RE2>>().cloned())
    {
        Some(re) => re,
        None => {
            let pattern = match pattern_value.to_text() {
                Ok(pattern) => pattern,
                Err(err) => {
                    ctx.result_error(ResultCode::ERROR, &err.to_string());
                    return;
                }
            };
            match RE2::new(pattern) {
                Ok(re) => {
                    let re = Rc::new(re);
                    ctx.set_auxdata(0, Box::new(re.clone()));
                    re
                }
                Err(err) => {
                    ctx.result_error(ResultCode::ERROR, &err.to_string());
                    return;
                }
            }
        }
    };
    ctx.result_i64(re.easy_match(&text, None) as i64);
}

fn strip_iprefix<'a, 'b>(s: &'a str, prefix: &'b str) -> Option<&'a str> {
    let s_char_indices = s.char_indices().map(Some).chain(iter::repeat(None));
    let mut tail_start = 0usize;
    for ((prefix_i, prefix_c), s_elem) in prefix.char_indices().zip(s_char_indices) {
        let Some((s_i, s_c)) = s_elem else {
            return None;
        };
        tail_start = s_i + s_c.len_utf8();
        let s_slice = &s[s_i..tail_start];
        let prefix_slice = &s[prefix_i..][..prefix_c.len_utf8()];
        if stricmp(s_slice, prefix_slice).is_ne() {
            return None;
        }
    }
    Some(&s[tail_start..])
}

#[cfg(test)]
mod tests {
    use crate::wire::{IsCompleteRequest, IsCompleteStatus};

    use super::*;

    #[test]
    fn test_shell_add_schema_name() {
        assert_eq!(
            shell_add_schema_name("CREATE TABLE t1(x)", "xyz", "t1")
                .as_ref()
                .map(String::as_str),
            Some("CREATE TABLE xyz.t1(x)"),
        );
        assert_eq!(
            shell_add_schema_name("create table t1(x)", "xyz", "t1")
                .as_ref()
                .map(String::as_str),
            Some("CREATE TABLE xyz.t1(x)"),
        );
    }

    #[test]
    fn test_is_complete() {
        assert_eq!(
            is_complete(&IsCompleteRequest {
                code: String::from(""),
            })
            .unwrap()
            .status,
            IsCompleteStatus::Complete
        );
        assert_eq!(
            is_complete(&IsCompleteRequest {
                code: String::from("-- Foo"),
            })
            .unwrap()
            .status,
            IsCompleteStatus::Complete
        );
        assert_eq!(
            is_complete(&IsCompleteRequest {
                code: String::from("# Foo"),
            })
            .unwrap()
            .status,
            IsCompleteStatus::Complete
        );
        assert_eq!(
            is_complete(&IsCompleteRequest {
                code: String::from(".parameter set :x 123"),
            })
            .unwrap()
            .status,
            IsCompleteStatus::Complete
        );
        assert_eq!(
            is_complete(&IsCompleteRequest {
                code: String::from("CREATE TABLE foo ("),
            })
            .unwrap()
            .status,
            IsCompleteStatus::Incomplete
        );
        assert_eq!(
            is_complete(&IsCompleteRequest {
                code: String::from("SELECT 2 + 2"),
            })
            .unwrap()
            .status,
            IsCompleteStatus::Incomplete
        );
        assert_eq!(
            is_complete(&IsCompleteRequest {
                code: String::from("SELECT 2 + 2;"),
            })
            .unwrap()
            .status,
            IsCompleteStatus::Complete
        );
        assert_eq!(
            is_complete(&IsCompleteRequest {
                code: String::from("SELECT 2 FROM"),
            })
            .unwrap()
            .status,
            IsCompleteStatus::Incomplete
        );
    }
}
