use std::ffi::CString;
use std::fs;
use std::iter;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser as ClapParser;
use serde::{Deserialize, Serialize};
use zombiezen_zmq::{
    Context as ZeroMQContext, Errno, Events, PollItem, RecvFlags, SendFlags, Socket,
};

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
    #[serde(with = "hex::serde")]
    key: Vec<u8>,
}

#[derive(Clone, Debug, ClapParser)]
struct Args {
    /// Path to a JSON file with the Jupyter kernel connection information.
    #[arg(id = "FILE")]
    connection_file: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let connection_file_data = fs::read(&args.connection_file)?;
    let config: KernelConfig = serde_json::from_slice(&connection_file_data)?;

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
                _ => unreachable!("unknown index {}", socket_idx),
            }
        }
    }
}

fn handle_heartbeat(socket: &mut Socket) -> Result<()> {
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
