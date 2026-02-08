use std::net::SocketAddr;

use noraft::NodeId;
use rufton::{JsonLineValue, Node, Result};

type KvsMachine = std::collections::HashMap<String, usize>;
type Socket = std::net::UdpSocket; // rufton::LineFramedTcpSocket;

pub fn main() -> rufton::Result<()> {
    let Some(arg) = std::env::args().nth(1) else {
        return Err(rufton::Error::new("missing PORT arg"));
    };
    let port: u16 = arg.parse()?;

    run(format!("127.0.0.1:{port}").parse()?)
}

fn run(addr: std::net::SocketAddr) -> rufton::Result<()> {
    let mut sock = Socket::bind(addr)?;
    let mut machine = KvsMachine::new();

    let node_id = noraft::NodeId::new(addr.port() as u64);
    let mut node = rufton::Node::start(node_id);
    let members = [NodeId::new(9000), NodeId::new(9001), NodeId::new(9002)];
    if node_id == members[0] {
        node.init_cluster(&members);
    }

    let mut buf = [0; 65535];
    loop {
        while let Some(action) = node.next_action() {
            match action {
                rufton::Action::BroadcastMessage(msg) => broadcast_message(&mut sock, &node, msg)?,
                rufton::Action::SendMessage(dst, msg) => send_message(&mut sock, dst, msg)?,
                rufton::Action::Apply {
                    proposal_id,
                    command,
                    ..
                } => {
                    // TODO: call apply() here
                    handle_command(&mut sock, &mut machine, proposal_id.is_some(), command)?;
                }
                rufton::Action::SetTimeout(_) | rufton::Action::AppendStorageEntry(_) => {}
                a => todo!("{a:?}"),
            }
        }

        let (len, src_addr) = sock.recv_from(&mut buf)?;
        let text = str::from_utf8(&buf[..len])?; // TODO: note
        let json = nojson::RawJson::parse(text)?;
        let request = json.value();

        let method: &str = request.to_member("method")?.required()?.try_into()?;
        let params = request.to_member("params")?.required()?;

        if method == "_message" {
            // TODO: remove JsonLineValue
            node.handle_message(&rufton::JsonLineValue::new(params));
        } else {
            let id: u64 = request.to_member("id")?.required()?.try_into()?;
            let command = nojson::object(|f| {
                f.member("method", method)?;
                f.member("params", params)?;
                f.member("id", id)?;
                f.member("src", src_addr)
            });
            let _proposal_id = node.propose_command(rufton::JsonLineValue::new(command));
        }
    }
}

fn broadcast_message(sock: &mut Socket, node: &Node, msg: JsonLineValue) -> Result<()> {
    let req = format!(r#"{{"jsonrpc":"2.0","method":"_message","params":{msg}}}"#);
    for dst in node.peers() {
        let addr = SocketAddr::from(([127, 0, 0, 1], dst.get() as u16));
        sock.send_to(req.as_bytes(), addr)?;
    }
    Ok(())
}

fn send_message(sock: &mut Socket, dst: NodeId, msg: JsonLineValue) -> Result<()> {
    let req = format!(r#"{{"jsonrpc":"2.0","method":"_message","params":{msg}}}"#);
    let addr = SocketAddr::from(([127, 0, 0, 1], dst.get() as u16));
    sock.send_to(req.as_bytes(), addr)?;
    Ok(())
}

fn handle_command(
    sock: &mut Socket,
    machine: &mut KvsMachine,
    is_proposed: bool,
    command: JsonLineValue,
) -> Result<()> {
    let v = command.get().to_member("command")?.required()?; // TODO: Remove this call
    let method: &str = v.to_member("method")?.required()?.try_into()?;
    let params = v.to_member("params")?.required()?;
    let id: u64 = v.to_member("id")?.required()?.try_into()?;
    let src: SocketAddr = v.to_member("src")?.required()?.try_into()?;

    let result = apply(machine, method, params)?;
    if is_proposed {
        let res = format!(r#"{{"jsonrpc":"2.0", "id":{id}, "result":{result}}}"#);
        sock.send_to(res.as_bytes(), src)?;
    }

    Ok(())
}

fn apply(machine: &mut KvsMachine, method: &str, params: nojson::RawJsonValue) -> Result<String> {
    match method {
        "put" => {
            let key: String = params.to_member("key")?.required()?.try_into()?;
            let value: usize = params.to_member("value")?.required()?.try_into()?;
            let old = machine.insert(key, value);
            Ok(format!(r#"{{"old": {}}}"#, nojson::Json(old)))
        }
        "get" => {
            let key: &str = params.to_member("key")?.required()?.try_into()?;
            let value = machine.get(key);
            Ok(format!(r#"{{"value": {}}}"#, nojson::Json(value)))
        }
        _ => Err(rufton::Error::new("unknown method")),
    }
}
