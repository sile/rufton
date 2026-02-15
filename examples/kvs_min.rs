use rufton::{JsonValue, Node, NodeId, Result};

mod kvs;

pub fn main() -> rufton::Result<()> {
    let Some(arg) = std::env::args().nth(1) else {
        return Err(rufton::Error::new("missing PORT arg"));
    };
    let port: u16 = arg.parse()?;

    run(format!("127.0.0.1:{port}").parse()?)
}

fn run(addr: std::net::SocketAddr) -> rufton::Result<()> {
    let mut sock = std::net::UdpSocket::bind(addr)?;
    let mut machine = kvs::Machine::new();

    let node_id = NodeId::new(addr.port() as u64);
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
                    is_proposer,
                    request,
                    ..
                } => handle_request(&mut sock, &mut machine, is_proposer, request.get())?,
                rufton::Action::NotifyEvent(event) => {
                    eprintln!("Event: {}", event);
                }
                rufton::Action::SetTimeout(_) | rufton::Action::AppendStorageEntry(_) => {}
                a => todo!("{a:?}"),
            }
        }

        let (json, src_addr) = kvs::recv_request(&sock, &mut buf)?;
        let request = json.value();

        let method: &str = request.to_member("method")?.required()?.try_into()?;
        let params = request.to_member("params")?.required()?;

        if method == "_message" {
            node.handle_message(params);
        } else {
            let id: u64 = request.to_member("id")?.required()?.try_into()?;
            let command = nojson::object(|f| {
                f.member("method", method)?;
                f.member("params", params)?;
                f.member("id", id)?;
                f.member("src", src_addr)
            });
            node.propose_command(command);
        }
    }
}

fn broadcast_message(sock: &std::net::UdpSocket, node: &Node, msg: JsonValue) -> Result<()> {
    let req = format!(r#"{{"jsonrpc":"2.0","method":"_message","params":{msg}}}"#);
    for dst in node.peers() {
        sock.send_to(req.as_bytes(), dst.to_localhost_addr()?)?;
    }
    Ok(())
}

fn send_message(sock: &std::net::UdpSocket, dst: NodeId, msg: JsonValue) -> Result<()> {
    let req = format!(r#"{{"jsonrpc":"2.0","method":"_message","params":{msg}}}"#);
    sock.send_to(req.as_bytes(), dst.to_localhost_addr()?)?;
    Ok(())
}

fn handle_request(
    sock: &std::net::UdpSocket,
    machine: &mut kvs::Machine,
    is_proposed: bool,
    request: nojson::RawJsonValue,
) -> Result<()> {
    let result = kvs::apply(machine, request);
    if is_proposed {
        let src = request.to_member("src")?.required()?.try_into()?;
        kvs::send_response(sock, request, result, src)?;
    }
    Ok(())
}
