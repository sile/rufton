use rufton::{Node, NodeId, Result};

mod kvs;

pub fn main() -> rufton::Result<()> {
    let Some(arg) = std::env::args().nth(1) else {
        return Err(rufton::Error::new("missing PORT arg"));
    };
    let port: u16 = arg.parse()?;

    run(format!("127.0.0.1:{port}").parse()?)
}

fn run(addr: std::net::SocketAddr) -> rufton::Result<()> {
    let sock = std::net::UdpSocket::bind(addr)?;
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
            handle_action(&sock, &node, &mut machine, action)?;
        }

        let (json, src_addr) = kvs::recv_request(&sock, &mut buf)?;
        let request = json.value();

        let method: &str = request.to_member("method")?.required()?.try_into()?;
        if method == "_" {
            let params = request.to_member("params")?.required()?;
            node.handle_message(params);
        } else {
            node.propose_command(src_addr, request);
        }
    }
}

fn handle_action(
    sock: &std::net::UdpSocket,
    node: &Node,
    machine: &mut kvs::Machine,
    action: rufton::Action,
) -> Result<()> {
    match action {
        rufton::Action::Broadcast(msg) => {
            let req = format!(r#"{{"jsonrpc":"2.0","method":"_","params":{msg}}}"#);
            for dst in node.peers() {
                sock.send_to(req.as_bytes(), dst.to_localhost_addr()?)?;
            }
        }
        rufton::Action::Send(dst, msg) => {
            let req = format!(r#"{{"jsonrpc":"2.0","method":"_","params":{msg}}}"#);
            sock.send_to(req.as_bytes(), dst.to_localhost_addr()?)?;
        }
        rufton::Action::Apply(apply) => {
            let request = apply.request();
            let result = kvs::apply(machine, request);
            if let Some(source) = apply.source() {
                kvs::send_response(sock, request, result, source.try_into()?)?;
            }
        }
        rufton::Action::NotifyEvent(event) => {
            eprintln!("Event: {}", event);
        }
        _ => {}
    }
    Ok(())
}
