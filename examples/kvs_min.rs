use std::net::SocketAddr;

use noraft::NodeId;

type KvsMachine = std::collections::HashMap<String, usize>;

pub fn main() -> rufton::Result<()> {
    let Some(arg) = std::env::args().nth(1) else {
        return Err(rufton::Error::new("missing PORT arg"));
    };
    let port: u16 = arg.parse()?;

    let mut kvs = Kvs::new(noraft::NodeId::new(port as u64))?;
    kvs.run()?;
    Ok(())
}

struct Kvs {
    socket: rufton::LineFramedTcpSocket,
    node: rufton::Node,
    machine: KvsMachine,
}

impl Kvs {
    fn new(node_id: noraft::NodeId) -> rufton::Result<Self> {
        let addr = SocketAddr::from(([127, 0, 0, 1], node_id.get() as u16));
        let socket = rufton::LineFramedTcpSocket::bind(addr)?;
        let machine = KvsMachine::new();

        let mut node = rufton::Node::start(node_id);

        let members = [NodeId::new(9000), NodeId::new(9001), NodeId::new(9002)];
        if node_id == members[0] {
            node.init_cluster(&members);
        }

        Ok(Self {
            socket,
            node,
            machine,
        })
    }

    fn run(&mut self) -> rufton::Result<()> {
        let mut buf = [0; 65535];
        loop {
            while let Some(action) = self.node.next_action() {
                self.handle_action(action)?;
            }

            let (len, src_addr) = self.socket.recv_from(&mut buf)?;
            let text = str::from_utf8(&buf[..len])?; // TODO: note
            let json = nojson::RawJson::parse(text)?;
            let request = json.value();

            let method: &str = request.to_member("method")?.required()?.try_into()?;
            let params = request.to_member("params")?.required()?;
            let id: u64 = request.to_member("id")?.required()?.try_into()?;

            if method == "_message" {
                // TODO: remove JsonLineValue
                self.node
                    .handle_message(&rufton::JsonLineValue::new(params));
            } else {
                let command = nojson::object(|f| {
                    f.member("method", method)?;
                    f.member("params", params)?;
                    f.member("id", id)?;
                    f.member("src", src_addr)
                });
                self.node
                    .propose_command(rufton::JsonLineValue::new(command));
            }
        }
    }

    fn handle_action(&mut self, action: rufton::Action) -> rufton::Result<()> {
        match action {
            rufton::Action::BroadcastMessage(msg) => {
                let req = format!(r#"{{"jsonrpc":"2.0","method":"_message","params":{msg}}}"#);
                for dst in self.node.peers() {
                    let addr = SocketAddr::from(([127, 0, 0, 1], dst.get() as u16));
                    self.socket.send_to(req.as_bytes(), addr)?;
                }
            }
            rufton::Action::SendMessage(dst, msg) => {
                let req = format!(r#"{{"jsonrpc":"2.0","method":"_message","params":{msg}}}"#);
                let addr = SocketAddr::from(([127, 0, 0, 1], dst.get() as u16));
                self.socket.send_to(req.as_bytes(), addr)?;
            }
            rufton::Action::Commit {
                proposal_id,
                index,
                command,
            } => {
                eprintln!("Commit: {} ({:?})", index.get(), proposal_id);

                if let Some(command) = command {
                    let v = command.get().to_member("command")?.required()?; // TODO: Remove this call
                    let method: &str = v.to_member("method")?.required()?.try_into()?;
                    let params = v.to_member("params")?.required()?;
                    let id: u64 = v.to_member("id")?.required()?.try_into()?;
                    let src: SocketAddr = v.to_member("src")?.required()?.try_into()?;

                    let result = apply(&mut self.machine, method, params)?;
                    if proposal_id.is_some() {
                        let res = format!(r#"{{"jsonrpc":"2.0", "id":{id}, "result":{result}}}"#);
                        self.socket.send_to(res.as_bytes(), src)?;
                    }
                }
            }
            _ => todo!(),
        }
        Ok(())
    }
}

fn apply(
    machine: &mut KvsMachine,
    method: &str,
    params: nojson::RawJsonValue,
) -> rufton::Result<String> {
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
