use std::io::Write;
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

fn addr(id: noraft::NodeId) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], id.get() as u16))
}

struct Kvs {
    socket: rufton::LineFramedTcpSocket,
    node: rufton::Node,
    machine: KvsMachine,
    requests: std::collections::HashMap<rufton::ProposalId, (SocketAddr, u64)>,
}

impl Kvs {
    fn new(node_id: noraft::NodeId) -> rufton::Result<Self> {
        let socket = rufton::LineFramedTcpSocket::bind(addr(node_id))?;
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
            requests: std::collections::HashMap::new(), // TODO: use VecDeque
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
            let id: Option<u64> = request.to_member("id")?.try_into()?;

            if method == "_internal" {
                // TODO: remove JsonLineValue
                self.node
                    .handle_message(&rufton::JsonLineValue::new(params));
            } else {
                let proposal_id = self
                    .node
                    .propose_command(rufton::JsonLineValue::new(request));
                if let Some(id) = id {
                    self.requests.insert(proposal_id, (src_addr, id));
                }
            }
        }
    }

    fn handle_action(&mut self, action: rufton::Action) -> rufton::Result<()> {
        match action {
            rufton::Action::BroadcastMessage(m) => {
                let peers: Vec<_> = self.node.peers().collect();
                for dst in peers {
                    self.send_request(addr(dst), "_internal", &m)?;
                }
            }
            rufton::Action::SendMessage(dst, m) => {
                // TODO: take snapshot if node.recent_commits().len() gets too long
                self.send_request(addr(dst), "_internal", &m)?;
            }
            rufton::Action::Commit {
                proposal_id,
                index,
                command,
            } => {
                eprintln!("Commit: {} ({:?})", index.get(), proposal_id);

                if let Some(command) = command {
                    let v = command.get().to_member("command")?.required()?; // TODO: Remove this call
                    let ty = v.to_member("type")?.required()?.as_string_str()?;
                    let result = apply(&mut self.machine, ty, v)?;
                    if let Some((client_addr, req_id)) =
                        proposal_id.and_then(|id| self.requests.remove(&id))
                    {
                        self.send_response(client_addr, req_id, result)?;
                    }
                }
            }
            _ => todo!(),
        }
        Ok(())
    }

    fn send_request(
        &mut self,
        dst: SocketAddr,
        method: &str,
        params: &rufton::JsonLineValue,
    ) -> rufton::Result<()> {
        let req = format!(r#"{{"jsonrpc":"2.0","method":{method},"params":{params}}}"#);
        self.socket.send_to(req.as_bytes(), dst)?;
        Ok(())
    }

    fn send_response<T: nojson::DisplayJson>(
        &mut self,
        dst: SocketAddr,
        request_id: u64,
        result: T,
    ) -> std::io::Result<()> {
        let id = nojson::Json(request_id);
        let result = nojson::Json(result);
        let mut buf = Vec::new();
        write!(
            &mut buf,
            r#"{{"jsonrpc":"2.0","id":{id},"result":{result}}}"#,
        )?;
        self.socket.send_to(&buf, dst)?;
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
