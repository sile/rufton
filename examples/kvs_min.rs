use std::io::Write;
use std::net::SocketAddr;

pub fn main() -> rufton::Result<()> {
    let Some(arg) = std::env::args().nth(1) else {
        return Err(rufton::Error::new("missing PORT arg"));
    };
    let port: u16 = arg.parse()?;

    let init_node_ids: Option<Vec<noraft::NodeId>> = Some(vec![
        noraft::NodeId::new(9000),
        noraft::NodeId::new(9001),
        noraft::NodeId::new(9002),
    ]);

    let mut kvs = Kvs::new(noraft::NodeId::new(port as u64), init_node_ids)?;
    kvs.run()?;
    Ok(())
}

fn addr(id: noraft::NodeId) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], id.get() as u16))
}

struct Kvs {
    socket: rufton::LineFramedTcpSocket,
    node: rufton::Node,
    machine: std::collections::HashMap<String, nojson::RawJsonOwned>,
    requests: std::collections::HashMap<rufton::ProposalId, (SocketAddr, rufton::JsonRpcRequestId)>,
    buf: Vec<u8>,
}

impl Kvs {
    fn new(
        node_id: noraft::NodeId,
        init_node_ids: Option<Vec<noraft::NodeId>>,
    ) -> rufton::Result<Self> {
        let socket = rufton::LineFramedTcpSocket::bind(addr(node_id))?;
        let machine = std::collections::HashMap::new();

        let mut node = rufton::Node::start(node_id);
        if let Some(members) = init_node_ids {
            node.init_cluster(&members);
        }

        Ok(Self {
            socket,
            node,
            machine,
            requests: std::collections::HashMap::new(), // TODO: use VecDeque
            buf: vec![0u8; 65535],
        })
    }

    fn run(&mut self) -> rufton::Result<()> {
        loop {
            while let Some(action) = self.node.next_action() {
                self.handle_action(action)?;
            }

            let (len, src_addr) = self.socket.recv_from(&mut self.buf)?;
            let req =
                rufton::JsonRpcRequest::parse(&self.buf[..len]).expect("should return err res");
            let params = req.params().expect("bug");
            if req.method() == "Command" {
                let proposal_id = self
                    .node
                    .propose_command(rufton::JsonLineValue::new(params));
                if let Some(req_id) = req.id().cloned() {
                    self.requests.insert(proposal_id, (src_addr, req_id));
                }
            } else {
                self.node
                    .handle_message(&rufton::JsonLineValue::new(params));
            }
        }
    }

    fn handle_action(&mut self, action: rufton::Action) -> rufton::Result<()> {
        match action {
            rufton::Action::BroadcastMessage(m) => {
                let peers: Vec<_> = self.node.peers().collect();
                for dst in peers {
                    self.send_request(addr(dst), "_Message", &m)?;
                }
            }
            rufton::Action::SendMessage(dst, m) => {
                // TODO: take snapshot if node.recent_commits().len() gets too long
                self.send_request(addr(dst), "_Message", &m)?;
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
                    let result = match ty {
                        "put" => {
                            let key = v.to_member("key")?.required()?.try_into()?;
                            let value = v.to_member("value")?.required()?.extract().into_owned();
                            let old = self.machine.insert(key, value);
                            rufton::JsonLineValue::new(nojson::object(|f| f.member("old", &old)))
                        }
                        "get" => {
                            let key: String = v.to_member("key")?.required()?.try_into()?; // TODO: dont use String
                            let value = self.machine.get(&key);
                            rufton::JsonLineValue::new(nojson::object(|f| f.member("value", value)))
                        }
                        _ => rufton::JsonLineValue::new("unknown type"),
                    };
                    if let Some((client_addr, req_id)) =
                        proposal_id.and_then(|id| self.requests.remove(&id))
                    {
                        self.send_response(client_addr, &req_id, result)?;
                    }
                }
            }
            _ => todo!(),
        }
        Ok(())
    }

    fn send_request<T: nojson::DisplayJson>(
        &mut self,
        dst: SocketAddr,
        method: &str,
        params: T,
    ) -> std::io::Result<()> {
        let method = nojson::Json(method);
        let params = nojson::Json(params);
        let mut buf = Vec::new();
        write!(
            &mut buf,
            r#"{{"jsonrpc":"2.0","method":{method},"params":{params}}}"#,
        )?;
        self.socket.send_to(&buf, dst)?;
        Ok(())
    }

    fn send_response<T: nojson::DisplayJson>(
        &mut self,
        dst: SocketAddr,
        request_id: &rufton::JsonRpcRequestId,
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
