use std::io::Write;
use std::net::SocketAddr;

pub fn main() -> noargs::Result<()> {
    let mut args = noargs::raw_args();
    noargs::HELP_FLAG.take_help(&mut args);

    let port: u16 = noargs::arg("PORT")
        .example("9000")
        .take(&mut args)
        .then(|a| a.value().parse())?;
    let init_node_ids: Option<Vec<noraft::NodeId>> = noargs::opt("init")
        .take(&mut args)
        .present_and_then(|a| -> Result<_, nojson::JsonParseError> {
            let ids: nojson::Json<Vec<_>> = a.value().parse()?;
            Ok(ids
                .0
                .into_iter()
                .map(noraft::NodeId::new)
                .collect::<Vec<_>>())
        })?;

    if let Some(help) = args.finish()? {
        print!("{help}");
        return Ok(());
    }

    run(noraft::NodeId::new(port as u64), init_node_ids)?;
    Ok(())
}

fn addr(id: noraft::NodeId) -> SocketAddr {
    ([127, 0, 0, 1], id.get() as u16).into()
}

struct Kvs {
    socket: rufton::LineFramedTcpSocket,
    node: rufton::Node,
    machine: std::collections::HashMap<String, nojson::RawJsonOwned>,
    storage: rufton::FileStorage,
    requests: std::collections::HashMap<rufton::ProposalId, (SocketAddr, rufton::JsonRpcRequestId)>,
    timeout_time: std::time::Instant,
    buf: [u8; 65535],
}

impl Kvs {
    fn new(
        node_id: noraft::NodeId,
        init_node_ids: Option<Vec<noraft::NodeId>>,
    ) -> noargs::Result<Self> {
        let socket = rufton::LineFramedTcpSocket::bind(addr(node_id))?;
        eprintln!("Started node {}", node_id.get());

        let mut node = rufton::Node::start(node_id);
        let mut machine = std::collections::HashMap::<String, nojson::RawJsonOwned>::new();
        let mut storage = rufton::FileStorage::open(format!("/tmp/kvs-{}.jsonl", node_id.get()))?;
        let entries = storage.load_entries()?;
        if entries.is_empty() {
            if let Some(members) = init_node_ids {
                node.init_cluster(&members);
            }
        } else {
            let (ok, snapshot) = node.load(&entries);
            assert!(ok);
            if let Some(snapshot) = snapshot {
                machine = snapshot.try_into()?;
            }
        }

        Ok(Self {
            socket,
            node,
            machine,
            storage,
            requests: std::collections::HashMap::new(), // TODO: use VecDeque
            timeout_time: next_timeout_time(noraft::Role::Follower),
            buf: [0u8; 65535],
        })
    }

    fn run_loop(&mut self) -> noargs::Result<()> {
        loop {
            let now = std::time::Instant::now();
            if self.timeout_time < now {
                self.node.handle_timeout();
            }

            while let Some(action) = self.node.next_action() {
                self.handle_action(action)?;
            }

            let timeout = self.timeout_time.saturating_duration_since(now);
            self.socket.set_read_timeout(Some(timeout))?;

            let (len, src_addr) = match self.socket.recv_from(&mut self.buf) {
                Ok((len, src_addr)) => (len, src_addr),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e.into()),
            };

            let req =
                rufton::JsonRpcRequest::parse(&self.buf[..len]).expect("should return err res");
            if let Some(req_id) = req.id().cloned() {
                assert_eq!(req.method(), "Command");
                let params = req.params().expect("bug");
                let proposal_id = self
                    .node
                    .propose_command(rufton::JsonLineValue::new(params));
                self.requests.insert(proposal_id, (src_addr, req_id));
            } else {
                assert_eq!(req.method(), "Internal");
                let params = req.params().expect("bug");
                assert!(
                    self.node
                        .handle_message(&rufton::JsonLineValue::new(params))
                );
            }
        }
    }

    fn handle_action(&mut self, action: rufton::Action) -> noargs::Result<()> {
        match action {
            rufton::Action::AppendStorageEntry(x) => self.storage.append_entry(&x)?,
            rufton::Action::SendSnapshot(_dst) => {
                // TODO: take snapshot json, split the json, send each fragment
                unreachable!()
            }
            rufton::Action::SetTimeout(role) => {
                self.timeout_time = next_timeout_time(role);
            }
            rufton::Action::BroadcastMessage(m) => {
                let peers: Vec<_> = self.node.peers().collect();
                for dst in peers {
                    self.send_request(addr(dst), "Internal", &m)?;
                }
            }
            rufton::Action::SendMessage(dst, m) => {
                // TODO: take snapshot if node.recent_commits().len() gets too long
                self.send_request(addr(dst), "Internal", &m)?;
            }
            rufton::Action::Apply {
                proposal_id,
                index,
                command,
            } => {
                eprintln!("Apply: {} ({:?})", index.get(), proposal_id);
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
            rufton::Action::Query { .. } => unreachable!(), // TODO: Merge Query with Apply
                                                            // TODO: Add NotifyEvent
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

fn run(node_id: noraft::NodeId, init_node_ids: Option<Vec<noraft::NodeId>>) -> noargs::Result<()> {
    let mut kvs = Kvs::new(node_id, init_node_ids)?;
    kvs.run_loop()
}

fn next_timeout_time(role: noraft::Role) -> std::time::Instant {
    let timeout_duration = if role.is_leader() {
        // Leader sends heartbeats frequently
        std::time::Duration::from_millis(50)
    } else {
        // Follower/Candidate wait longer for election timeout
        // TODO: Add random jitter if candidate
        std::time::Duration::from_millis(150)
    };
    std::time::Instant::now() + timeout_duration
}
