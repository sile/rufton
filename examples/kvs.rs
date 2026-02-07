use std::io::Write;
use std::net::SocketAddr;

pub fn main() -> noargs::Result<()> {
    let mut args = noargs::raw_args();
    args.metadata_mut().app_name = "kvs";
    args.metadata_mut().app_description = "KVS example";

    noargs::HELP_FLAG.take_help(&mut args);

    let port: u16 = noargs::opt("port")
        .short('p')
        .default("9000")
        .take(&mut args)
        .then(|a| a.value().parse())?;
    let contact_node = noargs::opt("contact")
        .short('c')
        .take(&mut args)
        .present_and_then(|a| a.value().parse().map(noraft::NodeId::new))?;

    if let Some(help) = args.finish()? {
        print!("{help}");
        return Ok(());
    }

    run_node(noraft::NodeId::new(port as u64), contact_node)?;
    Ok(())
}

fn addr(id: noraft::NodeId) -> SocketAddr {
    ([127, 0, 0, 1], id.get() as u16).into()
}

fn send_request<T: nojson::DisplayJson>(
    socket: &mut rufton::LineFramedTcpSocket,
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
    socket.send_to(&buf, dst)?;
    Ok(())
}

fn send_response<T: nojson::DisplayJson>(
    socket: &mut rufton::LineFramedTcpSocket,
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
    socket.send_to(&buf, dst)?;
    Ok(())
}

fn run_node(node_id: noraft::NodeId, contact_node: Option<noraft::NodeId>) -> noargs::Result<()> {
    let mut socket = rufton::LineFramedTcpSocket::bind(addr(node_id))?;
    eprintln!("Started node {}", node_id.get());

    let mut node = rufton::Node::start(node_id);
    let mut machine = std::collections::HashMap::<String, nojson::RawJsonOwned>::new();

    let mut storage = rufton::FileStorage::open(format!("/tmp/kvs-{}.jsonl", node_id.get()))?;
    let entries = storage.load_entries()?;
    if entries.is_empty() {
        if let Some(contact) = contact_node {
            let params = nojson::object(|f| {
                f.member("type", "AddNode")?;
                f.member("proposal_id", [0, 0, 0])?;
                f.member("id", node_id.get())
            });
            send_request(&mut socket, addr(contact), "Internal", params)?;
        } else {
            node.init_cluster();
        }
    } else {
        let (ok, snapshot) = node.load(&entries);
        assert!(ok);
        if let Some(snapshot) = snapshot {
            machine = snapshot.try_into()?;
        }
    }

    let mut timeout_time = next_timeout_time(noraft::Role::Follower);
    let mut requests = std::collections::HashMap::new();
    let mut buf = [0u8; 65535];
    loop {
        let now = std::time::Instant::now();
        if timeout_time < now {
            node.handle_timeout();
        }

        drain_actions(
            &mut socket,
            &mut storage,
            &mut node,
            &mut machine,
            &mut requests,
            &mut timeout_time,
        )?;

        let timeout = timeout_time.saturating_duration_since(now);
        socket.set_read_timeout(Some(timeout))?;

        let (len, src_addr) = match socket.recv_from(&mut buf) {
            Ok((len, src_addr)) => (len, src_addr),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        let req = rufton::JsonRpcRequest::parse(&buf[..len]).expect("should return err res");
        if let Some(req_id) = req.id().cloned() {
            assert_eq!(req.method(), "Command");
            let params = req.params().expect("bug");
            let proposal_id = node.propose_command(rufton::JsonLineValue::new(params));
            requests.insert(proposal_id, (src_addr, req_id));
        } else {
            assert_eq!(req.method(), "Internal");
            let params = req.params().expect("bug");
            assert!(node.handle_message(&rufton::JsonLineValue::new(params)));
        }
    }
}

fn drain_actions(
    socket: &mut rufton::LineFramedTcpSocket,
    storage: &mut rufton::FileStorage,
    node: &mut rufton::Node,
    machine: &mut std::collections::HashMap<String, nojson::RawJsonOwned>,
    requests: &mut std::collections::HashMap<
        rufton::ProposalId,
        (SocketAddr, rufton::JsonRpcRequestId),
    >,
    timeout_time: &mut std::time::Instant,
) -> noargs::Result<()> {
    while let Some(action) = node.next_action() {
        match action {
            rufton::Action::AppendStorageEntry(x) => storage.append_entry(&x)?,
            rufton::Action::SendSnapshot(_dst) => {
                // TODO: take snapshot if node.recent_commits().len() gets too long
                unreachable!()
            }
            rufton::Action::SetTimeout(role) => {
                *timeout_time = next_timeout_time(role);
            }
            rufton::Action::BroadcastMessage(m) => {
                for dst in node.members() {
                    if dst != node.id() {
                        send_request(socket, addr(dst), "Internal", &m)?;
                    }
                }
            }
            rufton::Action::SendMessage(dst, m) => {
                send_request(socket, addr(dst), "Internal", &m)?;
            }
            rufton::Action::Commit {
                proposal_id,
                index,
                command,
            } => {
                eprintln!("Commit: {} ({:?})", index.get(), proposal_id);

                if let Some(command) = command {
                    let v = command.get().to_member("command")?.required()?; // TODO: Remove this call
                    let ty: String = v.to_member("type")?.required()?.try_into()?; // TODO: dont use String
                    let result = match ty.as_str() {
                        "put" => {
                            let key = v.to_member("key")?.required()?.try_into()?;
                            let value = v.to_member("value")?.required()?.extract().into_owned();
                            let old = machine.insert(key, value);
                            rufton::JsonLineValue::new(nojson::object(|f| f.member("old", &old)))
                        }
                        "get" => {
                            let key: String = v.to_member("key")?.required()?.try_into()?; // TODO: dont use String
                            let value = machine.get(&key);
                            rufton::JsonLineValue::new(nojson::object(|f| f.member("value", value)))
                        }
                        _ => rufton::JsonLineValue::new("unknown type"),
                    };
                    if let Some((client_addr, req_id)) =
                        proposal_id.and_then(|id| requests.remove(&id))
                    {
                        send_response(socket, client_addr, &req_id, result)?;
                    }
                }
            }
            rufton::Action::Query { .. } => unreachable!(), // TODO: Merge with Commit (and rename to Apply)
                                                            // TODO: Add NotifyEvent
        }
    }
    Ok(())
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
