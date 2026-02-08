use std::io::Write;
use std::net::{SocketAddr, UdpSocket};

pub fn main() -> noargs::Result<()> {
    let mut args = noargs::raw_args();
    noargs::HELP_FLAG.take_help(&mut args);

    let port: u16 = noargs::opt("port")
        .short('p')
        .example("9000")
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
    socket: &UdpSocket,
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
    socket: &UdpSocket,
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
    let socket = UdpSocket::bind(addr(node_id))?;
    eprintln!("Started node {}", node_id.get());

    let mut node = rufton::Node::start(node_id);
    let mut machine = std::collections::HashMap::<String, nojson::RawJsonOwned>::new();

    let mut storage = rufton::FileStorage::open(format!("/tmp/kvs-{}.jsonl", node_id.get()))?;
    let entries = storage.load_entries()?;
    if entries.is_empty() {
        if let Some(contact) = contact_node {
            let members = [node_id, contact];
            node.init_cluster(&members);
        } else {
            node.init_cluster(&[node_id]);
        }
    } else {
        let (ok, snapshot) = node.load(&entries);
        assert!(ok);
        if let Some(snapshot) = snapshot {
            machine = snapshot.try_into()?;
        }
    }

    let mut timeout_time = next_timeout_time(noraft::Role::Follower);
    let mut buf = [0u8; 65535];
    loop {
        let now = std::time::Instant::now();
        if timeout_time < now {
            node.handle_timeout();
        }

        drain_actions(
            &socket,
            &mut storage,
            &mut node,
            &mut machine,
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
            let request = nojson::object(|f| {
                f.member("params", params)?;
                f.member("id", req_id.clone())?;
                f.member("src", src_addr)
            });
            node.propose_command(rufton::JsonValue::new(request));
        } else {
            assert_eq!(req.method(), "Internal");
            let params = req.params().expect("bug");
            assert!(node.handle_message(&rufton::JsonValue::new(params)));
        }
    }
}

fn drain_actions(
    socket: &UdpSocket,
    storage: &mut rufton::FileStorage,
    node: &mut rufton::Node,
    machine: &mut std::collections::HashMap<String, nojson::RawJsonOwned>,
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
            rufton::Action::Apply {
                is_proposer,
                index,
                request,
            } => {
                eprintln!("Apply: {} (is_proposer={})", index.get(), is_proposer);

                let request_value = request.get();
                let command_value = request
                    .get()
                    .to_member("params")
                    .and_then(|value| value.required())
                    .unwrap_or(request_value);
                let ty = command_value
                    .to_member("type")?
                    .required()?
                    .as_string_str()?;
                let result = match ty {
                    "put" => {
                        let key = command_value.to_member("key")?.required()?.try_into()?;
                        let value = command_value
                            .to_member("value")?
                            .required()?
                            .extract()
                            .into_owned();
                        let old = machine.insert(key, value);
                        rufton::JsonValue::new(nojson::object(|f| f.member("old", &old)))
                    }
                    "get" => {
                        let key: String = command_value.to_member("key")?.required()?.try_into()?; // TODO: dont use String
                        let value = machine.get(&key);
                        rufton::JsonValue::new(nojson::object(|f| f.member("value", value)))
                    }
                    _ => rufton::JsonValue::new("unknown type"),
                };
                if is_proposer {
                    let req_id: rufton::JsonRpcRequestId =
                        request_value.to_member("id")?.required()?.try_into()?;
                    let src: SocketAddr = request_value.to_member("src")?.required()?.try_into()?;
                    send_response(socket, src, &req_id, result)?;
                }
            } // TODO: Add NotifyEvent
        }
    }
    Ok(())
}

fn next_timeout_time(role: noraft::Role) -> std::time::Instant {
    let ms = match role {
        noraft::Role::Leader => 50,
        noraft::Role::Follower => 150,
        noraft::Role::Candidate => 150 + rand::random::<u64>() % 50,
    };
    std::time::Instant::now() + std::time::Duration::from_millis(ms)
}
