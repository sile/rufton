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

fn addr(id: noraft::NodeId) -> std::net::SocketAddr {
    ([127, 0, 0, 1], id.get() as u16).into()
}

fn run_node(node_id: noraft::NodeId, contact_node: Option<noraft::NodeId>) -> noargs::Result<()> {
    let mut poll = mio::Poll::new()?;
    let mut server =
        rufton::JsonRpcServer::start(&mut poll, mio::Token(0), mio::Token(100), addr(node_id))?;
    let mut client = rufton::JsonRpcClient::new(mio::Token(200), mio::Token(300))?;
    eprintln!("Started node {}", node_id.get());

    let mut node = rufton::RaftNode::start(node_id);
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
            client.send_request(&mut poll, addr(contact), None, "Internal", params)?;
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

    let mut events = mio::Events::with_capacity(128);
    let mut timeout_time = next_timeout_time(noraft::Role::Follower);
    let mut requests = std::collections::HashMap::new();
    loop {
        if timeout_time < std::time::Instant::now() {
            node.handle_timeout();
        }

        while let Some(action) = node.next_action() {
            match action {
                rufton::Action::AppendStorageEntry(x) => storage.append_entry(&x)?,
                rufton::Action::SendSnapshot(_dst) => {
                    // TODO: take snapshot if node.recent_commits().len() gets too long
                    unreachable!()
                }
                rufton::Action::SetTimeout(role) => {
                    timeout_time = next_timeout_time(role);
                }
                rufton::Action::BroadcastMessage(m) => {
                    for dst in node.members() {
                        if dst != node_id {
                            client.send_request(&mut poll, addr(dst), None, "Internal", &m)?;
                        }
                    }
                }
                rufton::Action::SendMessage(dst, m) => {
                    client.send_request(&mut poll, addr(dst), None, "Internal", &m)?;
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
                                let value =
                                    v.to_member("value")?.required()?.extract().into_owned();
                                let old = machine.insert(key, value);
                                rufton::JsonLineValue::new(nojson::object(|f| {
                                    f.member("old", &old)
                                }))
                            }
                            "get" => {
                                let key: String = v.to_member("key")?.required()?.try_into()?; // TODO: dont use String
                                let value = machine.get(&key);
                                rufton::JsonLineValue::new(nojson::object(|f| {
                                    f.member("value", value)
                                }))
                            }
                            _ => rufton::JsonLineValue::new("unknown type"),
                        };
                        if let Some((client_id, req_id)) =
                            proposal_id.and_then(|id| requests.remove(&id))
                        {
                            server.reply_ok(&mut poll, client_id, &req_id, result)?;
                        }
                    }
                }
                rufton::Action::Query { .. } => unreachable!(),
            }
        }

        /*let now = std::time::Instant::now();
        let timeout = if timeout_time <= now {
            std::time::Duration::from_millis(0)
        } else {
            timeout_time - now
        };
        poll.poll(&mut events, Some(timeout))?;*/
        poll.poll(&mut events, None)?;

        for event in events.iter() {
            let _ = server.handle_mio_event(&mut poll, event)?
                || client.handle_mio_event(&mut poll, event)?;
        }
        assert!(client.next_response_line().is_none());

        while let Some((client_id, line)) = server.next_request_line() {
            match rufton::JsonRpcRequest::parse(line) {
                Err(e) => {
                    server.reply_err(&mut poll, client_id, None, e.code(), e.message())?;
                }
                Ok(req) => {
                    if let Some(req_id) = req.id().cloned() {
                        // Assumes external API
                        //
                        // Example: {"jsonrpc":"2.0","method":"Command","params":{"type":"put","key":"foo","value":30},"id":0}
                        assert_eq!(req.method(), "Command");
                        let params = req.params().expect("bug");
                        let proposal_id = node.propose_command(rufton::JsonLineValue::new(params));
                        requests.insert(proposal_id, (client_id, req_id));
                    } else {
                        // Assumes internal node communication
                        assert_eq!(req.method(), "Internal");
                        let params = req.params().expect("bug");
                        assert!(node.handle_message(&rufton::JsonLineValue::new(params)));
                    };
                }
            }
        }
    }
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
