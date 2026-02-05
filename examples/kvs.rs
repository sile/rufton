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

    loop {
        while let Some(action) = node.next_action() {
            match action {
                rufton::Action::AppendStorageEntry(x) => storage.append_entry(&x)?,
                rufton::Action::SendSnapshot(dst) => {
                    // TODO: take snapshot if node.recent_commits().len() gets too long
                    unreachable!()
                }
                rufton::Action::SetTimeout(role) => {
                    // TODO
                }
                rufton::Action::BroadcastMessage(m) => todo!(),
                rufton::Action::SendMessage(dst, m) => todo!(),
                rufton::Action::Commit {
                    proposal_id,
                    index,
                    command,
                } => {
                    eprintln!("Commit: {} ({:?})", index.get(), proposal_id);
                    if let Some(command) = command {
                        todo!("apply to machine");
                    }
                }
                rufton::Action::Query { .. } => unreachable!(),
            }
        }

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
                        todo!()
                    } else {
                        // Assumes internal node communication
                        assert_eq!(req.method(), "Internal");
                        if let Some(params) = req.params() {
                            node.handle_message(&rufton::JsonLineValue::new(params));
                        }
                    };
                }
            }
        }
    }
}
