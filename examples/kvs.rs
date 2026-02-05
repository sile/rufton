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
            client.send_request(&mut poll, addr(contact), None, "Propose", params)?;
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
        poll.poll(&mut events, None)?;

        for event in events.iter() {
            server.handle_mio_event(&mut poll, event)?;
        }

        while let Some((client_id, line)) = server.next_request_line() {
            match rufton::JsonRpcRequest::parse(line) {
                Err(e) => {
                    server.reply_err(&mut poll, client_id, None, e.code(), e.message())?;
                }
                Ok(req) => {
                    let Some(req_id) = req.id().cloned() else {
                        continue;
                    };
                    let json = req.into_json().into_owned();
                    server.reply_ok(&mut poll, client_id, &req_id, json)?;
                }
            }
        }
    }
}
