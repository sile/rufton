pub fn main() -> noargs::Result<()> {
    let mut args = noargs::raw_args();

    args.metadata_mut().app_name = "echo_server";
    args.metadata_mut().app_description = "JSON-RPC echo server example";

    noargs::HELP_FLAG.take_help(&mut args);

    let port: u16 = noargs::opt("port")
        .short('p')
        .doc("Port to listen on")
        .default("9000")
        .take(&mut args)
        .then(|a| a.value().parse())?;

    if let Some(help) = args.finish()? {
        print!("{help}");
        return Ok(());
    }

    let addr = format!("127.0.0.1:{port}");
    run_server(&addr)?;
    Ok(())
}

fn run_server(listen_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let socket_addr: std::net::SocketAddr = listen_addr.parse()?;
    let mut poll = mio::Poll::new()?;
    let min_token = mio::Token(0);
    let max_token = mio::Token(1024);

    let mut server = raftjson::json_rpc_server::JsonRpcServer::start(
        &mut poll,
        min_token,
        max_token,
        socket_addr,
    )?;
    eprintln!("Echo server listening on {}", listen_addr);

    let mut events = mio::Events::with_capacity(128);

    loop {
        poll.poll(&mut events, None)?;

        for event in events.iter() {
            let _ = server.handle_mio_event(&mut poll, event);
        }

        while let Some((client_id, line)) = server.next_request_line() {
            match raftjson::json_rpc_server::JsonRpcRequest::parse(line) {
                Err(e) => {
                    server.reply_err(&mut poll, client_id, None, e.code(), e.message())?;
                }
                Ok(req) => {
                    let Some(req_id) = req.id().cloned() else {
                        continue;
                    };
                }
            }
        }
    }
}
