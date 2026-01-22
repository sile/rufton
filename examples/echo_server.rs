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
    let listener = std::net::TcpListener::bind(listen_addr)?;
    eprintln!("Echo server listening on {}", listener.local_addr()?);

    for incoming in listener.incoming() {
        let stream = incoming?;
        std::thread::spawn(move || {
            let _ = handle_client(stream);
        });
    }
    Ok(())
}

fn handle_client(stream: std::net::TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{BufRead, BufReader, BufWriter, Write};

    let reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);

    for line in reader.lines() {
        let line = line?;
        writeln!(writer, "{}", line)?;
        writer.flush()?;
    }
    Ok(())
}
