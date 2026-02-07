use std::io::Write;
use std::net::SocketAddr;

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

fn send_response_ok<T: nojson::DisplayJson>(
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

fn send_response_err(
    socket: &mut rufton::LineFramedTcpSocket,
    dst: SocketAddr,
    request_id: Option<&rufton::JsonRpcRequestId>,
    code: i32,
    message: &str,
) -> std::io::Result<()> {
    let id = nojson::Json(request_id);
    let mut buf = Vec::new();
    write!(
        &mut buf,
        r#"{{"jsonrpc":"2.0","id":{},"error":{{"code":{},"message":"{}"}}}}"#,
        id, code, message
    )?;
    socket.send_to(&buf, dst)?;
    Ok(())
}

fn run_server(listen_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let socket_addr: std::net::SocketAddr = listen_addr.parse()?;
    let mut socket = rufton::LineFramedTcpSocket::bind(socket_addr)?;
    eprintln!("Echo server listening on {}", listen_addr);

    let mut buf = [0u8; 65535];
    loop {
        let (len, src_addr) = socket.recv_from(&mut buf)?;
        match rufton::JsonRpcRequest::parse(&buf[..len]) {
            Err(e) => {
                send_response_err(&mut socket, src_addr, None, e.code(), e.message())?;
            }
            Ok(req) => {
                let Some(req_id) = req.id().cloned() else {
                    continue;
                };
                let json = req.into_json().into_owned();
                send_response_ok(&mut socket, src_addr, &req_id, json)?;
            }
        }
    }
}
