type KvsMachine = std::collections::HashMap<String, usize>;

pub fn main() -> rufton::Result<()> {
    let Some(arg) = std::env::args().nth(1) else {
        return Err(rufton::Error::new("missing PORT argument"));
    };
    let port: u16 = arg.parse()?;

    let socket = rufton::LineFramedTcpSocket::bind(format!("127.0.0.1:{port}").parse()?)?;
    run(socket)?;

    Ok(())
}

fn run(mut socket: rufton::LineFramedTcpSocket) -> rufton::Result<()> {
    let mut buf = [0; 65535];
    let mut machine = KvsMachine::new();

    loop {
        let (len, src_addr) = socket.recv_from(&mut buf)?;

        let text = str::from_utf8(&buf[..len])?; // TODO: note
        let json = nojson::RawJson::parse(text)?;
        let request = json.value();

        let result = match request.to_member("method")?.required()?.as_string_str()? {
            "put" => {
                let params = request.to_member("params")?.required()?;
                let key: String = params.to_member("key")?.required()?.try_into()?;
                let value: usize = params.to_member("value")?.required()?.try_into()?;
                let old = machine.insert(key, value);
                format!(r#"{{"old": {}}}"#, nojson::Json(old))
            }
            "get" => {
                let params = request.to_member("params")?.required()?;
                let key: &str = params.to_member("key")?.required()?.try_into()?;
                let value = machine.get(key);
                format!(r#"{{"value": {}}}"#, nojson::Json(value))
            }
            method => format!(r#"{{"error": "unknown method: {method}"}}"#),
        };

        if let Some(id) = request.to_member("id")?.get() {
            let response = format!(r#"{{"jsonrpc":"2.0", "id":{id}, "result":{result}}}"#);
            socket.send_to(response.as_bytes(), src_addr)?;
        }
    }
}
