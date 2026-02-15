pub type Machine = std::collections::HashMap<String, usize>;

pub fn apply(machine: &mut Machine, request: nojson::RawJsonValue) -> rufton::Result<String> {
    let method: &str = request.to_member("method")?.required()?.try_into()?;
    match method {
        "put" => {
            let params = request.to_member("params")?.required()?;
            let key = params.to_member("key")?.required()?.try_into()?;
            let value = params.to_member("value")?.required()?.try_into()?;
            let old_value = machine.insert(key, value);
            Ok(format!(r#"{{ "old_value":{} }}"#, nojson::Json(old_value)))
        }
        "get" => {
            let params = request.to_member("params")?.required()?;
            let key: &str = params.to_member("key")?.required()?.try_into()?;
            let value = machine.get(key);
            Ok(format!(r#"{{ "value":{} }}"#, nojson::Json(value)))
        }
        _ => Err(format!("unknown method: {}", method).into()),
    }
}

pub fn recv_request<'a>(
    socket: &std::net::UdpSocket,
    buf: &'a mut [u8],
) -> rufton::Result<(nojson::RawJson<'a>, std::net::SocketAddr)> {
    let (recv_size, client_addr) = socket.recv_from(buf)?;
    let text = std::str::from_utf8(&buf[..recv_size])?;
    let json = nojson::RawJson::parse(text)?;
    Ok((json, client_addr))
}

pub fn send_response(
    socket: &std::net::UdpSocket,
    request: nojson::RawJsonValue<'_, '_>,
    result: rufton::Result<String>,
    client_addr: std::net::SocketAddr,
) -> rufton::Result<()> {
    // TODO: use optional()
    let Some(id) = request.to_member("id")?.get() else {
        return Ok(());
    };

    let result_or_error = match result {
        Err(e) => format!(r#""error": {{"code": -1, "message":{e}}}"#),
        Ok(v) => format!(r#""result": {v}"#),
    };
    let response = format!(r#"{{"jsonrpc":"2.0", "id":{id}, {result_or_error}}}"#);
    socket.send_to(response.as_bytes(), client_addr)?;

    Ok(())
}
