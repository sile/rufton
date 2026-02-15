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
