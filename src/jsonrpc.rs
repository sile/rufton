#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum JsonRpcRequestId {
    Integer(i64),
    String(String),
}

impl nojson::DisplayJson for JsonRpcRequestId {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        match self {
            JsonRpcRequestId::Integer(n) => f.value(n),
            JsonRpcRequestId::String(s) => f.string(s),
        }
    }
}

impl<'text, 'raw> TryFrom<nojson::RawJsonValue<'text, 'raw>> for JsonRpcRequestId {
    type Error = nojson::JsonParseError;

    fn try_from(value: nojson::RawJsonValue<'text, 'raw>) -> Result<Self, Self::Error> {
        match value.kind() {
            nojson::JsonValueKind::Integer => value.try_into().map(Self::Integer),
            nojson::JsonValueKind::String => value.try_into().map(Self::String),
            _ => Err(value.invalid("id must be an integer or string")),
        }
    }
}

/// JSON-RPC 2.0 predefined error codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonRpcPredefinedError {
    /// Invalid JSON was received by the server
    ParseError = -32700,
    /// The JSON sent is not a valid Request object
    InvalidRequest = -32600,
    /// The method does not exist / is not available
    MethodNotFound = -32601,
    /// Invalid method parameter(s)
    InvalidParams = -32602,
    /// Internal JSON-RPC error
    InternalError = -32603,
}

impl JsonRpcPredefinedError {
    pub fn code(self) -> i32 {
        match self {
            Self::ParseError => -32700,
            Self::InvalidRequest => -32600,
            Self::MethodNotFound => -32601,
            Self::InvalidParams => -32602,
            Self::InternalError => -32603,
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::ParseError => "Parse error",
            Self::InvalidRequest => "Invalid Request",
            Self::MethodNotFound => "Method not found",
            Self::InvalidParams => "Invalid params",
            Self::InternalError => "Internal error",
        }
    }
}

fn is_jsonrpc_2_0<'text, 'raw>(val: nojson::RawJsonValue<'text, 'raw>) -> bool {
    match val.to_unquoted_string_str() {
        Ok(version) => version == "2.0",
        Err(_) => false,
    }
}

fn require_jsonrpc_2_0<'text, 'raw>(
    val: nojson::RawJsonValue<'text, 'raw>,
) -> Result<(), nojson::JsonParseError> {
    if val.to_unquoted_string_str()? != "2.0" {
        return Err(val.invalid("unsupported JSON-RPC version"));
    }
    Ok(())
}

#[derive(Debug)]
pub struct JsonRpcRequest<'text> {
    json: nojson::RawJson<'text>,
    method: std::borrow::Cow<'text, str>,
    params_index: Option<usize>,
    id: Option<JsonRpcRequestId>,
}

impl<'text> JsonRpcRequest<'text> {
    pub fn parse(line: &'text [u8]) -> Result<Self, JsonRpcPredefinedError> {
        let json = std::str::from_utf8(line)
            .ok()
            .and_then(|line| nojson::RawJson::parse(line).ok())
            .ok_or(JsonRpcPredefinedError::ParseError)?;
        Self::from_json(json).ok_or(JsonRpcPredefinedError::InvalidRequest)
    }

    pub fn method(&self) -> &str {
        self.method.as_ref()
    }

    pub fn id(&self) -> Option<&JsonRpcRequestId> {
        self.id.as_ref()
    }

    pub fn params(&self) -> Option<nojson::RawJsonValue<'text, '_>> {
        self.params_index
            .and_then(|i| self.json.get_value_by_index(i))
    }

    pub fn json(&self) -> &nojson::RawJson<'text> {
        &self.json
    }

    pub fn into_json(self) -> nojson::RawJson<'text> {
        self.json
    }

    fn from_json(json: nojson::RawJson<'text>) -> Option<Self> {
        let mut parts = RequestParts::new();
        let value = json.value();
        for (key, val) in value.to_object().ok()? {
            let key = key.to_unquoted_string_str().ok()?;
            parts.apply_member(key.as_ref(), val)?;
        }
        parts.finish(json)
    }
}

struct RequestParts<'text> {
    has_jsonrpc: bool,
    method: Option<std::borrow::Cow<'text, str>>,
    id: Option<JsonRpcRequestId>,
    params_index: Option<usize>,
}

impl<'text> RequestParts<'text> {
    fn new() -> Self {
        Self {
            has_jsonrpc: false,
            method: None,
            id: None,
            params_index: None,
        }
    }

    fn apply_member<'raw>(
        &mut self,
        key: &str,
        val: nojson::RawJsonValue<'text, 'raw>,
    ) -> Option<()> {
        match key {
            "jsonrpc" => {
                if !is_jsonrpc_2_0(val) {
                    return None;
                }
                self.has_jsonrpc = true;
            }
            "method" => {
                self.method = Some(val.to_unquoted_string_str().ok()?);
            }
            "id" => {
                self.id = Some(JsonRpcRequestId::try_from(val).ok()?);
            }
            "params" => {
                if !matches!(
                    val.kind(),
                    nojson::JsonValueKind::Object | nojson::JsonValueKind::Array
                ) {
                    return None;
                }
                self.params_index = Some(val.index());
            }
            _ => {}
        }
        Some(())
    }

    fn finish(self, json: nojson::RawJson<'text>) -> Option<JsonRpcRequest<'text>> {
        if !self.has_jsonrpc {
            return None;
        }
        Some(JsonRpcRequest {
            json,
            method: self.method?,
            params_index: self.params_index,
            id: self.id,
        })
    }
}

#[derive(Debug)]
pub struct JsonRpcResponse<'text> {
    json: nojson::RawJson<'text>,
    result: Result<usize, usize>,
    id: Option<JsonRpcRequestId>,
}

impl<'text> JsonRpcResponse<'text> {
    pub fn parse(line: &'text str) -> Result<Self, nojson::JsonParseError> {
        let json = nojson::RawJson::parse(line)?;
        let mut parts = ResponseParts::new();

        let value = json.value();
        for (key, val) in value.to_object()? {
            let key_str = key.to_unquoted_string_str()?;
            parts.apply_member(key_str.as_ref(), val)?;
        }

        parts.finish(json)
    }

    pub fn id(&self) -> Option<&JsonRpcRequestId> {
        self.id.as_ref()
    }

    pub fn result(
        &self,
    ) -> Result<nojson::RawJsonValue<'text, '_>, nojson::RawJsonValue<'text, '_>> {
        match self.result {
            Ok(i) => Ok(self.json.get_value_by_index(i).expect("bug")),
            Err(i) => Err(self.json.get_value_by_index(i).expect("bug")),
        }
    }

    pub fn json(&self) -> &nojson::RawJson<'text> {
        &self.json
    }

    pub fn into_json(self) -> nojson::RawJson<'text> {
        self.json
    }
}

struct ResponseParts {
    has_jsonrpc: bool,
    has_id: bool,
    id: Option<JsonRpcRequestId>,
    result_index: Option<usize>,
    error_index: Option<usize>,
}

impl ResponseParts {
    fn new() -> Self {
        Self {
            has_jsonrpc: false,
            has_id: false,
            id: None,
            result_index: None,
            error_index: None,
        }
    }

    fn apply_member<'text, 'raw>(
        &mut self,
        key: &str,
        val: nojson::RawJsonValue<'text, 'raw>,
    ) -> Result<(), nojson::JsonParseError> {
        match key {
            "jsonrpc" => {
                require_jsonrpc_2_0(val)?;
                self.has_jsonrpc = true;
            }
            "id" => {
                if !val.kind().is_null() {
                    self.id = Some(JsonRpcRequestId::try_from(val)?);
                }
                self.has_id = true;
            }
            "result" => {
                self.result_index = Some(val.index());
            }
            "error" => {
                if !val.to_member("code")?.required()?.kind().is_integer() {
                    return Err(val.invalid("non integer error code"));
                }
                self.error_index = Some(val.index());
            }
            _ => {}
        }
        Ok(())
    }

    fn finish<'text>(
        self,
        json: nojson::RawJson<'text>,
    ) -> Result<JsonRpcResponse<'text>, nojson::JsonParseError> {
        if !self.has_jsonrpc {
            return Err(json.value().invalid("missing \"jsonrpc\" member"));
        }
        if !self.has_id {
            return Err(json.value().invalid("missing \"id\" member"));
        }

        let result = if let Some(i) = self.result_index {
            Ok(i)
        } else if let Some(i) = self.error_index {
            Err(i)
        } else {
            return Err(json
                .value()
                .invalid("either \"result\" or \"error\" member is required"));
        };

        Ok(JsonRpcResponse {
            json,
            result,
            id: self.id,
        })
    }
}
