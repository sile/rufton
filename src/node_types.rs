#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(noraft::NodeId);

impl NodeId {
    pub fn new(node_id: u64) -> Self {
        Self(noraft::NodeId::new(node_id))
    }

    pub fn from_localhost_port(port: u16) -> Self {
        Self::new(u64::from(port))
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }

    pub fn to_localhost_addr(self) -> crate::Result<std::net::SocketAddr> {
        let port = u16::try_from(self.get()).map_err(|_| {
            crate::Error::new(format!(
                "node id {} is out of localhost port range",
                self.get()
            ))
        })?;
        Ok(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
    }

    pub(crate) fn from_inner(node_id: noraft::NodeId) -> Self {
        Self(node_id)
    }

    pub(crate) fn into_inner(self) -> noraft::NodeId {
        self.0
    }
}

impl From<u64> for NodeId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<NodeId> for u64 {
    fn from(value: NodeId) -> Self {
        value.get()
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

impl nojson::DisplayJson for NodeId {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        self.get().fmt(f)
    }
}

impl<'text, 'raw> TryFrom<nojson::RawJsonValue<'text, 'raw>> for NodeId {
    type Error = nojson::JsonParseError;

    fn try_from(value: nojson::RawJsonValue<'text, 'raw>) -> Result<Self, Self::Error> {
        let node_id: u64 = value.try_into()?;
        Ok(Self::new(node_id))
    }
}

pub type RecentCommands = std::collections::BTreeMap<noraft::LogIndex, JsonValue>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ProposalId {
    node_id: NodeId,
    generation: u64,
    local_seqno: u64,
}

impl ProposalId {
    pub(crate) fn new(node_id: NodeId, generation: u64, local_seqno: u64) -> Self {
        Self {
            node_id,
            generation,
            local_seqno,
        }
    }

    pub(crate) fn is_proposer(&self, node_id: NodeId, generation: u64) -> bool {
        self.node_id == node_id && self.generation == generation
    }
}

impl nojson::DisplayJson for ProposalId {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        [self.node_id.get(), self.generation, self.local_seqno].fmt(f)
    }
}

impl<'text, 'raw> TryFrom<nojson::RawJsonValue<'text, 'raw>> for ProposalId {
    type Error = nojson::JsonParseError;

    fn try_from(value: nojson::RawJsonValue<'text, 'raw>) -> Result<Self, Self::Error> {
        let [node_id, generation, local_seqno] = value.try_into()?;
        Ok(ProposalId {
            node_id: NodeId::new(node_id),
            generation,
            local_seqno,
        })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct JsonValue(std::sync::Arc<nojson::RawJsonOwned>);

impl JsonValue {
    pub fn new<T: nojson::DisplayJson>(v: T) -> Self {
        let line = nojson::Json(v).to_string();
        let json = nojson::RawJsonOwned::parse(line).expect("infallible");
        Self(std::sync::Arc::new(json))
    }

    pub fn get(&self) -> nojson::RawJsonValue<'_, '_> {
        self.0.value()
    }

    pub(crate) fn get_member<'a, T>(&'a self, name: &str) -> Result<T, nojson::JsonParseError>
    where
        T: TryFrom<nojson::RawJsonValue<'a, 'a>, Error = nojson::JsonParseError>,
    {
        self.get().to_member(name)?.required()?.try_into()
    }

    pub(crate) fn get_optional_member<'a, T>(
        &'a self,
        name: &str,
    ) -> Result<Option<T>, nojson::JsonParseError>
    where
        T: TryFrom<nojson::RawJsonValue<'a, 'a>, Error = nojson::JsonParseError>,
    {
        self.get().to_member(name)?.try_into()
    }
}

impl nojson::DisplayJson for JsonValue {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        write!(f.inner_mut(), "{}", self.0.text())
    }
}

impl std::fmt::Debug for JsonValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.text())
    }
}

impl std::fmt::Display for JsonValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.text())
    }
}

// TODO
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueryMessage {
    Redirect {
        from: NodeId,
        proposal_id: ProposalId,
        request: JsonValue,
    },
    Proposed {
        proposal_id: ProposalId,
        position: noraft::LogPosition,
        request: JsonValue,
    },
}

impl nojson::DisplayJson for QueryMessage {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        match self {
            QueryMessage::Redirect {
                from,
                proposal_id,
                request,
            } => f.object(|f| {
                f.member("type", "Redirect")?;
                f.member("from", from.get())?;
                f.member("proposal_id", proposal_id)?;
                f.member("request", request)
            }),
            QueryMessage::Proposed {
                proposal_id,
                position,
                request,
            } => f.object(|f| {
                f.member("type", "Proposed")?;
                f.member("proposal_id", proposal_id)?;
                f.member("term", position.term.get())?;
                f.member("index", position.index.get())?;
                f.member("request", request)
            }),
        }
    }
}

impl<'text, 'raw> TryFrom<nojson::RawJsonValue<'text, 'raw>> for QueryMessage {
    type Error = nojson::JsonParseError;

    fn try_from(value: nojson::RawJsonValue<'text, 'raw>) -> Result<Self, Self::Error> {
        let ty = value
            .to_member("type")?
            .required()?
            .to_unquoted_string_str()?;
        match ty.as_ref() {
            "Redirect" => {
                let from: u64 = value.to_member("from")?.required()?.try_into()?;
                let proposal_id = value.to_member("proposal_id")?.required()?.try_into()?;
                let request_json = value.to_member("request")?.required()?;
                let request = JsonValue::new(request_json);
                Ok(QueryMessage::Redirect {
                    from: NodeId::new(from),
                    proposal_id,
                    request,
                })
            }
            "Proposed" => {
                let proposal_id = value.to_member("proposal_id")?.required()?.try_into()?;
                let term = noraft::Term::new(value.to_member("term")?.required()?.try_into()?);
                let index =
                    noraft::LogIndex::new(value.to_member("index")?.required()?.try_into()?);
                let request_json = value.to_member("request")?.required()?;
                let request = JsonValue::new(request_json);
                Ok(QueryMessage::Proposed {
                    proposal_id,
                    position: noraft::LogPosition { term, index },
                    request,
                })
            }
            ty => Err(value.invalid(format!("unknown query message type: {ty}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Command {
    Apply {
        proposal_id: ProposalId,
        source: JsonValue,
        command: JsonValue,
    },
    Query,
}

impl nojson::DisplayJson for Command {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        match self {
            Command::Apply {
                proposal_id,
                source,
                command,
            } => f.object(|f| {
                f.member("type", "Apply")?;
                f.member("proposal_id", proposal_id)?;
                f.member("source", source)?;
                f.member("command", command)
            }),
            Command::Query => f.object(|f| f.member("type", "Query")),
        }
    }
}

impl<'text, 'raw> TryFrom<nojson::RawJsonValue<'text, 'raw>> for Command {
    type Error = nojson::JsonParseError;

    fn try_from(value: nojson::RawJsonValue<'text, 'raw>) -> Result<Self, Self::Error> {
        let ty = value
            .to_member("type")?
            .required()?
            .to_unquoted_string_str()?;
        match ty.as_ref() {
            "Apply" => {
                let proposal_id = value.to_member("proposal_id")?.required()?.try_into()?;
                let source_json = value.to_member("source")?.required()?;
                let source = JsonValue::new(source_json);
                let command_json = value.to_member("command")?.required()?;
                let command = JsonValue::new(command_json);
                Ok(Command::Apply {
                    proposal_id,
                    source,
                    command,
                })
            }
            "Query" => Ok(Command::Query),
            ty => Err(value.invalid(format!("unknown command type: {ty}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyAction {
    is_proposer: bool,
    index: noraft::LogIndex,
    source: JsonValue,
    request: JsonValue,
}

impl ApplyAction {
    pub(crate) fn new(
        is_proposer: bool,
        index: noraft::LogIndex,
        source: JsonValue,
        request: JsonValue,
    ) -> Self {
        Self {
            is_proposer,
            index,
            source,
            request,
        }
    }

    pub fn index(&self) -> noraft::LogIndex {
        self.index
    }

    pub fn request(&self) -> nojson::RawJsonValue<'_, '_> {
        self.request.get()
    }

    pub fn source(&self) -> Option<nojson::RawJsonValue<'_, '_>> {
        self.is_proposer.then(|| self.source.get())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    SetTimeout,
    AppendStorageEntry(JsonValue),
    Broadcast(JsonValue),
    Send(NodeId, JsonValue),
    SendSnapshot(NodeId),
    NotifyEvent(Event),
    Apply(ApplyAction),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

impl NodeRole {
    pub(crate) fn from_inner(role: noraft::Role) -> Self {
        match role {
            noraft::Role::Follower => Self::Follower,
            noraft::Role::Candidate => Self::Candidate,
            noraft::Role::Leader => Self::Leader,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    RoleChanged { from: NodeRole, to: NodeRole },
    BecameLeader { term: noraft::Term },
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn role_label(role: NodeRole) -> &'static str {
            match role {
                NodeRole::Follower => "Follower",
                NodeRole::Candidate => "Candidate",
                NodeRole::Leader => "Leader",
            }
        }

        match self {
            Event::RoleChanged { from, to } => {
                write!(
                    f,
                    "role changed: {} -> {}",
                    role_label(*from),
                    role_label(*to)
                )
            }
            Event::BecameLeader { term } => write!(f, "became leader (term={})", term.get()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageEntry {
    Term(noraft::Term),
    VotedFor(Option<NodeId>),
    NodeGeneration(u64),
}

impl nojson::DisplayJson for StorageEntry {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        match self {
            StorageEntry::Term(term) => f.object(|f| {
                f.member("type", "Term")?;
                f.member("term", term.get())
            }),
            StorageEntry::VotedFor(node_id) => f.object(|f| {
                f.member("type", "VotedFor")?;
                f.member("node_id", node_id.map(|id| id.get()))
            }),
            StorageEntry::NodeGeneration(generation) => f.object(|f| {
                f.member("type", "NodeGeneration")?;
                f.member("generation", generation)
            }),
        }
    }
}

impl<'text, 'raw> TryFrom<nojson::RawJsonValue<'text, 'raw>> for StorageEntry {
    type Error = nojson::JsonParseError;

    fn try_from(value: nojson::RawJsonValue<'text, 'raw>) -> Result<Self, Self::Error> {
        let ty = value
            .to_member("type")?
            .required()?
            .to_unquoted_string_str()?;
        match ty.as_ref() {
            "Term" => {
                let term = value.to_member("term")?.required()?.try_into()?;
                Ok(StorageEntry::Term(noraft::Term::new(term)))
            }
            "VotedFor" => {
                let node_id: Option<u64> = value.to_member("node_id")?.try_into()?;
                Ok(StorageEntry::VotedFor(node_id.map(NodeId::new)))
            }
            "NodeGeneration" => {
                let generation = value.to_member("generation")?.required()?.try_into()?;
                Ok(StorageEntry::NodeGeneration(generation))
            }
            ty => Err(value.invalid(format!("unknown storage entry type: {ty}"))),
        }
    }
}
