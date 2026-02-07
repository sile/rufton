pub type RecentCommands = std::collections::BTreeMap<noraft::LogIndex, JsonLineValue>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProposalId {
    node_id: noraft::NodeId,
    generation: u64,
    local_seqno: u64,
}

impl ProposalId {
    pub(crate) fn new(node_id: noraft::NodeId, generation: u64, local_seqno: u64) -> Self {
        Self {
            node_id,
            generation,
            local_seqno,
        }
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
            node_id: noraft::NodeId::new(node_id),
            generation,
            local_seqno,
        })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct JsonLineValue(std::sync::Arc<nojson::RawJsonOwned>);

impl JsonLineValue {
    pub(crate) fn new_internal<T: nojson::DisplayJson>(v: T) -> Self {
        let line = nojson::Json(v).to_string();
        let json = nojson::RawJsonOwned::parse(line).expect("infallible");
        Self(std::sync::Arc::new(json))
    }

    pub fn new<T: nojson::DisplayJson>(v: T) -> Self {
        Self::new_internal(v)
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

impl nojson::DisplayJson for JsonLineValue {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        write!(f.inner_mut(), "{}", self.0.text())
    }
}

impl std::fmt::Debug for JsonLineValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.text())
    }
}

impl std::fmt::Display for JsonLineValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.text())
    }
}

// TODO
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryMessage {
    Redirect {
        from: noraft::NodeId,
        proposal_id: ProposalId,
    },
    Proposed {
        proposal_id: ProposalId,
        position: noraft::LogPosition,
    },
}

impl nojson::DisplayJson for QueryMessage {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        match self {
            QueryMessage::Redirect { from, proposal_id } => f.object(|f| {
                f.member("type", "Redirect")?;
                f.member("from", from.get())?;
                f.member("proposal_id", proposal_id)
            }),
            QueryMessage::Proposed {
                proposal_id,
                position,
            } => f.object(|f| {
                f.member("type", "Proposed")?;
                f.member("proposal_id", proposal_id)?;
                f.member("term", position.term.get())?;
                f.member("index", position.index.get())
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
                Ok(QueryMessage::Redirect {
                    from: noraft::NodeId::new(from),
                    proposal_id,
                })
            }
            "Proposed" => {
                let proposal_id = value.to_member("proposal_id")?.required()?.try_into()?;
                let term = noraft::Term::new(value.to_member("term")?.required()?.try_into()?);
                let index =
                    noraft::LogIndex::new(value.to_member("index")?.required()?.try_into()?);
                Ok(QueryMessage::Proposed {
                    proposal_id,
                    position: noraft::LogPosition { term, index },
                })
            }
            ty => Err(value.invalid(format!("unknown query message type: {ty}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    AddNode {
        proposal_id: ProposalId,
        id: noraft::NodeId,
    },
    RemoveNode {
        proposal_id: ProposalId,
        id: noraft::NodeId,
    },
    Apply {
        proposal_id: ProposalId,
        command: JsonLineValue,
    },
    Query,
}

impl nojson::DisplayJson for Command {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        match self {
            Command::AddNode { proposal_id, id } => f.object(|f| {
                f.member("type", "AddNode")?; // TODO: add prefix to indicate internal messages
                f.member("proposal_id", proposal_id)?;
                f.member("id", id.get())
            }),
            Command::RemoveNode { proposal_id, id } => f.object(|f| {
                f.member("type", "RemoveNode")?;
                f.member("proposal_id", proposal_id)?;
                f.member("id", id.get())
            }),
            Command::Apply {
                proposal_id,
                command,
            } => f.object(|f| {
                f.member("type", "Apply")?;
                f.member("proposal_id", proposal_id)?;
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
            "AddNode" => {
                let proposal_id = value.to_member("proposal_id")?.required()?.try_into()?;
                let id = value.to_member("id")?.required()?.try_into()?;
                Ok(Command::AddNode {
                    proposal_id,
                    id: noraft::NodeId::new(id),
                })
            }
            "RemoveNode" => {
                let proposal_id = value.to_member("proposal_id")?.required()?.try_into()?;
                let id = value.to_member("id")?.required()?.try_into()?;
                Ok(Command::RemoveNode {
                    proposal_id,
                    id: noraft::NodeId::new(id),
                })
            }
            "Apply" => {
                let proposal_id = value.to_member("proposal_id")?.required()?.try_into()?;
                let command_json = value.to_member("command")?.required()?;
                let command = JsonLineValue::new_internal(command_json);
                Ok(Command::Apply {
                    proposal_id,
                    command,
                })
            }
            "Query" => Ok(Command::Query),
            ty => Err(value.invalid(format!("unknown command type: {ty}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    SetTimeout(noraft::Role),
    AppendStorageEntry(JsonLineValue),
    BroadcastMessage(JsonLineValue),
    SendMessage(noraft::NodeId, JsonLineValue),
    SendSnapshot(noraft::NodeId),
    // TODO: NotifyEvent
    Commit {
        proposal_id: Option<ProposalId>,
        index: noraft::LogIndex,
        command: Option<JsonLineValue>,
    },
    Query {
        proposal_id: ProposalId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageEntry {
    Term(noraft::Term),
    VotedFor(Option<noraft::NodeId>),
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
                Ok(StorageEntry::VotedFor(node_id.map(noraft::NodeId::new)))
            }
            "NodeGeneration" => {
                let generation = value.to_member("generation")?.required()?.try_into()?;
                Ok(StorageEntry::NodeGeneration(generation))
            }
            ty => Err(value.invalid(format!("unknown storage entry type: {ty}"))),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct NodeStateMachine {
    pub nodes: std::collections::BTreeSet<noraft::NodeId>,
}
