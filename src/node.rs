#[derive(Debug)]
pub struct RaftNode {
    pub inner: raftbare::Node,
    pub addr: std::net::SocketAddr,
    pub machine: RaftNodeStateMachine,
    pub action_queue: std::collections::VecDeque<RaftNodeAction>,
    pub recent_commands: std::collections::BTreeMap<raftbare::LogPosition, JsonLineValue>,
    pub initialized: bool,
    pub instance_id: u64,
    pub local_command_seqno: u64,
}

impl RaftNode {
    pub fn new(id: raftbare::NodeId, addr: std::net::SocketAddr, instance_id: u64) -> Self {
        Self {
            inner: raftbare::Node::start(id),
            addr,
            machine: RaftNodeStateMachine::default(),
            action_queue: std::collections::VecDeque::new(),

            recent_commands: std::collections::BTreeMap::new(),
            initialized: false,
            instance_id,
            local_command_seqno: 0,
        }
    }

    pub fn init_cluster(&mut self) -> bool {
        if self.initialized {
            return false;
        }

        let initial_members = [self.inner.id()];
        self.inner.create_cluster(&initial_members);
        self.initialized = true;

        self.propose_add_node(self.inner.id(), self.addr)
            .expect("infallible");

        true
    }

    // NOTE: Propsals should be treated as timeout by clients in the following cases:
    // - Taking too long time (simple timeout)
    // - Commit application to the state machine managed the node received the proposal was skipped by snapshot
    // - Redirected proposal was discarded by any reasons (e.g. node down, redirect limit reached)

    pub fn propose_add_node(
        &mut self,
        id: raftbare::NodeId,
        addr: std::net::SocketAddr,
    ) -> Result<ProposalId, NotInitialized> {
        if !self.initialized {
            return Err(NotInitialized);
        }

        let proposal_id = ProposalId {
            node_id: self.inner.id(),
            instance_id: self.instance_id,
            local_seqno: self.local_command_seqno,
        };
        self.local_command_seqno += 1;

        if !self.inner.role().is_leader() {
            todo!("{:?}", self.inner.role())
        }

        let position = self.inner.propose_command();

        let command = RaftNodeCommand::AddNode {
            proposal_id,
            id,
            addr,
        };
        let value = JsonLineValue::new_internal(command);
        self.recent_commands.insert(position, value);

        Ok(proposal_id)
    }

    fn push_action(&mut self, action: RaftNodeAction) {
        self.action_queue.push_back(action);
    }

    pub fn next_action(&mut self) -> Option<RaftNodeAction> {
        if !self.initialized {
            return None;
        }

        while let Some(inner_action) = self.inner.actions_mut().next() {
            match inner_action {
                raftbare::Action::SetElectionTimeout => {
                    self.push_action(RaftNodeAction::SetTimeout(self.inner.role()));
                }
                raftbare::Action::SaveCurrentTerm => {
                    let term = self.inner.current_term();
                    let entry = StorageEntry::Term(term);
                    let value = JsonLineValue::new_internal(entry);
                    self.push_action(RaftNodeAction::AppendStorageEntry(value));
                }
                raftbare::Action::SaveVotedFor => {
                    let voted_for = self.inner.voted_for();
                    let entry = StorageEntry::VotedFor(voted_for);
                    let value = JsonLineValue::new_internal(entry);
                    self.push_action(RaftNodeAction::AppendStorageEntry(value));
                }
                raftbare::Action::BroadcastMessage(_) => {
                    todo!("BroadcastMessage action")
                }
                raftbare::Action::AppendLogEntries(entries) => {
                    let value = JsonLineValue::new_internal(nojson::json(|f| {
                        self.fmt_log_entries_json(f, &entries)
                    }));
                    self.push_action(RaftNodeAction::AppendStorageEntry(value));
                }
                raftbare::Action::SendMessage(_, _) => {
                    todo!("SendMessage action")
                }
                raftbare::Action::InstallSnapshot(_) => {
                    todo!("InstallSnapshot action")
                }
            }
        }
        self.action_queue.pop_front()
    }

    fn fmt_log_position_members(
        &self,
        f: &mut nojson::JsonObjectFormatter<'_, '_, '_>,
        position: raftbare::LogPosition,
    ) -> std::fmt::Result {
        f.member("term", position.term.get())?;
        f.member("index", position.index.get())
    }

    fn fmt_log_entry_members(
        &self,
        f: &mut nojson::JsonObjectFormatter<'_, '_, '_>,
        pos: raftbare::LogPosition,
        entry: &raftbare::LogEntry,
    ) -> std::fmt::Result {
        match entry {
            raftbare::LogEntry::Term(term) => {
                f.member("type", "Term")?;
                f.member("term", term.get())
            }
            raftbare::LogEntry::ClusterConfig(config) => {
                // NOTE: This crate does not use non voters
                f.member("type", "ClusterConfig")?;
                f.member(
                    "voters",
                    nojson::array(|f| f.elements(config.voters.iter().map(|v| v.get()))),
                )?;
                f.member(
                    "new_voters",
                    nojson::array(|f| f.elements(config.new_voters.iter().map(|v| v.get()))),
                )
            }
            raftbare::LogEntry::Command => {
                f.member("type", "Command")?;
                let command = self.recent_commands.get(&pos).expect("bug");
                f.member("value", command)
            }
        }
    }

    fn fmt_log_entries_json(
        &self,
        f: &mut nojson::JsonFormatter<'_, '_>,
        entries: &raftbare::LogEntries,
    ) -> std::fmt::Result {
        f.object(|f| {
            self.fmt_log_position_members(f, entries.prev_position())?;
            f.member(
                "entries",
                nojson::array(|f| {
                    for (pos, entry) in entries.iter_with_positions() {
                        f.element(nojson::object(|f| {
                            self.fmt_log_entry_members(f, pos, &entry)
                        }))?;
                    }
                    Ok(())
                }),
            )
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotInitialized;

impl std::fmt::Display for NotInitialized {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Raft node is not initialized")
    }
}

impl std::error::Error for NotInitialized {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProposalId {
    node_id: raftbare::NodeId,
    instance_id: u64,
    local_seqno: u64,
}

impl nojson::DisplayJson for ProposalId {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        [self.node_id.get(), self.instance_id, self.local_seqno].fmt(f)
    }
}

impl<'text, 'raw> TryFrom<nojson::RawJsonValue<'text, 'raw>> for ProposalId {
    type Error = nojson::JsonParseError;

    fn try_from(value: nojson::RawJsonValue<'text, 'raw>) -> Result<Self, Self::Error> {
        let [node_id, instance_id, local_seqno] = value.try_into()?;
        Ok(ProposalId {
            node_id: raftbare::NodeId::new(node_id),
            instance_id,
            local_seqno,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonLineValue(std::sync::Arc<nojson::RawJsonOwned>);

impl JsonLineValue {
    // Assumes the input does not include newlines
    fn new_internal<T: nojson::DisplayJson>(v: T) -> Self {
        let line = nojson::Json(v).to_string();
        let json = nojson::RawJsonOwned::parse(line).expect("infallible");
        Self(std::sync::Arc::new(json))
    }

    pub fn get(&self) -> nojson::RawJsonValue<'_, '_> {
        self.0.value()
    }
}

impl nojson::DisplayJson for JsonLineValue {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        writeln!(f.inner_mut(), "{}", self.0.text())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftNodeCommand {
    AddNode {
        proposal_id: ProposalId,
        id: raftbare::NodeId,
        addr: std::net::SocketAddr,
    },
}

impl nojson::DisplayJson for RaftNodeCommand {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        match self {
            RaftNodeCommand::AddNode {
                proposal_id,
                id,
                addr,
            } => f.object(|f| {
                f.member("type", "AddNode")?;
                f.member("proposal_id", proposal_id)?;
                f.member("id", id.get())?;
                f.member("addr", addr)
            }),
        }
    }
}

impl<'text, 'raw> TryFrom<nojson::RawJsonValue<'text, 'raw>> for RaftNodeCommand {
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
                let addr = value.to_member("addr")?.required()?.try_into()?;
                Ok(RaftNodeCommand::AddNode {
                    proposal_id,
                    id: raftbare::NodeId::new(id),
                    addr,
                })
            }
            ty => Err(value.invalid(format!("unknown command type: {ty}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftNodeAction {
    SetTimeout(raftbare::Role),
    AppendStorageEntry(JsonLineValue),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageEntry {
    Term(raftbare::Term),
    VotedFor(Option<raftbare::NodeId>),
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
                Ok(StorageEntry::Term(raftbare::Term::new(term)))
            }
            "VotedFor" => {
                let node_id: Option<u64> = value.to_member("node_id")?.try_into()?;
                Ok(StorageEntry::VotedFor(node_id.map(raftbare::NodeId::new)))
            }
            ty => Err(value.invalid(format!("unknown storage entry type: {ty}"))),
        }
    }
}

#[derive(Debug, Default)]
pub struct RaftNodeStateMachine {
    pub node_addrs: std::collections::BTreeMap<raftbare::NodeId, std::net::SocketAddr>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_cluster() {
        let mut node = RaftNode::new(node_id(0), addr(9000), 0);
        assert!(node.init_cluster());
        assert!(!node.init_cluster());
        assert_eq!(node.next_action(), Some(set_leader_timeout_action()));
        assert_eq!(
            node.next_action(),
            Some(append_storage_entry_action(r#"{"type":"Term","term":1}"#))
        );
        assert_eq!(
            node.next_action(),
            Some(append_storage_entry_action(
                r#"{"type":"VotedFor","node_id":0}"#
            ))
        );
        assert!(matches!(
            node.next_action(),
            Some(RaftNodeAction::AppendStorageEntry(_))
        ));
        assert_eq!(node.next_action(), None);
    }

    #[test]
    fn propose_add_node() {
        let mut node = RaftNode::new(node_id(0), addr(9000), 0);
        assert!(node.init_cluster());

        let _proposal_id = node.propose_add_node(node_id(1), addr(9001)).expect("ok");
        // assert_eq!(node.next_action(), None);
    }

    fn append_storage_entry_action(json: &str) -> RaftNodeAction {
        let raw_json = nojson::RawJsonOwned::parse(json.to_string()).expect("invalid json");
        let value = JsonLineValue(std::sync::Arc::new(raw_json));
        RaftNodeAction::AppendStorageEntry(value)
    }

    fn set_leader_timeout_action() -> RaftNodeAction {
        RaftNodeAction::SetTimeout(raftbare::Role::Leader)
    }

    fn node_id(n: u64) -> raftbare::NodeId {
        raftbare::NodeId::new(n)
    }

    fn addr(port: u16) -> std::net::SocketAddr {
        std::net::SocketAddr::from(([127, 0, 0, 1], port))
    }
}
