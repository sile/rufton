pub type RecentCommands = std::collections::BTreeMap<raftbare::LogIndex, JsonLineValue>;

#[derive(Debug)]
pub struct RaftNode {
    pub inner: raftbare::Node,
    pub addr: std::net::SocketAddr,
    pub machine: RaftNodeStateMachine,
    pub action_queue: std::collections::VecDeque<Action>,
    pub recent_commands: RecentCommands,
    pub initialized: bool,
    pub instance_id: u64,
    pub local_command_seqno: u64,
    pub applied_index: raftbare::LogIndex,
    pub dirty_members: bool,
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
            applied_index: raftbare::LogIndex::ZERO,
            dirty_members: false,
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

        let command = Command::AddNode {
            proposal_id,
            id,
            addr,
        };
        let value = JsonLineValue::new_internal(command);
        self.recent_commands.insert(position.index, value); // TODO: write why use index here (not position)

        Ok(proposal_id)
    }

    fn push_action(&mut self, action: Action) {
        self.action_queue.push_back(action);
    }

    fn maybe_sync_raft_members(&mut self) {
        if !self.dirty_members {
            return;
        }
        if !self.inner.role().is_leader() {
            return;
        }
        if self.inner.config().is_joint_consensus() {
            return;
        }

        // Get current cluster config
        let current_config = self.inner.config();
        let current_voters: std::collections::BTreeSet<_> =
            current_config.voters.iter().copied().collect();

        // Get voters from state machine addresses
        let machine_voters: std::collections::BTreeSet<_> =
            self.machine.node_addrs.keys().copied().collect();

        // Calculate differences
        let nodes_to_add: Vec<_> = machine_voters
            .iter()
            .filter(|id| !current_voters.contains(id))
            .copied()
            .collect();
        let nodes_to_remove: Vec<_> = current_voters
            .iter()
            .filter(|id| !machine_voters.contains(id))
            .copied()
            .collect();

        // If there are changes, propose new configuration
        if !nodes_to_add.is_empty() || !nodes_to_remove.is_empty() {
            let new_config = current_config.to_joint_consensus(&nodes_to_add, &nodes_to_remove);
            let pos = self.inner.propose_config(new_config);
            assert_ne!(pos, raftbare::LogPosition::INVALID);
        }

        self.dirty_members = false;
    }

    pub fn handle_message(&mut self, message: &JsonLineValue) -> bool {
        // TODO: Remove String conversion
        let Ok(ty) = message.get_member::<String>("type") else {
            return false;
        };

        // TODO:
        // - Convert message to raftbare message
        // - update recent commands if needs (check inner's log)

        match ty.as_str() {
            _ => false,
        }
    }

    pub fn next_action(&mut self) -> Option<Action> {
        if !self.initialized {
            return None;
        }

        self.maybe_sync_raft_members();

        while let Some(inner_action) = self.inner.actions_mut().next() {
            match inner_action {
                raftbare::Action::SetElectionTimeout => {
                    self.push_action(Action::SetTimeout(self.inner.role()));
                }
                raftbare::Action::SaveCurrentTerm => {
                    let term = self.inner.current_term();
                    let entry = StorageEntry::Term(term);
                    let value = JsonLineValue::new_internal(entry);
                    self.push_action(Action::AppendStorageEntry(value));
                }
                raftbare::Action::SaveVotedFor => {
                    let voted_for = self.inner.voted_for();
                    let entry = StorageEntry::VotedFor(voted_for);
                    let value = JsonLineValue::new_internal(entry);
                    self.push_action(Action::AppendStorageEntry(value));
                }
                raftbare::Action::BroadcastMessage(message) => {
                    let value = JsonLineValue::new_internal(nojson::json(|f| {
                        crate::conv::fmt_message(f, &message, &self.recent_commands)
                    }));
                    self.push_action(Action::BroadcastMessage(value));
                }
                raftbare::Action::AppendLogEntries(entries) => {
                    let value = JsonLineValue::new_internal(nojson::json(|f| {
                        crate::conv::fmt_log_entries(f, &entries, &self.recent_commands)
                    }));
                    self.push_action(Action::AppendStorageEntry(value));
                }
                raftbare::Action::SendMessage(_, _) => {
                    todo!("SendMessage action")
                }
                raftbare::Action::InstallSnapshot(_) => {
                    todo!("InstallSnapshot action")
                }
            }
        }

        for i in self.applied_index.get()..self.inner.commit_index().get() {
            let index = raftbare::LogIndex::new(i + 1);

            let Some(command) = self.recent_commands.get(&index).cloned() else {
                // Igores non-command entries as they are handled by the raftbare layer
                continue;
            };
            let proposal_id = command.get_member("proposal_id").expect("bug");

            // TODO: Remove String conversion
            let command = match command.get_member::<String>("type").expect("bug").as_str() {
                "AddNode" => {
                    self.handle_add_node(&command).expect("bug");
                    None
                }
                "Apply" => Some(command),
                _ => panic!("bug"),
            };

            self.push_action(Action::Commit {
                index,
                proposal_id,
                command,
            });
        }
        self.applied_index = self.inner.commit_index();

        self.action_queue.pop_front()
    }

    fn handle_add_node(&mut self, command: &JsonLineValue) -> Result<(), nojson::JsonParseError> {
        let id = raftbare::NodeId::new(command.get_member("id")?);
        let addr = command.get_member("addr")?;

        if self.machine.node_addrs.contains_key(&id) {
            return Ok(());
        }

        self.machine.node_addrs.insert(id, addr);
        self.dirty_members = true;

        Ok(())
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

    fn get_member<'a, T>(&'a self, name: &str) -> Result<T, nojson::JsonParseError>
    where
        T: TryFrom<nojson::RawJsonValue<'a, 'a>, Error = nojson::JsonParseError>,
    {
        self.get().to_member(name)?.required()?.try_into()
    }
}

impl nojson::DisplayJson for JsonLineValue {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        writeln!(f.inner_mut(), "{}", self.0.text())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    AddNode {
        proposal_id: ProposalId,
        id: raftbare::NodeId,
        addr: std::net::SocketAddr,
    },
    Apply {
        proposal_id: ProposalId,
        command: JsonLineValue,
    },
}

impl nojson::DisplayJson for Command {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        match self {
            Command::AddNode {
                proposal_id,
                id,
                addr,
            } => f.object(|f| {
                f.member("type", "AddNode")?;
                f.member("proposal_id", proposal_id)?;
                f.member("id", id.get())?;
                f.member("addr", addr)
            }),
            Command::Apply {
                proposal_id,
                command,
            } => f.object(|f| {
                f.member("type", "Apply")?;
                f.member("proposal_id", proposal_id)?;
                f.member("command", command)
            }),
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
                let addr = value.to_member("addr")?.required()?.try_into()?;
                Ok(Command::AddNode {
                    proposal_id,
                    id: raftbare::NodeId::new(id),
                    addr,
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
            ty => Err(value.invalid(format!("unknown command type: {ty}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    SetTimeout(raftbare::Role),
    AppendStorageEntry(JsonLineValue),
    BroadcastMessage(JsonLineValue),
    Commit {
        proposal_id: ProposalId,
        index: raftbare::LogIndex,
        command: Option<JsonLineValue>,
    },
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
            Some(Action::AppendStorageEntry(_))
        ));
        assert!(matches!(node.next_action(), Some(Action::Commit { .. })));
        assert_eq!(node.next_action(), None);

        // Check that the initial node was added to cluster members
        assert_eq!(node.machine.node_addrs.len(), 1);
        assert_eq!(node.machine.node_addrs.get(&node_id(0)), Some(&addr(9000)));
    }

    #[test]
    fn propose_add_node() {
        let mut node = RaftNode::new(node_id(0), addr(9000), 0);
        assert!(node.init_cluster());
        while node.next_action().is_some() {}

        let proposal_id = node.propose_add_node(node_id(1), addr(9001)).expect("ok");

        // Loop until the proposal is committed
        let mut found_commit = false;
        while let Some(action) = node.next_action() {
            if let Action::Commit {
                proposal_id: commit_proposal_id,
                ..
            } = action
                && commit_proposal_id == proposal_id
            {
                found_commit = true;
                break;
            }
        }
        assert!(found_commit, "Proposal should be committed");

        while node.next_action().is_some() {}

        // Check that the new node was added to cluster members
        assert_eq!(node.machine.node_addrs.len(), 2);
        assert_eq!(node.machine.node_addrs.get(&node_id(1)), Some(&addr(9001)));
    }

    fn append_storage_entry_action(json: &str) -> Action {
        let raw_json = nojson::RawJsonOwned::parse(json.to_string()).expect("invalid json");
        let value = JsonLineValue(std::sync::Arc::new(raw_json));
        Action::AppendStorageEntry(value)
    }

    fn set_leader_timeout_action() -> Action {
        Action::SetTimeout(raftbare::Role::Leader)
    }

    fn node_id(n: u64) -> raftbare::NodeId {
        raftbare::NodeId::new(n)
    }

    fn addr(port: u16) -> std::net::SocketAddr {
        std::net::SocketAddr::from(([127, 0, 0, 1], port))
    }
}
