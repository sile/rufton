pub type RecentCommands = std::collections::BTreeMap<raftbare::LogIndex, JsonLineValue>;

#[derive(Debug)]
enum Pending {
    Command(JsonLineValue),
    Query(ProposalId),
}

#[derive(Debug)]
pub struct RaftNode {
    pub inner: raftbare::Node,
    pub machine: RaftNodeStateMachine,
    pub action_queue: std::collections::VecDeque<Action>,
    pub recent_commands: RecentCommands,
    pub initialized: bool,
    pub instance_id: u64,
    pub local_command_seqno: u64,
    pub applied_index: raftbare::LogIndex,
    pub dirty_members: bool,
    pub pending_queries: std::collections::BTreeSet<(raftbare::LogPosition, ProposalId)>,
    pending_proposals: Vec<Pending>, // TODO: remove?
}

impl RaftNode {
    pub fn new(id: raftbare::NodeId, instance_id: u64) -> Self {
        Self {
            inner: raftbare::Node::start(id),
            machine: RaftNodeStateMachine::default(),
            action_queue: std::collections::VecDeque::new(),
            recent_commands: std::collections::BTreeMap::new(),
            initialized: false,
            instance_id,
            local_command_seqno: 0,
            applied_index: raftbare::LogIndex::ZERO,
            dirty_members: false,
            pending_queries: std::collections::BTreeSet::new(),
            pending_proposals: Vec::new(),
        }
    }

    pub fn id(&self) -> raftbare::NodeId {
        self.inner.id()
    }

    pub fn init_cluster(&mut self) -> bool {
        if self.initialized {
            return false;
        }

        let initial_members = [self.inner.id()];
        self.inner.create_cluster(&initial_members);
        self.initialized = true;

        self.propose_add_node(self.inner.id());

        true
    }

    pub fn recent_commands(&self) -> &RecentCommands {
        &self.recent_commands
    }

    pub fn strip_memory_log(&mut self, index: raftbare::LogIndex) -> bool {
        if index > self.applied_index {
            return false;
        }

        let Some((position, config)) = self.inner.log().get_position_and_config(index) else {
            return false;
        };

        // TODO: add note
        if !self
            .inner
            .handle_snapshot_installed(position, config.clone())
        {
            return false;
        }

        let i = raftbare::LogIndex::new(index.get() + 1);
        self.recent_commands = self.recent_commands.split_off(&i);
        true
    }

    // TODO: snapshot

    // NOTE: Propsals should be treated as timeout by clients in the following cases:
    // - Taking too long time (simple timeout)
    // - Commit application to the state machine managed the node received the proposal was skipped by snapshot
    // - Redirected proposal was discarded by any reasons (e.g. node down, redirect limit reached)
    // - Uninitialized cluster
    // - re-election

    fn propose(&mut self, command: Command) {
        let value = JsonLineValue::new_internal(command);
        self.propose_command_value(value);
    }

    // TODO: in redirected case, this serialization can be eliminated
    fn propose_command_value(&mut self, command: JsonLineValue) {
        if !self.initialized {
            return;
        }

        if !self.inner.role().is_leader() {
            if let Some(maybe_leader) = self.inner.voted_for()
                && maybe_leader != self.id()
            {
                self.push_action(Action::SendMessage(maybe_leader, command));
            } else {
                self.pending_proposals.push(Pending::Command(command));
            }
            return;
        }

        let position = self.inner.propose_command();
        self.recent_commands.insert(position.index, command);
    }

    pub fn propose_command(&mut self, command: JsonLineValue) -> ProposalId {
        let proposal_id = self.next_proposal_id();
        let command = Command::Apply {
            proposal_id,
            command,
        };
        self.propose(command);
        proposal_id
    }

    fn get_next_broadcast_position(&self) -> Option<raftbare::LogPosition> {
        if let Some(raftbare::Message::AppendEntriesCall { entries, .. }) =
            &self.inner.actions().broadcast_message
            && let Some((pos, _)) = entries.iter_with_positions().next()
        {
            Some(pos)
        } else {
            None
        }
    }

    pub fn propose_query(&mut self) -> ProposalId {
        let proposal_id = self.next_proposal_id();
        self.propose_query_inner(proposal_id);
        proposal_id
    }

    fn propose_query_inner(&mut self, proposal_id: ProposalId) {
        if self.inner.role().is_leader() {
            let command = Command::Query;
            let position = if let Some(position) = self.get_next_broadcast_position() {
                position
            } else {
                let value = JsonLineValue::new_internal(command);
                let position = self.inner.propose_command();
                self.recent_commands.insert(position.index, value);
                position
            };
            self.pending_queries.insert((position, proposal_id));
        } else if let Some(maybe_leader_id) = self.inner.voted_for()
            && maybe_leader_id != self.id()
        {
            let from = self.id();
            let query_message = QueryMessage::Redirect { from, proposal_id };
            let message = JsonLineValue::new_internal(query_message);
            self.push_action(Action::SendMessage(maybe_leader_id, message));
        } else {
            self.pending_proposals.push(Pending::Query(proposal_id));
        }
    }

    // TODO: refactor
    fn propose_query_for_redirect(&mut self, from: raftbare::NodeId, proposal_id: ProposalId) {
        if self.inner.role().is_leader() {
            let command = Command::Query;
            let position = if let Some(position) = self.get_next_broadcast_position() {
                position
            } else {
                let value = JsonLineValue::new_internal(command);
                let position = self.inner.propose_command();
                self.recent_commands.insert(position.index, value);
                position
            };
            let query_message = QueryMessage::Proposed {
                proposal_id,
                position,
            };
            let message = JsonLineValue::new_internal(query_message);
            self.push_action(Action::SendMessage(from, message));
        } else if let Some(maybe_leader_id) = self.inner.voted_for()
            && maybe_leader_id != self.id()
        {
            let query_message = QueryMessage::Redirect { from, proposal_id };
            let message = JsonLineValue::new_internal(query_message);
            self.push_action(Action::SendMessage(maybe_leader_id, message));
        }
    }

    fn next_proposal_id(&mut self) -> ProposalId {
        let proposal_id = ProposalId {
            node_id: self.inner.id(),
            instance_id: self.instance_id,
            local_seqno: self.local_command_seqno,
        };
        self.local_command_seqno += 1;
        proposal_id
    }

    pub fn propose_add_node(&mut self, id: raftbare::NodeId) -> ProposalId {
        let proposal_id = self.next_proposal_id();
        let command = Command::AddNode { proposal_id, id };
        self.propose(command);
        proposal_id
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

        // Get voters from state machine nodes
        let machine_voters = &self.machine.nodes;

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

    pub fn handle_message(&mut self, message_value: &JsonLineValue) -> bool {
        // TODO: use match ty {}
        let Ok(message) = crate::conv::json_to_message(message_value.get()) else {
            if let Ok(c) = Command::try_from(message_value.get()) {
                // This is a redirected command
                //
                // TODO: Add redirect count limit
                self.propose(c);
                return true;
            } else if let Ok(m) = QueryMessage::try_from(message_value.get()) {
                match m {
                    QueryMessage::Redirect { from, proposal_id } => {
                        self.propose_query_for_redirect(from, proposal_id);
                    }
                    QueryMessage::Proposed {
                        proposal_id,
                        position,
                    } => {
                        self.pending_queries.insert((position, proposal_id));
                    }
                }
                return true;
            } else {
                return false;
            }
        };
        if !self.initialized {
            self.initialized = true;
        }
        self.inner.handle_message(message.clone());

        let command_values = crate::conv::get_command_values(message_value.get(), &message);
        for (pos, command) in command_values.into_iter().flatten() {
            if self.inner.log().entries().contains(pos) {
                self.recent_commands.insert(pos.index, command);
            }
        }

        true
    }

    pub fn next_action(&mut self) -> Option<Action> {
        if !self.initialized {
            return None;
        }

        self.maybe_sync_raft_members();

        while let Some(inner_action) = self.inner.actions_mut().next() {
            match inner_action {
                raftbare::Action::SetElectionTimeout => {
                    let role = self.inner.role();
                    for p in std::mem::take(&mut self.pending_proposals)
                        .into_iter()
                        .filter(|_| role.is_leader())
                    {
                        match p {
                            Pending::Command(command) => self.propose_command_value(command),
                            Pending::Query(id) => self.propose_query_inner(id),
                        }
                    }

                    self.push_action(Action::SetTimeout(role));
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
                raftbare::Action::SendMessage(node_id, message) => {
                    let message = JsonLineValue::new_internal(nojson::json(|f| {
                        crate::conv::fmt_message(f, &message, &self.recent_commands)
                    }));
                    self.push_action(Action::SendMessage(node_id, message));
                }
                raftbare::Action::InstallSnapshot(_) => {
                    todo!("InstallSnapshot action")
                }
            }
        }

        for i in self.applied_index.get()..self.inner.commit_index().get() {
            let index = raftbare::LogIndex::new(i + 1);

            let Some(command) = self.recent_commands.get(&index).cloned() else {
                continue;
            };

            let proposal_id = command.get_optional_member("proposal_id").expect("bug");

            let command = match command.get_member::<String>("type").expect("bug").as_str() {
                "AddNode" => {
                    self.handle_add_node(&command).expect("bug");
                    None
                }
                "Apply" => Some(command),
                "Query" => None,
                ty => panic!("bug: {ty}"),
            };

            self.push_action(Action::Commit {
                index,
                proposal_id,
                command,
            });
        }

        while let Some(&(position, proposal_id)) = self.pending_queries.first() {
            let status = self.inner.get_commit_status(position);
            match status {
                raftbare::CommitStatus::InProgress => break,
                raftbare::CommitStatus::Rejected | raftbare::CommitStatus::Unknown => {
                    self.pending_queries.pop_first();
                }
                raftbare::CommitStatus::Committed => {
                    self.pending_queries.pop_first();
                    self.push_action(Action::Query { proposal_id });
                }
            }
        }

        if self.applied_index < self.inner.commit_index() {
            self.applied_index = self.inner.commit_index();

            if self.inner.role().is_leader() {
                // Invokes heartbeat to notify the new commit position to followers as fast as possible
                self.inner.heartbeat();
            }
        }

        self.action_queue.pop_front()
    }

    fn handle_add_node(&mut self, command: &JsonLineValue) -> Result<(), nojson::JsonParseError> {
        let id = raftbare::NodeId::new(command.get_member("id")?);

        if self.machine.nodes.contains(&id) {
            return Ok(());
        }

        self.machine.nodes.insert(id);
        self.dirty_members = true;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

#[derive(Clone, PartialEq, Eq)]
pub struct JsonLineValue(std::sync::Arc<nojson::RawJsonOwned>);

impl JsonLineValue {
    pub(crate) fn new_internal<T: nojson::DisplayJson>(v: T) -> Self {
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

    fn get_optional_member<'a, T>(&'a self, name: &str) -> Result<Option<T>, nojson::JsonParseError>
    where
        T: TryFrom<nojson::RawJsonValue<'a, 'a>, Error = nojson::JsonParseError>,
    {
        self.get().to_member(name)?.try_into()
    }
}

impl nojson::DisplayJson for JsonLineValue {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        writeln!(f.inner_mut(), "{}", self.0.text())
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
        from: raftbare::NodeId,
        proposal_id: ProposalId,
    },
    Proposed {
        proposal_id: ProposalId,
        position: raftbare::LogPosition,
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
                    from: raftbare::NodeId::new(from),
                    proposal_id,
                })
            }
            "Proposed" => {
                let proposal_id = value.to_member("proposal_id")?.required()?.try_into()?;
                let term = raftbare::Term::new(value.to_member("term")?.required()?.try_into()?);
                let index =
                    raftbare::LogIndex::new(value.to_member("index")?.required()?.try_into()?);
                Ok(QueryMessage::Proposed {
                    proposal_id,
                    position: raftbare::LogPosition { term, index },
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
        id: raftbare::NodeId,
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
                    id: raftbare::NodeId::new(id),
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
    SetTimeout(raftbare::Role),
    AppendStorageEntry(JsonLineValue),
    BroadcastMessage(JsonLineValue),
    SendMessage(raftbare::NodeId, JsonLineValue),
    Commit {
        proposal_id: Option<ProposalId>,
        index: raftbare::LogIndex,
        command: Option<JsonLineValue>,
    },
    Query {
        proposal_id: ProposalId,
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
    pub nodes: std::collections::BTreeSet<raftbare::NodeId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_cluster() {
        let mut node = RaftNode::new(node_id(0), 0);
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
        assert_eq!(node.next_action(), Some(set_leader_timeout_action()));
        assert_eq!(node.next_action(), None);

        assert_eq!(node.machine.nodes.len(), 1);
        assert!(node.machine.nodes.contains(&node_id(0)));
    }

    #[test]
    fn propose_add_node() {
        let mut node = RaftNode::new(node_id(0), 0);
        assert!(node.init_cluster());
        while node.next_action().is_some() {}

        let proposal_id = node.propose_add_node(node_id(1));

        let mut found_commit = false;
        while let Some(action) = node.next_action() {
            if let Action::Commit {
                proposal_id: commit_proposal_id,
                ..
            } = action
                && commit_proposal_id == Some(proposal_id)
            {
                found_commit = true;
                break;
            }
        }
        assert!(found_commit, "Proposal should be committed");

        while node.next_action().is_some() {}

        assert_eq!(node.machine.nodes.len(), 2);
        assert!(node.machine.nodes.contains(&node_id(1)));
    }

    fn run_actions(nodes: &mut [RaftNode]) -> Vec<(raftbare::NodeId, Action)> {
        let mut actions = Vec::new();
        for _ in 0..1000 {
            let mut did_something = false;

            for i in 0..nodes.len() {
                while let Some(action) = nodes[i].next_action() {
                    did_something = true;
                    actions.push((nodes[i].id(), action.clone()));
                    match action {
                        Action::BroadcastMessage(m) => {
                            for j in 0..nodes.len() {
                                if i != j {
                                    assert!(nodes[j].handle_message(&m));
                                }
                            }
                        }
                        Action::SendMessage(j, m) => {
                            let j = j.get() as usize;
                            assert!(nodes[j].handle_message(&m));
                        }
                        _ => {}
                    }
                }
            }

            if !did_something {
                return actions;
            }
        }
        panic!()
    }

    #[test]
    fn two_node_broadcast_message_handling() {
        let mut node0 = RaftNode::new(node_id(0), 0);
        let node1 = RaftNode::new(node_id(1), 1);

        assert!(node0.init_cluster());
        while node0.next_action().is_some() {}

        node0.propose_add_node(node1.id());

        let mut nodes = [node0, node1];
        run_actions(&mut nodes);

        assert_eq!(nodes[0].machine.nodes.len(), 2);
        assert_eq!(nodes[1].machine.nodes.len(), 2);
    }

    #[test]
    fn propose_command_to_non_leader_node() {
        let mut node0 = RaftNode::new(node_id(0), 0);
        let node1 = RaftNode::new(node_id(1), 1);

        assert!(node0.init_cluster());
        while node0.next_action().is_some() {}

        node0.propose_add_node(node1.id());

        let mut nodes = [node0, node1];
        run_actions(&mut nodes);

        // node0 is the leader, node1 is a follower
        assert!(nodes[0].inner.role().is_leader());
        assert!(!nodes[1].inner.role().is_leader());

        // Try to propose a command to the non-leader (node1)
        let command = JsonLineValue::new_internal("test_command");
        let proposal_id = nodes[1].propose_command(command);

        let actions = run_actions(&mut nodes);
        // Check that actions contain a Commit with the matching proposal_id
        let found_commit = actions.iter().any(|(node_id, action)| {
            dbg!(node_id, action);
            if let Action::Commit {
                proposal_id: id, ..
            } = action
                && *id == Some(proposal_id)
            {
                dbg!(node_id, action);
                *node_id == nodes[1].id()
            } else {
                false
            }
        });
        assert!(
            found_commit,
            "Commit action with matching proposal_id should be in actions"
        );
    }

    #[test]
    fn propose_query() {
        let mut node0 = RaftNode::new(node_id(0), 0);
        let node1 = RaftNode::new(node_id(1), 1);

        assert!(node0.init_cluster());
        while node0.next_action().is_some() {}

        node0.propose_add_node(node1.id());

        let mut nodes = [node0, node1];
        run_actions(&mut nodes);

        // node0 is the leader, node1 is a follower
        assert!(nodes[0].inner.role().is_leader());
        assert!(!nodes[1].inner.role().is_leader());

        // Propose a query on the leader
        let query_proposal_id = nodes[0].propose_query();

        let actions = run_actions(&mut nodes);

        // Check that a Query action was generated with the matching proposal_id
        let found_query = actions.iter().any(|(node_id, action)| {
            if let Action::Query { proposal_id } = action {
                *node_id == nodes[0].id() && *proposal_id == query_proposal_id
            } else {
                false
            }
        });
        assert!(
            found_query,
            "Query action with matching proposal_id should be returned by leader"
        );
    }

    #[test]
    fn propose_query_on_non_leader_node() {
        let mut node0 = RaftNode::new(node_id(0), 0);
        let node1 = RaftNode::new(node_id(1), 1);

        assert!(node0.init_cluster());
        while node0.next_action().is_some() {}

        node0.propose_add_node(node1.id());

        let mut nodes = [node0, node1];
        run_actions(&mut nodes);

        // node0 is the leader, node1 is a follower
        assert!(nodes[0].inner.role().is_leader());
        assert!(!nodes[1].inner.role().is_leader());

        // Propose a query on the non-leader (node1)
        let query_proposal_id = nodes[1].propose_query();

        let actions = run_actions(&mut nodes);

        // Check that the query was redirected to the leader and eventually resolved
        let found_query = actions.iter().any(|(node_id, action)| {
            if let Action::Query { proposal_id } = action {
                *node_id == nodes[1].id() && *proposal_id == query_proposal_id
            } else {
                false
            }
        });
        assert!(
            found_query,
            "Query should be redirected to leader and resolved"
        );
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
}
