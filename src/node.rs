pub type RecentCommands = std::collections::BTreeMap<raftbare::LogIndex, JsonLineValue>;

#[derive(Debug, Clone)]
enum Pending {
    Command(JsonLineValue),
    Query(ProposalId),
}

#[derive(Debug, Clone)]
pub struct RaftNode {
    pub inner: raftbare::Node,
    pub machine: RaftNodeStateMachine,
    pub action_queue: std::collections::VecDeque<Action>,
    pub recent_commands: RecentCommands,
    pub initialized: bool,
    pub generation: u64,
    pub local_command_seqno: u64,
    pub applied_index: raftbare::LogIndex,
    pub dirty_members: bool,
    pub pending_queries: std::collections::BTreeSet<(raftbare::LogPosition, ProposalId)>,
    pending_proposals: Vec<Pending>, // TODO: remove?
}

impl RaftNode {
    pub fn start(id: raftbare::NodeId) -> Self {
        let mut action_queue = std::collections::VecDeque::new();
        let generation = 0;
        let entry = StorageEntry::NodeGeneration(generation);
        let value = JsonLineValue::new_internal(entry);
        action_queue.push_back(Action::AppendStorageEntry(value));
        Self {
            inner: raftbare::Node::start(id),
            machine: RaftNodeStateMachine::default(),
            action_queue,
            recent_commands: std::collections::BTreeMap::new(),
            initialized: false,
            generation,
            local_command_seqno: 0,
            applied_index: raftbare::LogIndex::ZERO,
            dirty_members: false,
            pending_queries: std::collections::BTreeSet::new(),
            pending_proposals: Vec::new(),
        }
    }

    fn parse_snapshot_json(
        snapshot: &JsonLineValue,
    ) -> Result<
        (
            raftbare::LogPosition,
            raftbare::ClusterConfig,
            RaftNodeStateMachine,
        ),
        nojson::JsonParseError,
    > {
        let snapshot_json = snapshot.get();

        // Extract position
        let position_json = snapshot_json.to_member("position")?.required()?;
        let position_term =
            raftbare::Term::new(position_json.to_member("term")?.required()?.try_into()?);
        let position_index =
            raftbare::LogIndex::new(position_json.to_member("index")?.required()?.try_into()?);
        let position = raftbare::LogPosition {
            term: position_term,
            index: position_index,
        };

        // Extract config
        let config_json = snapshot_json.to_member("config")?.required()?;
        let mut config = raftbare::ClusterConfig::new();

        let voters_json = config_json.to_member("voters")?.required()?;
        for voter_id in voters_json.to_array()? {
            let id: u64 = voter_id.try_into()?;
            config.voters.insert(raftbare::NodeId::new(id));
        }

        let new_voters_json = config_json.to_member("new_voters")?.required()?;
        for voter_id in new_voters_json.to_array()? {
            let id: u64 = voter_id.try_into()?;
            config.new_voters.insert(raftbare::NodeId::new(id));
        }

        // Extract machine state
        let machine_json = snapshot_json.to_member("machine")?.required()?;
        let nodes_json = machine_json.to_member("nodes")?.required()?;
        let mut machine = RaftNodeStateMachine::default();

        for node_id in nodes_json.to_array()? {
            let id: u64 = node_id.try_into()?;
            machine.nodes.insert(raftbare::NodeId::new(id));
        }

        Ok((position, config, machine))
    }

    pub fn load<'a>(
        &mut self,
        entries: &'a [JsonLineValue],
    ) -> (bool, Option<nojson::RawJsonValue<'a, 'a>>) {
        struct LoadState<'a> {
            current_term: raftbare::Term,
            voted_for: Option<raftbare::NodeId>,
            config: raftbare::ClusterConfig,
            machine: RaftNodeStateMachine,
            log_entries: raftbare::LogEntries,
            recent_commands: RecentCommands,
            applied_index: raftbare::LogIndex,
            last_generation: u64,
            user_machine: Option<nojson::RawJsonValue<'a, 'a>>,
        }

        fn parse_log_entry(
            entry_value: nojson::RawJsonValue<'_, '_>,
            log_entries: &mut raftbare::LogEntries,
            recent_commands: &mut RecentCommands,
            config: &mut raftbare::ClusterConfig,
        ) -> Result<(), nojson::JsonParseError> {
            let entry_type: String = entry_value
                .to_member("type")?
                .required()?
                .to_unquoted_string_str()?
                .into_owned();

            let log_entry = match entry_type.as_str() {
                "Term" => {
                    let term = raftbare::Term::new(
                        entry_value.to_member("term")?.required()?.try_into()?,
                    );
                    raftbare::LogEntry::Term(term)
                }
                "ClusterConfig" => {
                    let mut cfg = raftbare::ClusterConfig::new();
                    cfg.voters = entry_value
                        .to_member("voters")?
                        .required()?
                        .to_array()?
                        .map(|v| {
                            let node_id: u64 = v.try_into()?;
                            Ok(raftbare::NodeId::new(node_id))
                        })
                        .collect::<Result<_, nojson::JsonParseError>>()?;

                    cfg.new_voters = entry_value
                        .to_member("new_voters")?
                        .required()?
                        .to_array()?
                        .map(|v| {
                            let node_id: u64 = v.try_into()?;
                            Ok(raftbare::NodeId::new(node_id))
                        })
                        .collect::<Result<_, nojson::JsonParseError>>()?;

                    *config = cfg.clone();
                    raftbare::LogEntry::ClusterConfig(cfg)
                }
                "Command" => {
                    let command_json = entry_value.to_member("value")?.required()?;
                    let command = JsonLineValue::new_internal(command_json);

                    let current_index = raftbare::LogIndex::new(
                        log_entries.prev_position().index.get() + log_entries.len() as u64 + 1,
                    );
                    recent_commands.insert(current_index, command);

                    raftbare::LogEntry::Command
                }
                _ => {
                    return Err(
                        entry_value.invalid(format!("unknown entry type: {entry_type}"))
                    );
                }
            };

            log_entries.push(log_entry);
            Ok(())
        }

        let result: Result<LoadState<'a>, nojson::JsonParseError> = (|| {
            let mut last_generation: u64 = 0;
            let mut current_term = raftbare::Term::new(0);
            let mut voted_for = None;
            let mut config = raftbare::ClusterConfig::new();
            let mut machine = RaftNodeStateMachine::default();
            let mut log_entries = raftbare::LogEntries::new(raftbare::LogPosition::ZERO);
            let mut recent_commands = std::collections::BTreeMap::new();
            let mut applied_index = raftbare::LogIndex::ZERO;
            let mut user_machine = None;
            let mut snapshot_loaded = false;

            for entry in entries {
                let ty = entry
                    .get()
                    .to_member("type")
                    .ok()
                    .and_then(|t| t.required().ok())
                    .and_then(|t| t.to_unquoted_string_str().ok());
                let Some(ty) = ty else {
                    continue;
                };

                match ty.as_ref() {
                    "InstallSnapshotRpc" => {
                        let (position, snap_config, snap_machine) =
                            Self::parse_snapshot_json(entry)?;
                        config = snap_config;
                        machine = snap_machine;
                        log_entries = raftbare::LogEntries::new(position);
                        recent_commands = std::collections::BTreeMap::new();
                        applied_index = position.index;
                        snapshot_loaded = true;

                        let node_state = entry.get().to_member("node_state")?.required()?;
                        let term = raftbare::Term::new(
                            node_state.to_member("term")?.required()?.try_into()?,
                        );
                        let voted_for_value: Option<u64> =
                            node_state.to_member("voted_for")?.try_into()?;
                        current_term = term;
                        voted_for = voted_for_value.map(raftbare::NodeId::new);

                        let user_machine_value =
                            entry.get().to_member("user_machine")?.required()?;
                        user_machine = Some(user_machine_value);

                        let entries_array =
                            entry.get().to_member("log_entries")?.required()?.to_array()?;
                        for entry_value in entries_array {
                            parse_log_entry(
                                entry_value,
                                &mut log_entries,
                                &mut recent_commands,
                                &mut config,
                            )?;
                        }
                    }
                    "NodeGeneration" => {
                        last_generation = entry.get_member("generation")?;
                    }
                    "Term" => {
                        current_term = raftbare::Term::new(entry.get_member("term")?);
                    }
                    "VotedFor" => {
                        let node_id: Option<u64> = entry.get().to_member("node_id")?.try_into()?;
                        voted_for = node_id.map(raftbare::NodeId::new);
                    }
                    "LogEntries" => {
                        let prev_term = raftbare::Term::new(entry.get_member("term")?);
                        let prev_index = raftbare::LogIndex::new(entry.get_member("index")?);
                        if !snapshot_loaded && log_entries.len() == 0 {
                            log_entries = raftbare::LogEntries::new(raftbare::LogPosition {
                                term: prev_term,
                                index: prev_index,
                            });
                        }

                        let entries_array =
                            entry.get().to_member("entries")?.required()?.to_array()?;
                        for entry_value in entries_array {
                            parse_log_entry(
                                entry_value,
                                &mut log_entries,
                                &mut recent_commands,
                                &mut config,
                            )?;
                        }
                    }
                    _ => {}
                }
            }

            Ok(LoadState {
                current_term,
                voted_for,
                config,
                machine,
                log_entries,
                recent_commands,
                applied_index,
                last_generation,
                user_machine,
            })
        })();

        let state = match result {
            Ok(state) => state,
            Err(_) => return (false, None),
        };

        let log = raftbare::Log::new(state.config.clone(), state.log_entries);
        self.inner = raftbare::Node::restart(self.inner.id(), state.current_term, state.voted_for, log);
        self.machine = state.machine;
        self.recent_commands = state.recent_commands;
        self.applied_index = state.applied_index;
        self.initialized = !state.config.voters.is_empty() || !state.config.new_voters.is_empty();
        self.dirty_members = false;
        self.pending_queries = std::collections::BTreeSet::new();
        self.pending_proposals = Vec::new();
        self.local_command_seqno = 0;

        self.generation = state.last_generation.saturating_add(1);
        let entry = StorageEntry::NodeGeneration(self.generation);
        let value = JsonLineValue::new_internal(entry);
        self.push_action(Action::AppendStorageEntry(value));

        (true, state.user_machine)
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
            generation: self.generation,
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

    pub fn handle_timeout(&mut self) {
        self.inner.handle_election_timeout();
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

        if self.applied_index < self.inner.commit_index() && self.inner.role().is_leader() {
            // Invokes heartbeat to notify the new commit position to followers as fast as possible
            //
            // This would affect the result of inner.actions_mut(). So call it before that (minor optimization).
            self.inner.heartbeat();
        }

        let mut after_commit_actions = Vec::new();
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
                raftbare::Action::InstallSnapshot(dst) => {
                    after_commit_actions.push(Action::SendSnapshot(dst));
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
        self.applied_index = self.inner.commit_index();

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

        for a in after_commit_actions {
            // TODO: Should use separate queue (set of dst?)
            self.push_action(a);
        }

        self.action_queue.pop_front()
    }

    pub fn create_snapshot<T: nojson::DisplayJson>(
        &self,
        applied_index: raftbare::LogIndex,
        machine: &T,
    ) -> Option<JsonLineValue> {
        let i = self.applied_index;
        if i != applied_index || i != self.inner.commit_index() {
            return None;
        }

        let (position, config) = self.inner.log().get_position_and_config(i).expect("bug");
        let json = nojson::object(|f| {
            f.member("type", "InstallSnapshotRpc")?;
            f.member("from", self.id().get())?;
            f.member("term", self.inner.current_term().get())?;
            // TODO: Add utility funs
            f.member(
                "position",
                nojson::object(|f| crate::conv::fmt_log_position_members(f, position)),
            )?;
            f.member(
                "node_state",
                nojson::object(|f| {
                    f.member("node_id", self.id().get())?;
                    f.member("term", self.inner.current_term().get())?;
                    f.member("voted_for", self.inner.voted_for().map(|id| id.get()))
                }),
            )?;
            f.member(
                "config",
                nojson::object(|f| {
                    f.member(
                        "voters",
                        nojson::array(|f| f.elements(config.voters.iter().map(|v| v.get()))),
                    )?;
                    f.member(
                        "new_voters",
                        nojson::array(|f| f.elements(config.new_voters.iter().map(|v| v.get()))),
                    )
                }),
            )?;
            f.member("user_machine", machine)?; // TODO: "user_machine" と "machine" の名前は改善する
            f.member(
                "machine",
                nojson::object(|f| {
                    f.member(
                        "nodes",
                        nojson::array(|f| f.elements(self.machine.nodes.iter().map(|n| n.get()))),
                    )
                }),
            )?;
            f.member(
                "log_entries",
                nojson::array(|f| {
                    for (pos, entry) in self.inner.log().entries().iter_with_positions() {
                        if pos.index <= applied_index {
                            continue;
                        }
                        f.element(nojson::object(|f| {
                            crate::conv::fmt_log_entry_members(
                                f,
                                pos,
                                &entry,
                                &self.recent_commands,
                            )
                        }))?;
                    }
                    Ok(())
                }),
            )
        });
        let value = JsonLineValue::new_internal(json);
        Some(value)
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
    generation: u64,
    local_seqno: u64,
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
            node_id: raftbare::NodeId::new(node_id),
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
    SendSnapshot(raftbare::NodeId),
    // TODO: NotifyEvent
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
                Ok(StorageEntry::Term(raftbare::Term::new(term)))
            }
            "VotedFor" => {
                let node_id: Option<u64> = value.to_member("node_id")?.try_into()?;
                Ok(StorageEntry::VotedFor(node_id.map(raftbare::NodeId::new)))
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
pub struct RaftNodeStateMachine {
    pub nodes: std::collections::BTreeSet<raftbare::NodeId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_cluster() {
        let mut node = RaftNode::start(node_id(0));
        assert!(node.init_cluster());
        assert!(!node.init_cluster());
        assert_eq!(
            node.next_action(),
            Some(append_storage_entry_action(
                r#"{"type":"NodeGeneration","generation":0}"#
            ))
        );
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

        assert_eq!(node.machine.nodes.len(), 1);
        assert!(node.machine.nodes.contains(&node_id(0)));
    }

    #[test]
    fn load_increments_generation() {
        let mut node = RaftNode::start(node_id(0));
        node.action_queue.clear();

        let entry = JsonLineValue::new_internal(StorageEntry::NodeGeneration(0));
        node.load(std::slice::from_ref(&entry));

        assert_eq!(node.generation, 1);
        assert_eq!(
            node.action_queue.pop_front(),
            Some(append_storage_entry_action(
                r#"{"type":"NodeGeneration","generation":1}"#
            ))
        );
    }

    #[test]
    fn load_uses_last_generation() {
        let mut node = RaftNode::start(node_id(0));
        node.action_queue.clear();

        let entry1 = JsonLineValue::new_internal(StorageEntry::NodeGeneration(2));
        let entry2 = JsonLineValue::new_internal(StorageEntry::NodeGeneration(5));
        let entries = [entry1, entry2];
        node.load(&entries);

        assert_eq!(node.generation, 6);
        assert_eq!(
            node.action_queue.pop_front(),
            Some(append_storage_entry_action(
                r#"{"type":"NodeGeneration","generation":6}"#
            ))
        );
    }

    #[test]
    fn create_snapshot_includes_node_state() {
        let mut node = RaftNode::start(node_id(0));
        assert!(node.init_cluster());
        while node.next_action().is_some() {}

        let applied_index = node.applied_index;
        let snapshot = node
            .create_snapshot(applied_index, &"user")
            .expect("snapshot should be created");

        let node_state = snapshot
            .get()
            .to_member("node_state")
            .expect("node_state")
            .required()
            .expect("node_state required");
        let node_id: u64 = node_state
            .to_member("node_id")
            .expect("node_id")
            .required()
            .expect("node_id required")
            .try_into()
            .unwrap();
        let term: u64 = node_state
            .to_member("term")
            .expect("term")
            .required()
            .expect("term required")
            .try_into()
            .unwrap();
        let voted_for: Option<u64> = node_state
            .to_member("voted_for")
            .expect("voted_for")
            .try_into()
            .unwrap();

        assert_eq!(node_id, node.id().get());
        assert_eq!(term, node.inner.current_term().get());
        assert_eq!(voted_for, node.inner.voted_for().map(|id| id.get()));
    }

    #[test]
    fn create_snapshot_includes_log_entries_suffix() {
        let mut node0 = RaftNode::start(node_id(0));
        let node1 = RaftNode::start(node_id(1));

        assert!(node0.init_cluster());
        while node0.next_action().is_some() {}

        node0.propose_add_node(node1.id());

        let mut nodes = [node0, node1];
        run_actions(&mut nodes);

        assert!(nodes[0].inner.role().is_leader());
        assert!(!nodes[1].inner.role().is_leader());

        let command = JsonLineValue::new_internal("snapshot_test");
        let _proposal_id = nodes[0].propose_command(command);

        while let Some(action) = nodes[0].next_action() {
            if let Action::BroadcastMessage(m) = action {
                assert!(nodes[1].handle_message(&m));
                break;
            }
        }

        let applied_index = nodes[1].applied_index;
        let snapshot = nodes[1]
            .create_snapshot(applied_index, &"user")
            .expect("snapshot should be created");

        let entries = snapshot
            .get()
            .to_member("log_entries")
            .expect("log_entries")
            .required()
            .expect("log_entries required")
            .to_array()
            .expect("log_entries array");

        let mut count = 0;
        for entry in entries {
            let ty: String = entry
                .to_member("type")
                .expect("type")
                .required()
                .expect("type required")
                .to_unquoted_string_str()
                .expect("type string")
                .into_owned();
            assert_eq!(ty, "Command");
            count += 1;
        }
        assert_eq!(count, 1);
    }

    #[test]
    fn propose_add_node() {
        let mut node = RaftNode::start(node_id(0));
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
                        Action::SendSnapshot(j) => {
                            let j = j.get() as usize;
                            let applied_index = nodes[i].applied_index;
                            let snapshot = nodes[i]
                                .create_snapshot(applied_index, &"user")
                                .expect("snapshot should be created");
                            let (ok, _) = nodes[j].load(std::slice::from_ref(&snapshot));
                            assert!(ok);
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
        let mut node0 = RaftNode::start(node_id(0));
        let node1 = RaftNode::start(node_id(1));

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
        let mut node0 = RaftNode::start(node_id(0));
        let node1 = RaftNode::start(node_id(1));

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
        let mut node0 = RaftNode::start(node_id(0));
        let node1 = RaftNode::start(node_id(1));

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
        let mut node0 = RaftNode::start(node_id(0));
        let node1 = RaftNode::start(node_id(1));

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

    #[test]
    fn strip_memory_log() {
        let mut node0 = RaftNode::start(node_id(0));

        // Create single node cluster
        assert!(node0.init_cluster());
        while node0.next_action().is_some() {}

        // Propose and commit some commands
        let command1 = JsonLineValue::new_internal("command1");
        let _proposal_id1 = node0.propose_command(command1);
        while node0.next_action().is_some() {}

        let command2 = JsonLineValue::new_internal("command2");
        let _proposal_id2 = node0.propose_command(command2);
        while node0.next_action().is_some() {}

        // Get the current commit index before stripping
        let commit_index = node0.inner.commit_index();
        assert!(commit_index.get() > 0);

        // Strip memory log until the commit index
        assert!(node0.strip_memory_log(commit_index));

        // Verify recent_commands was trimmed
        let remaining_commands: Vec<_> = node0.recent_commands.keys().cloned().collect();
        assert!(
            remaining_commands.is_empty()
                || remaining_commands.iter().all(|idx| *idx > commit_index)
        );

        // Add a new node and verify it can sync
        let node1 = RaftNode::start(node_id(1));
        node0.propose_add_node(node1.id());

        let mut nodes = [node0, node1];
        run_actions(&mut nodes);

        // Verify the new node synced and has the same cluster state
        assert_eq!(nodes[0].machine.nodes.len(), 2);
        assert_eq!(nodes[1].machine.nodes.len(), 2);
        assert!(nodes[1].machine.nodes.contains(&node_id(0)));
        assert!(nodes[1].machine.nodes.contains(&node_id(1)));
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
