use crate::node_types::{
    Action, Command, JsonLineValue, ProposalId, QueryMessage, RecentCommands, StorageEntry,
};

#[derive(Debug, Clone)]
pub struct Node {
    pub inner: noraft::Node,
    pub action_queue: std::collections::VecDeque<Action>,
    pub recent_commands: RecentCommands,
    pub initialized: bool,
    pub local_command_seqno: u64,
    pub applied_index: noraft::LogIndex,
    pub pending_queries: std::collections::BTreeMap<
        (noraft::LogPosition, ProposalId),
        JsonLineValue,
    >,
}

impl Node {
    pub fn start(id: noraft::NodeId) -> Self {
        let mut action_queue = std::collections::VecDeque::new();
        let inner = noraft::Node::start(id);
        let entry = StorageEntry::NodeGeneration(inner.generation().get());
        let value = JsonLineValue::new_internal(entry);
        action_queue.push_back(Action::AppendStorageEntry(value));
        Self {
            inner,
            action_queue,
            recent_commands: std::collections::BTreeMap::new(),
            initialized: false,
            local_command_seqno: 0,
            applied_index: noraft::LogIndex::ZERO,
            pending_queries: std::collections::BTreeMap::new(),
        }
    }

    pub fn id(&self) -> noraft::NodeId {
        self.inner.id()
    }

    pub fn members(&self) -> impl Iterator<Item = noraft::NodeId> {
        self.inner.config().unique_nodes()
    }

    pub fn peers(&self) -> impl Iterator<Item = noraft::NodeId> {
        self.members().filter(|id| *id != self.id())
    }

    pub fn init_cluster(&mut self, members: &[noraft::NodeId]) -> bool {
        if self.initialized {
            return false;
        }
        if !members.contains(&self.inner.id()) {
            return false;
        }

        self.inner.create_cluster(members);
        self.initialized = true;

        true
    }

    pub fn recent_commands(&self) -> &RecentCommands {
        &self.recent_commands
    }

    pub fn strip_memory_log(&mut self, index: noraft::LogIndex) -> bool {
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

        let i = noraft::LogIndex::new(index.get() + 1);
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
            if let Some(maybe_leader) = self.leader_id() {
                self.push_action(Action::SendMessage(maybe_leader, command));
            } else {
                // TODO: add missing proposal event
            }
            return;
        }

        let position = self.inner.propose_command();
        self.recent_commands.insert(position.index, command);
    }

    pub fn propose_command(&mut self, request: JsonLineValue) -> ProposalId {
        let proposal_id = self.next_proposal_id();
        let command = Command::Apply {
            proposal_id,
            command: request,
        };
        self.propose(command);
        proposal_id
    }

    fn get_next_broadcast_position(&self) -> Option<noraft::LogPosition> {
        if let Some(noraft::Message::AppendEntriesCall { entries, .. }) =
            &self.inner.actions().broadcast_message
            && let Some((pos, _)) = entries.iter_with_positions().next()
        {
            Some(pos)
        } else {
            None
        }
    }

    fn leader_query_position(&mut self) -> noraft::LogPosition {
        if let Some(position) = self.get_next_broadcast_position() {
            return position;
        }

        let value = JsonLineValue::new_internal(Command::Query);
        let position = self.inner.propose_command();
        self.recent_commands.insert(position.index, value);
        position
    }

    pub fn propose_query(&mut self, request: JsonLineValue) -> ProposalId {
        let proposal_id = self.next_proposal_id();
        self.propose_query_inner(proposal_id, request);
        proposal_id
    }

    fn propose_query_inner(&mut self, proposal_id: ProposalId, request: JsonLineValue) {
        if self.inner.role().is_leader() {
            let position = self.leader_query_position();
            self.pending_queries
                .insert((position, proposal_id), request);
        } else if let Some(maybe_leader_id) = self.leader_id() {
            let from = self.id();
            let query_message = QueryMessage::Redirect {
                from,
                proposal_id,
                request,
            };
            let message = JsonLineValue::new_internal(query_message);
            self.push_action(Action::SendMessage(maybe_leader_id, message));
        } else {
            // TODO: add missing proposal event
        }
    }

    // TODO: refactor
    fn propose_query_for_redirect(
        &mut self,
        from: noraft::NodeId,
        proposal_id: ProposalId,
        request: JsonLineValue,
    ) {
        if self.inner.role().is_leader() {
            let position = self.leader_query_position();
            let query_message = QueryMessage::Proposed {
                proposal_id,
                position,
                request,
            };
            let message = JsonLineValue::new_internal(query_message);
            self.push_action(Action::SendMessage(from, message));
        } else if let Some(maybe_leader_id) = self.leader_id() {
            let query_message = QueryMessage::Redirect {
                from,
                proposal_id,
                request,
            };
            let message = JsonLineValue::new_internal(query_message);
            self.push_action(Action::SendMessage(maybe_leader_id, message));
        }
    }

    fn next_proposal_id(&mut self) -> ProposalId {
        let proposal_id = ProposalId::new(
            self.inner.id(),
            self.inner.generation().get(),
            self.local_command_seqno,
        );
        self.local_command_seqno += 1;
        proposal_id
    }

    fn leader_id(&self) -> Option<noraft::NodeId> {
        let leader = self.inner.voted_for()?;
        (leader != self.id()).then_some(leader)
    }

    pub(crate) fn push_action(&mut self, action: Action) {
        self.action_queue.push_back(action);
    }

    pub fn handle_timeout(&mut self) {
        self.inner.handle_election_timeout();
    }

    // TODO: take JsonRpcRequest
    pub fn handle_message(&mut self, message_value: &JsonLineValue) -> bool {
        let Ok(message) = crate::conv::json_to_message(message_value.get()) else {
            if self.handle_redirected_command(message_value) {
                return true;
            }
            if self.handle_query_message(message_value) {
                return true;
            }
            return false;
        };

        self.handle_raft_message(message_value, message);
        true
    }

    fn handle_raft_message(&mut self, message_value: &JsonLineValue, message: noraft::Message) {
        self.initialize_if_needed();
        self.inner.handle_message(&message);

        let command_values = crate::conv::get_command_values(message_value.get(), &message);
        for (pos, command) in command_values.into_iter().flatten() {
            if self.inner.log().entries().contains(pos) {
                self.recent_commands.insert(pos.index, command);
            }
        }
    }

    fn handle_redirected_command(&mut self, message_value: &JsonLineValue) -> bool {
        if let Ok(command) = Command::try_from(message_value.get()) {
            // This is a redirected command
            //
            // TODO: Add redirect count limit
            self.propose(command);
            true
        } else {
            false
        }
    }

    fn handle_query_message(&mut self, message_value: &JsonLineValue) -> bool {
        if let Ok(message) = QueryMessage::try_from(message_value.get()) {
            match message {
                QueryMessage::Redirect {
                    from,
                    proposal_id,
                    request,
                } => {
                    self.propose_query_for_redirect(from, proposal_id, request);
                }
                QueryMessage::Proposed {
                    proposal_id,
                    position,
                    request,
                } => {
                    self.pending_queries
                        .insert((position, proposal_id), request);
                }
            }
            true
        } else {
            false
        }
    }

    fn initialize_if_needed(&mut self) {
        if !self.initialized {
            self.initialized = true;
        }
    }

    pub fn next_action(&mut self) -> Option<Action> {
        if !self.initialized {
            return None;
        }

        self.maybe_heartbeat_on_leader();

        let mut after_commit_actions = Vec::new();
        self.process_inner_actions(&mut after_commit_actions);
        self.emit_commit_actions();
        self.emit_query_actions();
        self.enqueue_after_commit_actions(after_commit_actions);

        self.action_queue.pop_front()
    }

    fn maybe_heartbeat_on_leader(&mut self) {
        if self.applied_index < self.inner.commit_index() && self.inner.role().is_leader() {
            // Invokes heartbeat to notify the new commit position to followers as fast as possible
            //
            // This would affect the result of inner.actions_mut(). So call it before that (minor optimization).
            self.inner.heartbeat();
        }
    }

    fn process_inner_actions(&mut self, after_commit_actions: &mut Vec<Action>) {
        // TODO: donto use acitons_mut() (direct fields hanlding instead)
        while let Some(inner_action) = self.inner.actions_mut().next() {
            match inner_action {
                noraft::Action::SetElectionTimeout => self.handle_set_election_timeout(),
                noraft::Action::SaveCurrentTerm => {
                    let term = self.inner.current_term();
                    self.enqueue_storage_entry(StorageEntry::Term(term));
                }
                noraft::Action::SaveVotedFor => {
                    let voted_for = self.inner.voted_for();
                    self.enqueue_storage_entry(StorageEntry::VotedFor(voted_for));
                }
                noraft::Action::BroadcastMessage(message) => {
                    let value = self.encode_message(&message);
                    self.push_action(Action::BroadcastMessage(value));
                }
                noraft::Action::AppendLogEntries(entries) => {
                    let value = self.encode_log_entries(&entries);
                    self.push_action(Action::AppendStorageEntry(value));
                }
                noraft::Action::SendMessage(node_id, message) => {
                    let message = self.encode_message(&message);
                    self.push_action(Action::SendMessage(node_id, message));
                }
                noraft::Action::InstallSnapshot(dst) => {
                    after_commit_actions.push(Action::SendSnapshot(dst));
                }
            }
        }
    }

    fn handle_set_election_timeout(&mut self) {
        let role = self.inner.role();
        self.push_action(Action::SetTimeout(role));
    }

    fn emit_commit_actions(&mut self) {
        for i in self.applied_index.get()..self.inner.commit_index().get() {
            let index = noraft::LogIndex::new(i + 1);

            let Some(command) = self.recent_commands.get(&index).cloned() else {
                continue;
            };

            let proposal_id = command.get_optional_member("proposal_id").expect("bug");

            let request = match command.get_member::<String>("type").expect("bug").as_str() {
                "Apply" => {
                    let request_value = command
                        .get()
                        .to_member("command")
                        .and_then(|value| value.required())
                        .expect("bug");
                    JsonLineValue::new_internal(request_value)
                }
                "Query" => continue,
                ty => panic!("bug: {ty}"),
            };

            self.push_action(Action::Apply {
                index,
                proposal_id,
                request,
            });
        }
        self.applied_index = self.inner.commit_index();
    }

    fn emit_query_actions(&mut self) {
        while let Some((&(position, _), _)) = self.pending_queries.first_key_value() {
            let status = self.inner.get_commit_status(position);
            match status {
                noraft::CommitStatus::InProgress => break,
                noraft::CommitStatus::Rejected | noraft::CommitStatus::Unknown => {
                    let keys: Vec<_> = self
                        .pending_queries
                        .keys()
                        .take_while(|(pos, _)| *pos == position)
                        .cloned()
                        .collect();
                    for key in keys {
                        self.pending_queries.remove(&key);
                    }
                }
                noraft::CommitStatus::Committed => {
                    let keys: Vec<_> = self
                        .pending_queries
                        .keys()
                        .take_while(|(pos, _)| *pos == position)
                        .cloned()
                        .collect();
                    for (position, proposal_id) in keys {
                        let request = self
                            .pending_queries
                            .remove(&(position, proposal_id))
                            .expect("pending_queries should have entry");
                        self.push_action(Action::Apply {
                            proposal_id: Some(proposal_id),
                            index: position.index,
                            request,
                        });
                    }
                }
            }
        }
    }

    fn enqueue_after_commit_actions(&mut self, actions: Vec<Action>) {
        for action in actions {
            // TODO: Should use separate queue (set of dst?)
            self.push_action(action);
        }
    }

    fn enqueue_storage_entry(&mut self, entry: StorageEntry) {
        let value = JsonLineValue::new_internal(entry);
        self.push_action(Action::AppendStorageEntry(value));
    }

    fn encode_message(&self, message: &noraft::Message) -> JsonLineValue {
        JsonLineValue::new_internal(nojson::json(|f| {
            crate::conv::fmt_message(f, message, &self.recent_commands)
        }))
    }

    fn encode_log_entries(&self, entries: &noraft::LogEntries) -> JsonLineValue {
        JsonLineValue::new_internal(nojson::json(|f| {
            crate::conv::fmt_log_entries(f, entries, &self.recent_commands)
        }))
    }
}
