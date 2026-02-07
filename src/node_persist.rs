use crate::node_core::Node;
use crate::node_types::{Action, JsonLineValue, NodeStateMachine, RecentCommands, StorageEntry};

impl Node {
    fn parse_snapshot_json(
        snapshot: &JsonLineValue,
    ) -> Result<
        (noraft::LogPosition, noraft::ClusterConfig, NodeStateMachine),
        nojson::JsonParseError,
    > {
        let snapshot_json = snapshot.get();

        // Extract position
        let position_json = snapshot_json.to_member("position")?.required()?;
        let position_term =
            noraft::Term::new(position_json.to_member("term")?.required()?.try_into()?);
        let position_index =
            noraft::LogIndex::new(position_json.to_member("index")?.required()?.try_into()?);
        let position = noraft::LogPosition {
            term: position_term,
            index: position_index,
        };

        // Extract config
        let config_json = snapshot_json.to_member("config")?.required()?;
        let mut config = noraft::ClusterConfig::new();

        let voters_json = config_json.to_member("voters")?.required()?;
        for voter_id in voters_json.to_array()? {
            let id: u64 = voter_id.try_into()?;
            config.voters.insert(noraft::NodeId::new(id));
        }

        let new_voters_json = config_json.to_member("new_voters")?.required()?;
        for voter_id in new_voters_json.to_array()? {
            let id: u64 = voter_id.try_into()?;
            config.new_voters.insert(noraft::NodeId::new(id));
        }

        // Extract machine state
        let machine_json = snapshot_json.to_member("machine")?.required()?;
        let nodes_json = machine_json.to_member("nodes")?.required()?;
        let mut machine = NodeStateMachine::default();

        for node_id in nodes_json.to_array()? {
            let id: u64 = node_id.try_into()?;
            machine.nodes.insert(noraft::NodeId::new(id));
        }

        Ok((position, config, machine))
    }

    pub fn load<'a>(
        &mut self,
        entries: &'a [JsonLineValue],
    ) -> (bool, Option<nojson::RawJsonValue<'a, 'a>>) {
        struct LoadState<'a> {
            current_term: noraft::Term,
            voted_for: Option<noraft::NodeId>,
            config: noraft::ClusterConfig,
            machine: NodeStateMachine,
            log_entries: noraft::LogEntries,
            recent_commands: RecentCommands,
            applied_index: noraft::LogIndex,
            last_generation: u64,
            user_machine: Option<nojson::RawJsonValue<'a, 'a>>,
        }

        fn parse_log_entry(
            entry_value: nojson::RawJsonValue<'_, '_>,
            log_entries: &mut noraft::LogEntries,
            recent_commands: &mut RecentCommands,
            config: &mut noraft::ClusterConfig,
        ) -> Result<(), nojson::JsonParseError> {
            let entry_type: String = entry_value
                .to_member("type")?
                .required()?
                .to_unquoted_string_str()?
                .into_owned();

            let log_entry = match entry_type.as_str() {
                "Term" => {
                    let term =
                        noraft::Term::new(entry_value.to_member("term")?.required()?.try_into()?);
                    noraft::LogEntry::Term(term)
                }
                "ClusterConfig" => {
                    let mut cfg = noraft::ClusterConfig::new();
                    cfg.voters = entry_value
                        .to_member("voters")?
                        .required()?
                        .to_array()?
                        .map(|v| {
                            let node_id: u64 = v.try_into()?;
                            Ok(noraft::NodeId::new(node_id))
                        })
                        .collect::<Result<_, nojson::JsonParseError>>()?;

                    cfg.new_voters = entry_value
                        .to_member("new_voters")?
                        .required()?
                        .to_array()?
                        .map(|v| {
                            let node_id: u64 = v.try_into()?;
                            Ok(noraft::NodeId::new(node_id))
                        })
                        .collect::<Result<_, nojson::JsonParseError>>()?;

                    *config = cfg.clone();
                    noraft::LogEntry::ClusterConfig(cfg)
                }
                "Command" => {
                    let command_json = entry_value.to_member("value")?.required()?;
                    let command = JsonLineValue::new_internal(command_json);

                    let current_index = noraft::LogIndex::new(
                        log_entries.prev_position().index.get() + log_entries.len() as u64 + 1,
                    );
                    recent_commands.insert(current_index, command);

                    noraft::LogEntry::Command
                }
                _ => {
                    return Err(entry_value.invalid(format!("unknown entry type: {entry_type}")));
                }
            };

            log_entries.push(log_entry);
            Ok(())
        }

        let result: Result<LoadState<'a>, nojson::JsonParseError> = (|| {
            let mut last_generation: u64 = 0;
            let mut current_term = noraft::Term::new(0);
            let mut voted_for = None;
            let mut config = noraft::ClusterConfig::new();
            let mut machine = NodeStateMachine::default();
            let mut log_entries = noraft::LogEntries::new(noraft::LogPosition::ZERO);
            let mut recent_commands = std::collections::BTreeMap::new();
            let mut applied_index = noraft::LogIndex::ZERO;
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
                        log_entries = noraft::LogEntries::new(position);
                        recent_commands = std::collections::BTreeMap::new();
                        applied_index = position.index;
                        snapshot_loaded = true;

                        let node_state = entry.get().to_member("node_state")?.required()?;
                        let term = noraft::Term::new(
                            node_state.to_member("term")?.required()?.try_into()?,
                        );
                        let voted_for_value: Option<u64> =
                            node_state.to_member("voted_for")?.try_into()?;
                        current_term = term;
                        voted_for = voted_for_value.map(noraft::NodeId::new);

                        let user_machine_value =
                            entry.get().to_member("user_machine")?.required()?;
                        user_machine = Some(user_machine_value);

                        let entries_array = entry
                            .get()
                            .to_member("log_entries")?
                            .required()?
                            .to_array()?;
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
                        current_term = noraft::Term::new(entry.get_member("term")?);
                    }
                    "VotedFor" => {
                        let node_id: Option<u64> = entry.get().to_member("node_id")?.try_into()?;
                        voted_for = node_id.map(noraft::NodeId::new);
                    }
                    "LogEntries" => {
                        let prev_term = noraft::Term::new(entry.get_member("term")?);
                        let prev_index = noraft::LogIndex::new(entry.get_member("index")?);
                        if !snapshot_loaded && log_entries.is_empty() {
                            log_entries = noraft::LogEntries::new(noraft::LogPosition {
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

        let log = noraft::Log::new(state.config.clone(), state.log_entries);
        let new_generation = state.last_generation.saturating_add(1);
        let generation = noraft::NodeGeneration::new(new_generation);
        self.inner = noraft::Node::restart(
            self.inner.id(),
            generation,
            state.current_term,
            state.voted_for,
            log,
        );
        self.machine = state.machine;
        self.recent_commands = state.recent_commands;
        self.applied_index = state.applied_index;
        self.initialized = !state.config.voters.is_empty() || !state.config.new_voters.is_empty();
        self.dirty_members = false;
        self.pending_queries = std::collections::BTreeSet::new();
        self.local_command_seqno = 0;

        let entry = StorageEntry::NodeGeneration(new_generation);
        let value = JsonLineValue::new_internal(entry);
        self.push_action(Action::AppendStorageEntry(value));

        (true, state.user_machine)
    }

    pub fn create_snapshot<T: nojson::DisplayJson>(
        &self,
        applied_index: noraft::LogIndex,
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
}
