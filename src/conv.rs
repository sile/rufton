pub fn fmt_log_position_members(
    f: &mut nojson::JsonObjectFormatter<'_, '_, '_>,
    position: noraft::LogPosition,
) -> std::fmt::Result {
    f.member("term", position.term.get())?;
    f.member("index", position.index.get())
}

pub fn fmt_log_entry_members(
    f: &mut nojson::JsonObjectFormatter<'_, '_, '_>,
    pos: noraft::LogPosition,
    entry: &noraft::LogEntry,
    commands: &crate::node::RecentCommands,
) -> std::fmt::Result {
    match entry {
        noraft::LogEntry::Term(term) => {
            f.member("type", "Term")?;
            f.member("term", term.get())
        }
        noraft::LogEntry::ClusterConfig(config) => {
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
        noraft::LogEntry::Command => {
            f.member("type", "Command")?;
            let command = commands.get(&pos.index).expect("bug");
            f.member("value", command)
        }
    }
}

pub fn fmt_log_entries(
    f: &mut nojson::JsonFormatter<'_, '_>,
    entries: &noraft::LogEntries,
    commands: &crate::node::RecentCommands,
) -> std::fmt::Result {
    f.object(|f| {
        f.member("type", "LogEntries")?;
        fmt_log_position_members(f, entries.prev_position())?;
        f.member(
            "entries",
            nojson::array(|f| {
                for (pos, entry) in entries.iter_with_positions() {
                    f.element(nojson::object(|f| {
                        fmt_log_entry_members(f, pos, &entry, commands)
                    }))?;
                }
                Ok(())
            }),
        )
    })
}

pub fn fmt_message(
    f: &mut nojson::JsonFormatter<'_, '_>,
    message: &noraft::Message,
    commands: &crate::node::RecentCommands,
) -> std::fmt::Result {
    f.object(|f| match message {
        noraft::Message::RequestVoteCall {
            from,
            term,
            last_position,
        } => {
            f.member("type", "RequestVoteCall")?;
            fmt_message_common_members(f, *from, *term)?;
            f.member("last_term", last_position.term.get())?;
            f.member("last_index", last_position.index.get())
        }
        noraft::Message::RequestVoteReply {
            from,
            term,
            vote_granted,
        } => {
            f.member("type", "RequestVoteReply")?;
            fmt_message_common_members(f, *from, *term)?;
            f.member("vote_granted", vote_granted)
        }
        noraft::Message::AppendEntriesCall {
            from,
            term,
            commit_index,
            entries,
        } => {
            f.member("type", "AppendEntriesCall")?;
            fmt_message_common_members(f, *from, *term)?;
            f.member("commit_index", commit_index.get())?;
            let prev_position = entries.prev_position();
            f.member("prev_term", prev_position.term.get())?;
            f.member("prev_index", prev_position.index.get())?;
            f.member(
                "entries",
                nojson::array(|f| {
                    for (pos, entry) in entries.iter_with_positions() {
                        f.element(nojson::object(|f| {
                            fmt_log_entry_members(f, pos, &entry, commands)
                        }))?;
                    }
                    Ok(())
                }),
            )
        }
        noraft::Message::AppendEntriesReply {
            from,
            term,
            generation,
            last_position,
        } => {
            f.member("type", "AppendEntriesReply")?;
            fmt_message_common_members(f, *from, *term)?;
            f.member("generation", generation.get())?;
            f.member("last_term", last_position.term.get())?;
            f.member("last_index", last_position.index.get())
        }
    })
}

fn fmt_message_common_members(
    f: &mut nojson::JsonObjectFormatter<'_, '_, '_>,
    from: noraft::NodeId,
    term: noraft::Term,
) -> std::fmt::Result {
    f.member("from", from.get())?;
    f.member("term", term.get())
}

pub fn get_command_values(
    value: nojson::RawJsonValue<'_, '_>,
    message: &noraft::Message,
) -> Option<impl Iterator<Item = (noraft::LogPosition, crate::node::JsonLineValue)>> {
    let noraft::Message::AppendEntriesCall { entries, .. } = message else {
        return None;
    };

    let entry_values = value
        .to_member("entries")
        .and_then(|v| v.required())
        .and_then(|v| v.to_array())
        .expect("bug");
    Some(
        entries
            .iter_with_positions()
            .zip(entry_values)
            .filter_map(|((pos, entry), value)| {
                if !matches!(entry, noraft::LogEntry::Command) {
                    return None;
                };
                let command_value = value
                    .to_member("value")
                    .and_then(|v| v.required())
                    .expect("bug");
                Some((pos, crate::node::JsonLineValue::new_internal(command_value)))
            }),
    )
}

/// Converts a JSON value to a Message, excluding the command value
///
/// This function parses JSON representations of Raft messages back into their
/// corresponding Message types. Note that for Command entries, only the structure
/// is validated; the actual command data must be managed separately by the caller.
pub fn json_to_message(
    value: nojson::RawJsonValue<'_, '_>,
) -> Result<noraft::Message, nojson::JsonParseError> {
    // TODO: use str
    let msg_type_str: String = value.to_member("type")?.required()?.try_into()?;

    let from = noraft::NodeId::new(value.to_member("from")?.required()?.try_into()?);
    let term = noraft::Term::new(value.to_member("term")?.required()?.try_into()?);

    match msg_type_str.as_str() {
        "RequestVoteCall" => {
            let last_term =
                noraft::Term::new(value.to_member("last_term")?.required()?.try_into()?);
            let last_index =
                noraft::LogIndex::new(value.to_member("last_index")?.required()?.try_into()?);

            Ok(noraft::Message::RequestVoteCall {
                from,
                term,
                last_position: noraft::LogPosition {
                    term: last_term,
                    index: last_index,
                },
            })
        }
        "RequestVoteReply" => {
            let vote_granted: bool = value.to_member("vote_granted")?.required()?.try_into()?;

            Ok(noraft::Message::RequestVoteReply {
                from,
                term,
                vote_granted,
            })
        }
        "AppendEntriesCall" => {
            let commit_index =
                noraft::LogIndex::new(value.to_member("commit_index")?.required()?.try_into()?);

            let prev_term =
                noraft::Term::new(value.to_member("prev_term")?.required()?.try_into()?);
            let prev_index =
                noraft::LogIndex::new(value.to_member("prev_index")?.required()?.try_into()?);
            let prev_position = noraft::LogPosition {
                term: prev_term,
                index: prev_index,
            };

            let entries_array = value.to_member("entries")?.required()?.to_array()?;

            let mut entries = noraft::LogEntries::new(prev_position);
            for entry_value in entries_array {
                // TODO: use str
                let entry_type_str: String = entry_value
                    .to_member("type")?
                    .required()?
                    .to_unquoted_string_str()?
                    .into_owned();

                let entry = match entry_type_str.as_str() {
                    "Term" => {
                        let term = noraft::Term::new(
                            entry_value.to_member("term")?.required()?.try_into()?,
                        );
                        noraft::LogEntry::Term(term)
                    }
                    "ClusterConfig" => {
                        let mut config = noraft::ClusterConfig::new();
                        config.voters = entry_value
                            .to_member("voters")?
                            .required()?
                            .to_array()?
                            .map(|v| {
                                let node_id: u64 = v.try_into()?;
                                Ok(noraft::NodeId::new(node_id))
                            })
                            .collect::<Result<_, _>>()?;

                        config.new_voters = entry_value
                            .to_member("new_voters")?
                            .required()?
                            .to_array()?
                            .map(|v| {
                                let node_id: u64 = v.try_into()?;
                                Ok(noraft::NodeId::new(node_id))
                            })
                            .collect::<Result<_, _>>()?;

                        noraft::LogEntry::ClusterConfig(config)
                    }
                    "Command" => {
                        entry_value.to_member("value")?.required()?;
                        noraft::LogEntry::Command
                    }
                    _ => {
                        return Err(entry_value
                            .invalid(format!("unknown log entry type: {}", entry_type_str)));
                    }
                };

                entries.push(entry);
            }

            Ok(noraft::Message::AppendEntriesCall {
                from,
                term,
                commit_index,
                entries,
            })
        }
        "AppendEntriesReply" => {
            let generation: u64 = value.to_member("generation")?.required()?.try_into()?;
            let last_term =
                noraft::Term::new(value.to_member("last_term")?.required()?.try_into()?);
            let last_index =
                noraft::LogIndex::new(value.to_member("last_index")?.required()?.try_into()?);

            Ok(noraft::Message::AppendEntriesReply {
                from,
                term,
                generation: noraft::NodeGeneration::new(generation),
                last_position: noraft::LogPosition {
                    term: last_term,
                    index: last_index,
                },
            })
        }
        _ => Err(value.invalid(format!("Unknown message type: {}", msg_type_str))),
    }
}
