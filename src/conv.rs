pub fn fmt_log_position_members(
    f: &mut nojson::JsonObjectFormatter<'_, '_, '_>,
    position: raftbare::LogPosition,
) -> std::fmt::Result {
    f.member("term", position.term.get())?;
    f.member("index", position.index.get())
}

pub fn fmt_log_entry_members(
    f: &mut nojson::JsonObjectFormatter<'_, '_, '_>,
    pos: raftbare::LogPosition,
    entry: &raftbare::LogEntry,
    commands: &crate::node::RecentCommands,
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
            let command = commands.get(&pos.index).expect("bug");
            f.member("value", command)
        }
    }
}

pub fn fmt_log_entries(
    f: &mut nojson::JsonFormatter<'_, '_>,
    entries: &raftbare::LogEntries,
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
    message: &raftbare::Message,
    commands: &crate::node::RecentCommands,
) -> std::fmt::Result {
    f.object(|f| match message {
        raftbare::Message::RequestVoteCall {
            header,
            last_position,
        } => {
            f.member("type", "RequestVoteCall")?;
            fmt_message_header_members(f, header)?;
            f.member("last_term", last_position.term.get())?;
            f.member("last_index", last_position.index.get())
        }
        raftbare::Message::RequestVoteReply {
            header,
            vote_granted,
        } => {
            f.member("type", "RequestVoteReply")?;
            fmt_message_header_members(f, header)?;
            f.member("vote_granted", vote_granted)
        }
        raftbare::Message::AppendEntriesCall {
            header,
            commit_index,
            entries,
        } => {
            f.member("type", "AppendEntriesCall")?;
            fmt_message_header_members(f, header)?;
            f.member("commit_index", commit_index.get())?;
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
        raftbare::Message::AppendEntriesReply {
            header,
            last_position,
        } => {
            f.member("type", "AppendEntriesReply")?;
            fmt_message_header_members(f, header)?;
            f.member("last_term", last_position.term.get())?;
            f.member("last_index", last_position.index.get())
        }
    })
}

fn fmt_message_header_members(
    f: &mut nojson::JsonObjectFormatter<'_, '_, '_>,
    header: &raftbare::MessageHeader,
) -> std::fmt::Result {
    f.member("from", header.from.get())?;
    f.member("term", header.term.get())?;
    f.member("seqno", header.seqno.get())
}

/// Converts a JSON value to a Message, excluding the command value
///
/// This function parses JSON representations of Raft messages back into their
/// corresponding Message types. Note that for Command entries, only the structure
/// is validated; the actual command data must be managed separately by the caller.
pub fn json_to_message(
    value: nojson::RawJsonValue<'_, '_>,
) -> Result<raftbare::Message, nojson::JsonParseError> {
    // TODO: use str
    let msg_type_str: String = value.to_member("type")?.required()?.try_into()?;

    let from = raftbare::NodeId::new(value.to_member("from")?.required()?.try_into()?);
    let term = raftbare::Term::new(value.to_member("term")?.required()?.try_into()?);
    let seqno = raftbare::MessageSeqNo::new(value.to_member("seqno")?.required()?.try_into()?);

    let header = raftbare::MessageHeader { from, term, seqno };

    match msg_type_str.as_str() {
        "RequestVoteCall" => {
            let last_term =
                raftbare::Term::new(value.to_member("last_term")?.required()?.try_into()?);
            let last_index =
                raftbare::LogIndex::new(value.to_member("last_index")?.required()?.try_into()?);

            Ok(raftbare::Message::RequestVoteCall {
                header,
                last_position: raftbare::LogPosition {
                    term: last_term,
                    index: last_index,
                },
            })
        }
        "RequestVoteReply" => {
            let vote_granted: bool = value.to_member("vote_granted")?.required()?.try_into()?;

            Ok(raftbare::Message::RequestVoteReply {
                header,
                vote_granted,
            })
        }
        "AppendEntriesCall" => {
            let commit_index =
                raftbare::LogIndex::new(value.to_member("commit_index")?.required()?.try_into()?);

            let entries_array = value.to_member("entries")?.required()?.to_array()?;

            let mut entries = raftbare::LogEntries::new(raftbare::LogPosition::ZERO);
            for entry_value in entries_array {
                let entry_type_str: String = entry_value
                    .to_member("type")?
                    .required()?
                    .to_unquoted_string_str()?
                    .into_owned();

                let entry = match entry_type_str.as_str() {
                    "Term" => {
                        let term = raftbare::Term::new(
                            entry_value.to_member("term")?.required()?.try_into()?,
                        );
                        raftbare::LogEntry::Term(term)
                    }
                    "ClusterConfig" => {
                        let voters: Vec<raftbare::NodeId> = entry_value
                            .to_member("voters")?
                            .required()?
                            .to_array()?
                            .map(|v| {
                                let node_id: u64 = v.try_into()?;
                                Ok(raftbare::NodeId::new(node_id))
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        let new_voters: Vec<raftbare::NodeId> = entry_value
                            .to_member("new_voters")?
                            .required()?
                            .to_array()?
                            .map(|v| {
                                let node_id: u64 = v.try_into()?;
                                Ok(raftbare::NodeId::new(node_id))
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        let mut config = raftbare::ClusterConfig::new();
                        for voter in voters {
                            config.voters.insert(voter);
                        }
                        for new_voter in new_voters {
                            config.new_voters.insert(new_voter);
                        }

                        raftbare::LogEntry::ClusterConfig(config)
                    }
                    "Command" => raftbare::LogEntry::Command,
                    _ => {
                        return Err(entry_value
                            .invalid(format!("Unknown log entry type: {}", entry_type_str)));
                    }
                };

                entries.push(entry);
            }

            Ok(raftbare::Message::AppendEntriesCall {
                header,
                commit_index,
                entries,
            })
        }
        "AppendEntriesReply" => {
            let last_term =
                raftbare::Term::new(value.to_member("last_term")?.required()?.try_into()?);
            let last_index =
                raftbare::LogIndex::new(value.to_member("last_index")?.required()?.try_into()?);

            Ok(raftbare::Message::AppendEntriesReply {
                header,
                last_position: raftbare::LogPosition {
                    term: last_term,
                    index: last_index,
                },
            })
        }
        _ => Err(value.invalid(format!("Unknown message type: {}", msg_type_str))),
    }
}
