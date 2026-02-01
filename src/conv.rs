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
