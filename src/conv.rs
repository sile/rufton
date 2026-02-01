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
