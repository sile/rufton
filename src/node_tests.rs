use crate::{Action, JsonLineValue, RaftNode, StorageEntry};

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

    assert_eq!(node.inner.generation().get(), 1);
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

    assert_eq!(node.inner.generation().get(), 6);
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

#[test]
fn propose_remove_node() {
    let mut node0 = RaftNode::start(node_id(0));
    let node1 = RaftNode::start(node_id(1));

    assert!(node0.init_cluster());
    while node0.next_action().is_some() {}

    node0.propose_add_node(node1.id());

    let mut nodes = [node0, node1];
    run_actions(&mut nodes);

    let proposal_id = nodes[0].remove_node(node_id(1));

    let mut found_commit = false;
    let actions = run_actions(&mut nodes);
    for (node_id, action) in actions {
        if node_id == nodes[0].id() {
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
    }
    assert!(found_commit, "Remove proposal should be committed");

    assert_eq!(nodes[0].machine.nodes.len(), 1);
    assert_eq!(nodes[1].machine.nodes.len(), 1);
    assert!(nodes[0].machine.nodes.contains(&node_id(0)));
    assert!(nodes[1].machine.nodes.contains(&node_id(0)));
}

fn run_actions(nodes: &mut [RaftNode]) -> Vec<(noraft::NodeId, Action)> {
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
        remaining_commands.is_empty() || remaining_commands.iter().all(|idx| *idx > commit_index)
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
    let value = JsonLineValue::new_internal(raw_json.value());
    Action::AppendStorageEntry(value)
}

fn set_leader_timeout_action() -> Action {
    Action::SetTimeout(noraft::Role::Leader)
}

fn node_id(n: u64) -> noraft::NodeId {
    noraft::NodeId::new(n)
}
