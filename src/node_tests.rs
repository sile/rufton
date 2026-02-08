use crate::{Action, JsonValue, Node, StorageEntry};

#[test]
fn init_cluster() {
    let mut node = Node::start(node_id(0));
    let members = [node_id(0)];
    assert!(node.init_cluster(&members));
    assert!(!node.init_cluster(&members));
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
    while node.next_action().is_some() {}

    let node_members: Vec<_> = node.members().collect();
    assert_eq!(node_members, vec![node_id(0)]);
}

#[test]
fn init_cluster_requires_self_member() {
    let mut node = Node::start(node_id(0));
    assert!(!node.init_cluster(&[node_id(1)]));
    assert!(!node.initialized);
    assert!(node.init_cluster(&[node_id(0), node_id(1)]));
}

#[test]
fn load_increments_generation() {
    let mut node = Node::start(node_id(0));
    node.action_queue.clear();

    let entry = JsonValue::new(StorageEntry::NodeGeneration(0));
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
    let mut node = Node::start(node_id(0));
    node.action_queue.clear();

    let entry1 = JsonValue::new(StorageEntry::NodeGeneration(2));
    let entry2 = JsonValue::new(StorageEntry::NodeGeneration(5));
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
    let mut node = Node::start(node_id(0));
    assert!(node.init_cluster(&[node_id(0)]));
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
    let mut node0 = Node::start(node_id(0));
    let mut node1 = Node::start(node_id(1));

    let members = [node_id(0), node_id(1)];
    assert!(node0.init_cluster(&members));
    assert!(node1.init_cluster(&members));
    node0.handle_timeout();

    let mut nodes = [node0, node1];
    run_actions(&mut nodes);

    let leader_index = nodes
        .iter()
        .position(|node| node.inner.role().is_leader())
        .expect("leader should exist");
    let follower_index = 1 - leader_index;

    let request = JsonValue::new("snapshot_test");
    nodes[leader_index].propose_command(request);

    while let Some(action) = nodes[leader_index].next_action() {
        if let Action::BroadcastMessage(m) = action {
            assert!(nodes[follower_index].handle_message(m.get()));
            break;
        }
    }

    let applied_index = nodes[follower_index].applied_index;
    let snapshot = nodes[follower_index]
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
        let ty = entry
            .to_member("type")
            .expect("type")
            .required()
            .expect("type required")
            .as_string_str()
            .expect("type string");
        assert_eq!(ty, "Command");
        count += 1;
    }
    assert_eq!(count, 1);
}

fn run_actions(nodes: &mut [Node]) -> Vec<(noraft::NodeId, Action)> {
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
                                assert!(nodes[j].handle_message(m.get()));
                            }
                        }
                    }
                    Action::SendMessage(j, m) => {
                        let j = j.get() as usize;
                        assert!(nodes[j].handle_message(m.get()));
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
    let mut node0 = Node::start(node_id(0));
    let mut node1 = Node::start(node_id(1));

    let members = [node_id(0), node_id(1)];
    assert!(node0.init_cluster(&members));
    assert!(node1.init_cluster(&members));
    node0.handle_timeout();

    let mut nodes = [node0, node1];
    run_actions(&mut nodes);

    let members0: Vec<_> = nodes[0].members().collect();
    let members1: Vec<_> = nodes[1].members().collect();
    assert_eq!(members0, vec![node_id(0), node_id(1)]);
    assert_eq!(members1, vec![node_id(0), node_id(1)]);
}

#[test]
fn propose_command_to_non_leader_node() {
    let mut node0 = Node::start(node_id(0));
    let mut node1 = Node::start(node_id(1));

    let members = [node_id(0), node_id(1)];
    assert!(node0.init_cluster(&members));
    assert!(node1.init_cluster(&members));
    node0.handle_timeout();

    let mut nodes = [node0, node1];
    run_actions(&mut nodes);

    let leader_index = nodes
        .iter()
        .position(|node| node.inner.role().is_leader())
        .expect("leader should exist");
    let follower_index = 1 - leader_index;

    // Try to propose a command to the non-leader
    let request = JsonValue::new("test_command");
    nodes[follower_index].propose_command(request);

    let actions = run_actions(&mut nodes);
    // Check that actions contain an Apply from the proposer
    let found_apply = actions.iter().any(|(node_id, action)| {
        dbg!(node_id, action);
        if let Action::Apply { is_proposer, .. } = action
            && *is_proposer
        {
            dbg!(node_id, action);
            *node_id == nodes[follower_index].id()
        } else {
            false
        }
    });
    assert!(
        found_apply,
        "Apply action from proposer should be in actions"
    );
}

#[test]
fn propose_query() {
    let mut node0 = Node::start(node_id(0));
    let mut node1 = Node::start(node_id(1));

    let members = [node_id(0), node_id(1)];
    assert!(node0.init_cluster(&members));
    assert!(node1.init_cluster(&members));
    node0.handle_timeout();

    let mut nodes = [node0, node1];
    run_actions(&mut nodes);

    let leader_index = nodes
        .iter()
        .position(|node| node.inner.role().is_leader())
        .expect("leader should exist");

    // Propose a query on the leader
    let request = JsonValue::new("test_query");
    nodes[leader_index].propose_query(request.clone());

    let actions = run_actions(&mut nodes);

    // Check that an Apply action was generated with the matching request
    let found_apply = actions.iter().any(|(node_id, action)| {
        if let Action::Apply {
            is_proposer,
            request: action_request,
            ..
        } = action
        {
            *node_id == nodes[leader_index].id() && *is_proposer && *action_request == request
        } else {
            false
        }
    });
    assert!(
        found_apply,
        "Apply action with matching request should be returned by leader"
    );
}

#[test]
fn propose_query_on_non_leader_node() {
    let mut node0 = Node::start(node_id(0));
    let mut node1 = Node::start(node_id(1));

    let members = [node_id(0), node_id(1)];
    assert!(node0.init_cluster(&members));
    assert!(node1.init_cluster(&members));
    node0.handle_timeout();

    let mut nodes = [node0, node1];
    run_actions(&mut nodes);

    let leader_index = nodes
        .iter()
        .position(|node| node.inner.role().is_leader())
        .expect("leader should exist");
    let follower_index = 1 - leader_index;

    // Propose a query on the non-leader
    let request = JsonValue::new("test_query");
    nodes[follower_index].propose_query(request.clone());

    let actions = run_actions(&mut nodes);

    // Check that the query was redirected to the leader and eventually resolved
    let found_apply = actions.iter().any(|(node_id, action)| {
        if let Action::Apply {
            is_proposer,
            request: action_request,
            ..
        } = action
        {
            *node_id == nodes[follower_index].id() && *is_proposer && *action_request == request
        } else {
            false
        }
    });
    assert!(
        found_apply,
        "Query should be redirected to leader and resolved"
    );
}

#[test]
fn strip_memory_log() {
    let mut node0 = Node::start(node_id(0));

    // Create single node cluster
    assert!(node0.init_cluster(&[node_id(0)]));
    while node0.next_action().is_some() {}

    // Propose and commit some commands
    let request1 = JsonValue::new("command1");
    node0.propose_command(request1);
    while node0.next_action().is_some() {}

    let request2 = JsonValue::new("command2");
    node0.propose_command(request2);
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

    let node0_members: Vec<_> = node0.members().collect();
    assert_eq!(node0_members, vec![node_id(0)]);
}

fn append_storage_entry_action(json: &str) -> Action {
    let raw_json = nojson::RawJsonOwned::parse(json.to_string()).expect("invalid json");
    let value = JsonValue::new(raw_json.value());
    Action::AppendStorageEntry(value)
}

fn set_leader_timeout_action() -> Action {
    Action::SetTimeout(noraft::Role::Leader)
}

fn node_id(n: u64) -> noraft::NodeId {
    noraft::NodeId::new(n)
}
