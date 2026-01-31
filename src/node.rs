const UNINIT_NODE_ID: raftbare::NodeId = raftbare::NodeId::new(0);

#[derive(Debug)]
pub struct RaftNode {
    pub node: raftbare::Node,
    machine: RaftNodeStateMachine,
}

impl RaftNode {
    pub fn new() -> Self {
        Self {
            node: raftbare::Node::start(UNINIT_NODE_ID),
            machine: RaftNodeStateMachine {
                next_node_id: increment_node_id(UNINIT_NODE_ID),
            },
        }
    }

    pub fn initialize_cluster(&mut self) -> bool {
        if self.node.id() != UNINIT_NODE_ID {
            return false;
        }

        let node_id = self.machine.next_node_id;
        self.machine.next_node_id = increment_node_id(self.machine.next_node_id);

        self.node = raftbare::Node::start(node_id);
        true
    }
}

#[derive(Debug)]
pub struct RaftNodeStateMachine {
    pub next_node_id: raftbare::NodeId,
}

fn increment_node_id(id: raftbare::NodeId) -> raftbare::NodeId {
    raftbare::NodeId::new(id.get() + 1)
}
