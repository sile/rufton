const UNINIT_NODE_ID: raftbare::NodeId = raftbare::NodeId::new(0);

#[derive(Debug)]
pub struct RaftNode {
    pub inner: raftbare::Node,
    pub machine: RaftNodeStateMachine,
    pub action_queue: std::collections::VecDeque<RaftNodeAction>,
}

impl RaftNode {

    pub fn new() -> Self {
        Self {
            inner: raftbare::Node::start(UNINIT_NODE_ID),
            machine: RaftNodeStateMachine {
                next_node_id: increment_node_id(UNINIT_NODE_ID),
            },
            action_queue: std::collections::VecDeque::new(),
        }
    }

    pub fn init_cluster(&mut self) -> bool {
        if self.inner.id() != UNINIT_NODE_ID {
            return false;
        }

        let node_id = self.machine.next_node_id;
        self.machine.next_node_id = increment_node_id(self.machine.next_node_id);
        self.inner = raftbare::Node::start(node_id);
        self.push_action(RaftNodeAction::InitMachine);

        true
    }

    fn push_action(&mut self, action: RaftNodeAction) {
        for inner_action in self.inner.actions_mut() {
            todo!("convert: {inner_action:?}");
        }
        self.action_queue.push_back(action);
    }

    pub fn next_action(&mut self) -> Option<RaftNodeAction> {
        self.action_queue.pop_front()
    }
}

#[derive(Debug)]
pub enum RaftNodeAction {
    InitMachine,
}

#[derive(Debug)]
pub struct RaftNodeStateMachine {
    pub next_node_id: raftbare::NodeId,
}

fn increment_node_id(id: raftbare::NodeId) -> raftbare::NodeId {
    raftbare::NodeId::new(id.get() + 1)
}
