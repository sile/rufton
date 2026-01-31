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

    fn is_initialized(&self) -> bool {
        self.inner.id() != UNINIT_NODE_ID
    }

    pub fn init_cluster(&mut self) -> bool {
        if self.is_initialized() {
            return false;
        }

        let node_id = self.machine.next_node_id;
        self.machine.next_node_id = increment_node_id(self.machine.next_node_id);
        self.inner = raftbare::Node::start(node_id);
        self.push_action(RaftNodeAction::InitMachine);

        true
    }

    pub fn add_node(&mut self) -> bool {
        if !self.is_initialized() {
            return false;
        }

        todo!()
    }

    fn push_action(&mut self, action: RaftNodeAction) {
        self.action_queue.push_back(action);
    }

    pub fn next_action(&mut self) -> Option<RaftNodeAction> {
        if !self.is_initialized() {
            return None;
        }

        for inner_action in self.inner.actions_mut() {
            todo!("convert: {inner_action:?}");
        }
        self.action_queue.pop_front()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_cluster() {
        let mut node = RaftNode::new();
        assert!(node.init_cluster());
        assert!(!node.init_cluster());

        assert_eq!(node.next_action(), Some(RaftNodeAction::InitMachine));
        assert_eq!(node.next_action(), None);
    }
}
