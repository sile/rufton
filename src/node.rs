#[derive(Debug)]
pub struct RaftNode {
    pub addr: std::net::SocketAddr,
    pub inner: raftbare::Node,
    pub machine: RaftNodeStateMachine,
    pub action_queue: std::collections::VecDeque<RaftNodeAction>,
    pub initialized: bool,
}

impl RaftNode {
    pub fn new(id: raftbare::NodeId, addr: std::net::SocketAddr) -> Self {
        Self {
            addr,
            inner: raftbare::Node::start(id),
            machine: RaftNodeStateMachine {
                node_addrs: [(id, addr)].into_iter().collect(),
            },
            action_queue: std::collections::VecDeque::new(),
            initialized: false,
        }
    }

    pub fn init_cluster(&mut self) -> bool {
        if self.initialized {
            return false;
        }

        self.initialized = true;
        self.push_action(RaftNodeAction::InitMachine);

        true
    }

    pub fn add_node(&mut self) -> bool {
        if !self.initialized {
            return false;
        }

        todo!()
    }

    fn push_action(&mut self, action: RaftNodeAction) {
        self.action_queue.push_back(action);
    }

    pub fn next_action(&mut self) -> Option<RaftNodeAction> {
        if !self.initialized {
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
    InitMachine, // TODO: remove
}

#[derive(Debug)]
pub struct RaftNodeStateMachine {
    pub node_addrs: std::collections::BTreeMap<raftbare::NodeId, std::net::SocketAddr>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_cluster() {
        let mut node = RaftNode::new(id(0), ([127, 0, 0, 1], 9000).into());
        assert!(node.init_cluster());
        assert!(!node.init_cluster());

        assert_eq!(node.next_action(), Some(RaftNodeAction::InitMachine));
        assert_eq!(node.next_action(), None);
    }

    fn id(n: u64) -> raftbare::NodeId {
        raftbare::NodeId::new(n)
    }
}
