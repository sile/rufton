#[derive(Debug)]
pub struct RaftNode {
    pub inner: raftbare::Node,
    pub machine: RaftNodeStateMachine,
    pub action_queue: std::collections::VecDeque<RaftNodeAction>,
    pub initialized: bool,
    pub instance_id: u64,
    pub local_command_seqno: u64,
}

impl RaftNode {
    pub fn new(id: raftbare::NodeId, addr: std::net::SocketAddr, instance_id: u64) -> Self {
        Self {
            inner: raftbare::Node::start(id),
            machine: RaftNodeStateMachine {
                node_addrs: [(id, addr)].into_iter().collect(),
            },
            action_queue: std::collections::VecDeque::new(),
            initialized: false,
            instance_id,
            local_command_seqno: 0,
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

    pub fn propose(&mut self, command: RaftNodeCommand) -> bool {
        if !self.initialized {
            return false;
        }

        let proposal_id = ProposalId {
            node_id: self.inner.id(),
            instance_id: self.instance_id,
            local_seqno: self.local_command_seqno,
        };
        self.local_command_seqno += 1;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProposalId {
    node_id: raftbare::NodeId,
    instance_id: u64,
    local_seqno: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftNodeCommand {
    AddNode {
        id: raftbare::NodeId,
        addr: std::net::SocketAddr,
    },
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
        let mut node = RaftNode::new(id(0), ([127, 0, 0, 1], 9000).into(), 0);
        assert!(node.init_cluster());
        assert!(!node.init_cluster());

        assert_eq!(node.next_action(), Some(RaftNodeAction::InitMachine));
        assert_eq!(node.next_action(), None);
    }

    fn id(n: u64) -> raftbare::NodeId {
        raftbare::NodeId::new(n)
    }
}
