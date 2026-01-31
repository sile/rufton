const UNINIT_NODE_ID: raftbare::NodeId = raftbare::NodeId::new(0);

#[derive(Debug)]
pub struct RaftNode {
    pub node_id: raftbare::NodeId,
}

impl RaftNode {
    pub fn new() -> Self {
        Self {
            node_id: UNINIT_NODE_ID,
        }
    }

    pub fn initialize_cluster(&mut self) -> bool {
        if self.node_id != UNINIT_NODE_ID {
            return false;
        }
        todo!()
    }
}
