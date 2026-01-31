#[derive(Debug)]
pub struct RaftNode {
    pub inner: raftbare::Node,
    pub machine: RaftNodeStateMachine,
    pub action_queue: std::collections::VecDeque<RaftNodeAction>,
    pub recent_commands: std::collections::BTreeMap<raftbare::LogPosition, JsonLineValue>,
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

            recent_commands: std::collections::BTreeMap::new(),
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

    pub fn add_node(&mut self, id: raftbare::NodeId, addr: std::net::SocketAddr) -> ProposalId {
        let proposal_id = ProposalId {
            node_id: self.inner.id(),
            instance_id: self.instance_id,
            local_seqno: self.local_command_seqno,
        };
        self.local_command_seqno += 1;

        if !self.initialized {
            self.push_action(RaftNodeAction::Rejected {
                proposal_id,
                reason: "cluster has not been initialized".to_owned(),
            });
            return proposal_id;
        }

        if !self.inner.role().is_leader() {
            todo!()
        }

        let position = self.inner.propose_command();

        let command = RaftNodeCommand::AddNode {
            proposal_id,
            id,
            addr,
        };
        let value = JsonLineValue::new_internal(command);
        self.recent_commands.insert(position, value);

        proposal_id
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

impl nojson::DisplayJson for ProposalId {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        [self.node_id.get(), self.instance_id, self.local_seqno].fmt(f)
    }
}

impl<'text, 'raw> TryFrom<nojson::RawJsonValue<'text, 'raw>> for ProposalId {
    type Error = nojson::JsonParseError;

    fn try_from(value: nojson::RawJsonValue<'text, 'raw>) -> Result<Self, Self::Error> {
        let [node_id, instance_id, local_seqno] = value.try_into()?;
        Ok(ProposalId {
            node_id: raftbare::NodeId::new(node_id),
            instance_id,
            local_seqno,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonLineValue(std::sync::Arc<nojson::RawJsonOwned>);

impl JsonLineValue {
    // Assumes the input does not include newlines
    fn new_internal<T: nojson::DisplayJson>(v: T) -> Self {
        let line = nojson::Json(v).to_string();
        let json = nojson::RawJsonOwned::parse(line).expect("infallibe");
        Self(std::sync::Arc::new(json))
    }

    pub fn get(&self) -> nojson::RawJsonValue<'_, '_> {
        self.0.value()
    }
}

impl nojson::DisplayJson for JsonLineValue {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        writeln!(f.inner_mut(), "{}", self.0.text())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftNodeCommand {
    AddNode {
        proposal_id: ProposalId,
        id: raftbare::NodeId,
        addr: std::net::SocketAddr,
    },
}

impl nojson::DisplayJson for RaftNodeCommand {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> std::fmt::Result {
        match self {
            RaftNodeCommand::AddNode {
                proposal_id,
                id,
                addr,
            } => f.object(|f| {
                f.member("type", "AddNode")?;
                f.member("proposal_id", proposal_id)?;
                f.member("id", id.get())?;
                f.member("addr", addr)
            }),
        }
    }
}

impl<'text, 'raw> TryFrom<nojson::RawJsonValue<'text, 'raw>> for RaftNodeCommand {
    type Error = nojson::JsonParseError;

    fn try_from(value: nojson::RawJsonValue<'text, 'raw>) -> Result<Self, Self::Error> {
        let ty = value
            .to_member("type")?
            .required()?
            .to_unquoted_string_str()?;
        match ty.as_ref() {
            "AddNode" => {
                let proposal_id = value.to_member("proposal_id")?.required()?.try_into()?;
                let id = value.to_member("id")?.required()?.try_into()?;
                let addr = value.to_member("addr")?.required()?.try_into()?;
                Ok(RaftNodeCommand::AddNode {
                    proposal_id,
                    id: raftbare::NodeId::new(id),
                    addr,
                })
            }
            ty => Err(value.invalid(format!("unknown command type: {ty}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftNodeAction {
    InitMachine, // TODO: remove
    Commited {
        proposal_id: ProposalId,
        result: JsonLineValue,
    },
    // TODO: add a variant for "committed but the result is unknown (e.g. the commit position was skipped by snapshot)
    Rejected {
        proposal_id: ProposalId,
        reason: String,
    },
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
