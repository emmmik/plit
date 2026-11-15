const MIN_ELECTION_TIMEOUT: u64 = 150;
const MAX_ELECTION_TIMEOUT: u64 = 300;
const NODE_NUMBER: u32 = 3;

#[derive(Debug, PartialEq, Clone, Copy, Eq, Ord, PartialOrd)]
pub enum Event<M> {
    MessageDelivered { from: u32, to: u32, msg: M },
    TimerFired { node: u32 },
}

#[derive(Clone, Copy)]
pub enum Action<M> {
    Send { to: u32, msg: M },
    SetTimer { min_delay: u64, max_delay: u64 },
}

pub trait Protocol {
    type Msg;

    fn handle(&mut self, e: &Event<Self::Msg>) -> Vec<Action<Self::Msg>>;
}

impl<T> Event<T> {
    pub fn target_node(&self) -> u32 {
        match self {
            Event::TimerFired { node } => *node,
            Event::MessageDelivered { from, to, msg } => *to,
        }
    }
}

#[derive(Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub struct RaftNode {
    role: Role,
    current_term: u64,
    voted_for: Option<u32>,
    // log
    votes: u32,
    peers: Vec<u32>,
}

#[derive(Clone, Copy)]
pub enum RPC {
    AppendEntries {
        term: u64,
        // leader_id: u32,
    },
    RequestVote {
        term: u64,
        candidate_id: u32,
        // last_log_index: usize,
        // last_log_term: u64,
    },
    RequestVoteReply {
        term: u64,
        vote_granted: bool,
    },
}

impl Protocol for RaftNode {
    type Msg = RPC;

    fn handle(&mut self, event: &Event<RPC>) -> Vec<Action<RPC>> {
        match event {
            Event::TimerFired { node } => {
                if let Role::Leader = self.role {
                    panic!("Error: Timer fired on the leader!");
                }

                // Regardless if the node is a candidate or a follower, a new election has to be
                // started (with the current node prmoting itself to a candidate)
                self.role = Role::Candidate;

                self.current_term += 1;
                self.voted_for = Some(*node);
                self.votes = 1;

                let request_vote_rpc = RPC::RequestVote {
                    term: self.current_term,
                    candidate_id: *node,
                };

                let mut actions = vec![];

                self.start_timeout(&mut actions);
                self.push_to_peers(&mut actions, request_vote_rpc);

                // actions.push(Action::SetTimer {
                //     min_delay: MIN_ELECTION_TIMEOUT,
                //     max_delay: MAX_ELECTION_TIMEOUT,
                // });
                //
                // for &peer in &self.peers {
                //     actions.push(Action::Send {
                //         to: peer,
                //         msg: request_vote_rpc,
                //     });
                // }

                actions
            }
            Event::MessageDelivered { from, to, msg } => match msg {
                RPC::RequestVote { term, candidate_id } => {
                    let mut actions = vec![];

                    if term <= &self.current_term
                        || (term == &self.current_term && self.voted_for.is_some())
                    {
                        actions.push(Action::Send {
                            to: *candidate_id,
                            msg: RPC::RequestVoteReply {
                                term: self.current_term,
                                vote_granted: false,
                            },
                        });
                        return actions;
                    }

                    self.current_term = *term;
                    self.voted_for = Some(*candidate_id);
                    self.role = Role::Follower;
                    self.votes = 0;

                    self.start_timeout(&mut actions);

                    actions.push(Action::Send {
                        to: *candidate_id,
                        msg: RPC::RequestVoteReply {
                            term: self.current_term,
                            vote_granted: true,
                        },
                    });

                    actions
                }
                RPC::RequestVoteReply { term, vote_granted } => {
                    let mut actions = vec![];

                    if term > &self.current_term {
                        self.current_term = *term;
                        self.voted_for = None;
                        self.role = Role::Follower;
                        self.votes = 0;

                        self.start_timeout(&mut actions);

                        return actions;
                    }

                    if *vote_granted {
                        self.votes += 1;
                    }

                    if self.votes > NODE_NUMBER / 2 {
                        self.role = Role::Leader;

                        self.push_to_peers(
                            &mut actions,
                            RPC::AppendEntries {
                                term: self.current_term,
                                // leader_id: node,
                            },
                        );
                    }

                    actions
                }
                RPC::AppendEntries { term } => {
                    let mut actions = vec![];

                    self.role = Role::Follower;
                    self.current_term = *term;

                    self.start_timeout(&mut actions);

                    actions
                }
            },
        }
    }
}

impl RaftNode {
    fn push_to_peers(&self, actions: &mut Vec<Action<RPC>>, msg: RPC) {
        for &peer in &self.peers {
            actions.push(Action::Send { to: peer, msg });
        }
    }

    fn start_timeout(&self, actions: &mut Vec<Action<RPC>>) {
        actions.push(Action::SetTimer {
            min_delay: MIN_ELECTION_TIMEOUT,
            max_delay: MAX_ELECTION_TIMEOUT,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_election() {}
}
