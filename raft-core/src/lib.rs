const MIN_ELECTION_TIMEOUT: u64 = 150;
const MAX_ELECTION_TIMEOUT: u64 = 300;
const HEARTBEAT_TIMEOUT: u64 = 50;
const NODE_NUMBER: u32 = 3;

#[derive(Debug, PartialEq, Clone, Copy, Eq, Ord, PartialOrd)]
pub enum Event<M> {
    MessageDelivered {
        from: u32,
        to: u32,
        msg: M,
    },
    TimerFired {
        node: u32,
    },
    StatePersisted {
        node: u32,
        current_term: u64,
        voted_for: Option<u32>,
        // kind = false-if not for vote grant; kind = true if for vote grant
        kind: bool,
    },
    NodeCrashed {
        node: u32,
    },
    NodeRestarted {
        node: u32,
        current_term: u64,
        voted_for: Option<u32>,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum Action<M> {
    Send {
        to: u32,
        msg: M,
    },
    SetTimer {
        min_delay: u64,
        max_delay: u64,
    },
    Persist {
        current_term: u64,
        voted_for: Option<u32>,
        kind: bool,
    },
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
            Event::StatePersisted {
                node,
                current_term,
                voted_for,
                kind,
            } => *node,
            Event::NodeCrashed { node } => *node,
            Event::NodeRestarted {
                node,
                current_term,
                voted_for,
            } => *node,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub struct RaftNode {
    pub role: Role,
    pub current_term: u64,
    voted_for: Option<u32>,
    // log
    pub votes: u32,
    peers: Vec<u32>,
}

impl RaftNode {
    pub fn new(peers: Vec<u32>) -> Self {
        Self {
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            votes: 0,
            peers,
        }
    }
}

#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug)]
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
                    let mut actions = vec![];
                    actions.push(Action::SetTimer {
                        min_delay: HEARTBEAT_TIMEOUT,
                        max_delay: HEARTBEAT_TIMEOUT,
                    });
                    self.push_to_peers(
                        &mut actions,
                        RPC::AppendEntries {
                            term: self.current_term,
                        },
                    );

                    return actions;

                    // return vec![];
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

                self.persist(&mut actions, self.current_term, self.voted_for, false);
                self.start_timeout(&mut actions);
                self.push_to_peers(&mut actions, request_vote_rpc);

                actions
            }
            Event::MessageDelivered { from, to, msg } => match msg {
                RPC::RequestVote { term, candidate_id } => {
                    let mut actions = vec![];

                    if term < &self.current_term
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

                    self.persist(&mut actions, *term, Some(*candidate_id), true);

                    actions

                    // actions.push(Action::Send {
                    //     to: *candidate_id,
                    //     msg: RPC::RequestVoteReply {
                    //         term: self.current_term,
                    //         vote_granted: true,
                    //     },
                    // });
                    //
                    // actions
                }
                RPC::RequestVoteReply { term, vote_granted } => {
                    let mut actions = vec![];

                    if term > &self.current_term {
                        self.current_term = *term;
                        self.voted_for = None;
                        self.role = Role::Follower;
                        self.votes = 0;

                        self.persist(&mut actions, *term, None, false);
                        self.start_timeout(&mut actions);

                        return actions;
                    }

                    if *term == self.current_term && *vote_granted && self.role == Role::Candidate {
                        self.votes += 1;
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
                    }
                    actions
                }
                RPC::AppendEntries { term } => {
                    if *term < self.current_term {
                        return vec![];
                    }
                    let mut actions = vec![];

                    if *term > self.current_term {
                        self.role = Role::Follower;
                        self.current_term = *term;
                        self.votes = 0;
                        self.voted_for = None;
                        self.persist(&mut actions, *term, None, false);
                    }

                    // println!("{:?}", &self);
                    self.start_timeout(&mut actions);

                    actions
                }
            },
            Event::StatePersisted {
                node,
                current_term,
                voted_for,
                kind,
            } => {
                if !kind || self.current_term != *current_term || self.voted_for != *voted_for {
                    return vec![];
                }
                let voted_for = voted_for.expect("RequestVoteReply recepient undefined!");
                let actions = vec![Action::Send {
                    to: voted_for,
                    msg: RPC::RequestVoteReply {
                        term: *current_term,
                        vote_granted: true,
                    },
                }];

                actions
            }
            Event::NodeRestarted {
                node,
                current_term,
                voted_for,
            } => {
                self.current_term = *current_term;
                self.voted_for = *voted_for;
                self.role = Role::Follower;
                self.votes = 0;

                let mut actions = vec![];
                self.start_timeout(&mut actions);

                actions
            }
            Event::NodeCrashed { node } => {
                panic!("NodeCrashed handling inside raft-core!");
            }
        }
    }
}

impl RaftNode {
    pub fn push_to_peers(&self, actions: &mut Vec<Action<RPC>>, msg: RPC) {
        for &peer in &self.peers {
            actions.push(Action::Send { to: peer, msg });
        }
    }

    pub fn start_timeout(&self, actions: &mut Vec<Action<RPC>>) {
        actions.push(Action::SetTimer {
            min_delay: MIN_ELECTION_TIMEOUT,
            max_delay: MAX_ELECTION_TIMEOUT,
        });
    }

    pub fn persist(
        &self,
        actions: &mut Vec<Action<RPC>>,
        current_term: u64,
        voted_for: Option<u32>,
        kind: bool,
    ) {
        actions.push(Action::Persist {
            current_term,
            voted_for,
            kind,
        });
    }
}
