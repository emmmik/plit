const MIN_ELECTION_TIMEOUT: u64 = 150;
const MAX_ELECTION_TIMEOUT: u64 = 300;
const HEARTBEAT_TIMEOUT: u64 = 50;
const NODE_NUMBER: u32 = 3;

#[derive(Debug, PartialEq, Clone, Eq, Ord, PartialOrd)]
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
        log: RaftLog,
        // kind = false-if not for vote grant; kind = true if for vote grant
        kind: Option<ReplyKind>,
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

#[derive(Clone, Debug)]
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
        log: RaftLog,
        kind: Option<ReplyKind>,
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
            Event::MessageDelivered { to, .. } => *to,
            Event::StatePersisted { node, .. } => *node,
            Event::NodeCrashed { node } => *node,
            Event::NodeRestarted { node, .. } => *node,
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
    pub log: RaftLog,
    pub votes: u32,
    peers: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogEntry {
    SET { x: u64, term: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RaftLog {
    log: Vec<LogEntry>,
}

impl Default for RaftLog {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftLog {
    pub fn new() -> Self {
        Self { log: Vec::new() }
    }

    pub fn append(&mut self, entry: LogEntry) -> usize {
        self.log.push(entry);
        self.log.len()
    }

    pub fn term_at(&self, index: usize) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index > self.log.len() {
            return None;
        }
        let LogEntry::SET { term, .. } = self.log[index - 1];
        Some(term)
    }

    pub fn last(&self) -> (usize, u64) {
        if self.log.is_empty() {
            return (0, 0);
        }
        let LogEntry::SET { term, .. } = self.log.last().unwrap();
        (self.log.len(), *term)
    }

    // Overwrite everything AFTER index
    pub fn overwrite(&mut self, index: usize, entries: &[LogEntry]) {
        while self.log.len() > index {
            self.log.pop();
        }
        for entry in entries {
            self.log.push(*entry);
        }
    }

    // Return the entries AFTER index (needed for AppendEntries RPC)
    pub fn return_after(&self, index: usize) -> &[LogEntry] {
        &self.log[index..]
    }
}

impl RaftNode {
    pub fn new(peers: Vec<u32>) -> Self {
        Self {
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: RaftLog::new(),
            votes: 0,
            peers,
        }
    }
}

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord, Debug)]
pub enum RPC {
    AppendEntries {
        term: u64,
        prev_log_index: usize,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_id: u32,
    },
    AppendEntriesReply {
        term: u64,
        success: bool,
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

// VoteReply -> new current_term and voted_for are persisted, proceed to grant a vote
// AppendReply -> current_term and log[] are persisted, proceed to give a positive AppendEntries reply
// VoteRequest -> current_term and voted_for are persisted, proceed to ask other nodes for votes
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReplyKind {
    VoteReply,
    AppendReply { leader_id: u32 },
    VoteRequest,
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
                    let (prev_log_index, prev_log_term) = self.log.last();
                    self.push_to_peers(
                        &mut actions,
                        RPC::AppendEntries {
                            term: self.current_term,
                            entries: vec![],
                            leader_id: *node,
                            prev_log_index,
                            prev_log_term,
                        },
                    );

                    return actions;
                }

                // Regardless if the node is a candidate or a follower, a new election has to be
                // started (with the current node prmoting itself to a candidate)
                self.role = Role::Candidate;

                self.current_term += 1;
                self.voted_for = Some(*node);
                self.votes = 1;

                let mut actions = vec![];

                self.persist(
                    &mut actions,
                    self.current_term,
                    self.voted_for,
                    self.log.clone(),
                    Some(ReplyKind::VoteRequest),
                );
                self.start_timeout(&mut actions);
                // self.push_to_peers(&mut actions, request_vote_rpc);

                actions
            }
            Event::MessageDelivered { msg, to, .. } => match msg {
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

                    self.persist(
                        &mut actions,
                        *term,
                        Some(*candidate_id),
                        self.log.clone(),
                        Some(ReplyKind::VoteReply),
                    );

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

                        self.persist(
                            &mut actions,
                            self.current_term,
                            self.voted_for,
                            self.log.clone(),
                            None,
                        );
                        self.start_timeout(&mut actions);

                        return actions;
                    }

                    if *term == self.current_term && *vote_granted && self.role == Role::Candidate {
                        self.votes += 1;
                        if self.votes > NODE_NUMBER / 2 {
                            self.role = Role::Leader;
                            let (prev_log_index, prev_log_term) = self.log.last();

                            self.push_to_peers(
                                &mut actions,
                                RPC::AppendEntries {
                                    term: self.current_term,
                                    entries: self.log.return_after(prev_log_index).to_vec(),
                                    leader_id: *to,
                                    prev_log_index,
                                    prev_log_term,
                                    // leader_id: node,
                                },
                            );
                        }
                    }
                    actions
                }
                RPC::AppendEntries {
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_id,
                } => {
                    let mut actions = vec![];

                    if *term < self.current_term
                        || self.log.term_at(*prev_log_index) != Some(*prev_log_term)
                    {
                        actions.push(Action::Send {
                            to: *leader_id,
                            msg: RPC::AppendEntriesReply {
                                term: self.current_term,
                                success: false,
                            },
                        });
                        return actions;
                    }

                    self.role = Role::Follower;
                    if *term > self.current_term {
                        self.current_term = *term;
                        self.votes = 0;
                        self.voted_for = None;
                    }
                    self.log.overwrite(*prev_log_index, entries);
                    self.persist(
                        &mut actions,
                        self.current_term,
                        self.voted_for,
                        self.log.clone(),
                        Some(ReplyKind::AppendReply {
                            leader_id: *leader_id,
                        }),
                    );

                    // println!("{:?}", &self);
                    self.start_timeout(&mut actions);

                    actions
                }
                RPC::AppendEntriesReply { term, success } => {
                    vec![]
                }
            },
            Event::StatePersisted {
                current_term,
                voted_for,
                kind,
                log,
                node,
            } => {
                if kind.is_none()
                    || self.current_term != *current_term
                    || self.voted_for != *voted_for
                    || self.log != *log
                {
                    return vec![];
                }
                let mut actions = vec![];
                match kind {
                    &Some(ReplyKind::VoteRequest) => {
                        let request_vote_rpc = RPC::RequestVote {
                            term: self.current_term,
                            candidate_id: *node,
                        };
                        self.push_to_peers(&mut actions, request_vote_rpc);
                    }
                    &Some(ReplyKind::AppendReply { leader_id }) => {
                        actions.push(Action::Send {
                            to: leader_id,
                            msg: RPC::AppendEntriesReply {
                                term: self.current_term,
                                success: true,
                            },
                        });
                    }
                    &Some(ReplyKind::VoteReply) => {
                        let voted_for = voted_for.expect("RequestVoteReply recepient undefined!");
                        actions.push(Action::Send {
                            to: voted_for,
                            msg: RPC::RequestVoteReply {
                                term: *current_term,
                                vote_granted: true,
                            },
                        });
                    }
                    None => {}
                }

                actions
            }
            Event::NodeRestarted {
                current_term,
                voted_for,
                ..
            } => {
                self.current_term = *current_term;
                self.voted_for = *voted_for;
                self.role = Role::Follower;
                self.votes = 0;

                let mut actions = vec![];
                self.start_timeout(&mut actions);

                actions
            }
            Event::NodeCrashed { .. } => {
                panic!("NodeCrashed handling inside raft-core!");
            }
        }
    }
}

impl RaftNode {
    pub fn push_to_peers(&self, actions: &mut Vec<Action<RPC>>, msg: RPC) {
        for &peer in &self.peers {
            actions.push(Action::Send {
                to: peer,
                msg: msg.clone(),
            });
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
        log: RaftLog,
        kind: Option<ReplyKind>,
    ) {
        actions.push(Action::Persist {
            current_term,
            voted_for,
            log,
            kind,
        });
    }
}
