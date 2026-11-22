pub mod gossip;
pub mod raft;
use raft_core::{Action, Event, Protocol};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
    fmt::Debug,
};

pub trait Checker<N: Protocol> {
    fn check(&mut self, world: &World<N>) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub struct XorShiftRng {
    state: u64,
}

impl XorShiftRng {
    pub fn next_state(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 5;
        self.state = x;
        self.state
    }
}

#[derive(Clone)]
pub struct NodeState {
    current_term: u64,
    voted_for: Option<u32>,
}

pub struct World<N: Protocol> {
    now_ms: u64,
    next_event_id: u64,
    agenda: BinaryHeap<Reverse<(u64, u64, Event<N::Msg>)>>,
    trace: Vec<(u64, Event<N::Msg>)>,
    nodes: Vec<N>,
    node_states: Vec<NodeState>,
    node_is_up: Vec<bool>,
    last_timer_id: Vec<u64>,
    ghost_timer_cnt: Vec<u64>,
    // generator: XorShiftRng,
    node_timeouts: VecDeque<VecDeque<u64>>,
    delays: VecDeque<u64>,
    drops: VecDeque<bool>,
    node_downtimes: VecDeque<u64>,
    seed: u64,
    config: ScenarioConfig,
}

impl<N: Protocol + std::fmt::Debug> World<N>
where
    N::Msg: Ord + std::fmt::Debug,
{
    pub fn new(seed: u64, config: ScenarioConfig, nodes: Vec<N>) -> Self {
        let n = nodes.len();
        let mut generator = XorShiftRng { state: seed };

        // Order: node timeouts (0..n) -> delays -> drops -> ...

        let mut node_timeouts = VecDeque::new();
        for _ in 0..n {
            let mut current_timeouts = VecDeque::new();
            for _ in 0..config.preseed_size {
                let next_timeout = generator.next_state();
                current_timeouts.push_back(next_timeout);
            }
            node_timeouts.push_back(current_timeouts);
        }

        let mut delays = VecDeque::new();
        for _ in 0..config.preseed_size {
            delays.push_back(generator.next_state() % config.max_delay + 1);
        }

        let mut drops = VecDeque::new();
        for _ in 0..config.preseed_size {
            let current_drop = generator.next_state() % 100;
            if current_drop >= config.drop_percent {
                drops.push_back(false);
            } else {
                drops.push_back(true);
            }
        }

        // First crash should be anywhere between 500 and 2000 ms to ensure that something actually
        // happened before the first crash
        let mut crash_time = 500 + generator.next_state() % (2000 - 500);
        let mut crashes = vec![];
        let mut node_downtimes = VecDeque::new();

        while crash_time < config.until {
            let crashed_node = (generator.next_state() as u32) % config.node_count;

            // The downtime should be 1 - 3 full election timeouts, so the cluster can still try
            // electing without it
            let downtime = 300 + generator.next_state() % (800 - 300);
            node_downtimes.push_back(downtime);

            // The gap between crashes should be 3 - 5 election timeouts, so each crash is at a
            // different time of an electino
            let crash_gap = 800 + generator.next_state() % (1500 - 800);

            crashes.push((crash_time, crashed_node, downtime));

            crash_time += crash_gap;
        }

        let node_states = vec![
            NodeState {
                current_term: 0,
                voted_for: None,
            };
            n
        ];

        let mut world = Self {
            now_ms: 0,
            next_event_id: 0,
            agenda: BinaryHeap::new(),
            trace: vec![],
            nodes,
            node_states,
            node_is_up: vec![true; n],
            last_timer_id: vec![0; n],
            ghost_timer_cnt: vec![0; n],
            // generator: XorShiftRng { state: seed },
            node_timeouts,
            delays,
            drops,
            node_downtimes,
            seed,
            config,
        };
        if !config.seed_crashes {
            return world;
        }
        for (crash_time, crashed_node, _) in crashes {
            world.insert_into_agenda(crash_time, Event::NodeCrashed { node: crashed_node });
        }

        world
    }

    pub fn step(&mut self) -> Result<(), &'static str> {
        match self.agenda.is_empty() {
            false => {
                let Reverse(agenda_element) = self.agenda.pop().unwrap();
                let (due_time, id, mut next_event) = agenda_element;

                if let Event::TimerFired { node } = next_event
                    && id < self.last_timer_id[node as usize]
                {
                    self.ghost_timer_cnt[node as usize] += 1;
                    return Ok(());
                }

                self.now_ms = due_time;

                // Node crash handling
                if let Event::NodeCrashed { node } = next_event {
                    if !self.node_is_up[node as usize] {
                        return Ok(());
                    }
                    self.node_is_up[node as usize] = false;
                    let downtime = self
                        .node_downtimes
                        .pop_front()
                        .expect("The preseeded node_downtimes vector is exhausted!");
                    self.insert_into_agenda(
                        self.now_ms + downtime,
                        Event::NodeRestarted {
                            node,
                            current_term: self.node_states[node as usize].current_term,
                            voted_for: self.node_states[node as usize].voted_for,
                        },
                    );
                    self.trace.push((due_time, next_event));
                    return Ok(());
                }

                let action_node = next_event.target_node();

                if let Event::NodeRestarted { node, .. } = next_event {
                    self.node_is_up[node as usize] = true;

                    // The event must be rebuilt, as if any updates were written to the disk while
                    // the node was down, it is still guaranteed that they have been persisted
                    next_event = Event::NodeRestarted {
                        node,
                        current_term: self.node_states[node as usize].current_term,
                        voted_for: self.node_states[node as usize].voted_for,
                    };
                }

                if let Event::StatePersisted {
                    node,
                    current_term,
                    voted_for,
                    ..
                } = next_event
                {
                    self.node_states[node as usize].current_term = current_term;
                    self.node_states[node as usize].voted_for = voted_for;
                }

                if !self.node_is_up[action_node as usize] {
                    return Ok(());
                }

                let new_actions = self.nodes[action_node as usize].handle(&next_event);
                for action in new_actions {
                    if let Action::SetTimer { .. } = action {
                        self.handle_action(action_node, action);
                    } else if let Action::Persist { .. } = action {
                        self.handle_action(action_node, action);
                    } else {
                        let is_dropped = self.drops.pop_front();
                        match is_dropped {
                            Some(value) => {
                                if !value {
                                    self.handle_action(action_node, action);
                                }
                            }
                            None => {
                                panic!("The preseeded drops vector is exhausted!");
                            }
                        }
                    }
                }

                self.trace.push((due_time, next_event));
                Ok(())
            }
            true => Err("Can't execute a step(): the agenda is empty!"),
        }
    }

    pub fn run(&mut self, checker: &mut impl Checker<N>) -> Result<(), String> {
        let mut step_idx = 1;
        while !self.agenda.is_empty() && self.agenda.peek().unwrap().0.0 <= self.config.until {
            if step_idx > self.config.max_steps {
                return Err(format!(
                    "Over the step budget at time {} ms! {} more elements are in the agenda.",
                    self.now_ms,
                    self.agenda.len(),
                ));
            }

            let _ = self.step();
            // if !checker.check(self) {
            //     return false;
            // }
            if let Err(message) = checker.check(self) {
                return Err(format!(
                    "seed: {}, max_delay: {}, drop_percent: {}, time: {}, step {}: {}",
                    self.seed,
                    self.config.max_delay,
                    self.config.drop_percent,
                    self.now_ms,
                    step_idx,
                    message
                ));
            }
            step_idx += 1;
        }
        Ok(())
    }

    pub fn insert_into_agenda(&mut self, due_time: u64, event: Event<N::Msg>) {
        self.next_event_id += 1;
        if let Event::TimerFired { node } = event {
            self.last_timer_id[node as usize] = self.next_event_id;
        }
        self.agenda
            .push(Reverse((due_time, self.next_event_id, event)));
    }

    pub fn handle_action(&mut self, node: u32, action: Action<N::Msg>) {
        match action {
            Action::Send { to, msg } => {
                let new_event = Event::MessageDelivered {
                    from: node,
                    to,
                    msg,
                };
                // A demo delay in delivery time is put for testing
                let delay = self.delays.pop_front();
                match delay {
                    Some(value) => {
                        self.insert_into_agenda(self.now_ms + value, new_event);
                    }
                    None => {
                        panic!("The preseeded delays vector is exhausted!");
                    }
                }
            }
            Action::SetTimer {
                min_delay,
                max_delay,
            } => {
                if min_delay == max_delay {
                    self.insert_into_agenda(self.now_ms + min_delay, Event::TimerFired { node });
                    return;
                }
                let raw_timeout = self.node_timeouts[node as usize].pop_front();
                match raw_timeout {
                    Some(value) => {
                        self.insert_into_agenda(
                            self.now_ms + (min_delay + value % (max_delay - min_delay)),
                            Event::TimerFired { node },
                        );
                    }
                    None => {
                        panic!(
                            "The preseeded timeout vector for node {} is exhausted!",
                            &node
                        );
                    }
                }
            }
            Action::Persist {
                current_term,
                voted_for,
                kind,
            } => {
                self.insert_into_agenda(
                    self.now_ms + 30,
                    Event::StatePersisted {
                        node,
                        current_term,
                        voted_for,
                        kind,
                    },
                );
            }
        }
    }
    pub fn debug_trace(&self) {
        let start = self.trace.len().saturating_sub(30);
        for (i, (_due_time, event)) in self.trace[start..].iter().enumerate() {
            println!("{}) {}", i + 1, return_formatted_event(event));
        }
    }
}

fn return_formatted_event<M>(event: &Event<M>) -> String
where
    M: Debug,
{
    match event {
        Event::MessageDelivered { from, to, msg } => {
            format!(
                "A message was delivered from node {} to node {} with value {:?}. ",
                from, to, msg
            )
        }
        Event::TimerFired { node } => {
            format!("The timer has fired on node {}. ", node)
        }
        Event::StatePersisted {
            node,
            current_term,
            voted_for,
            ..
        } => {
            format!(
                "State was persisted on node {}: currentTerm = {}, votedFor = {:?}",
                node, current_term, voted_for
            )
        }
        Event::NodeCrashed { node } => {
            format!("Node {} has crashed", node)
        }
        Event::NodeRestarted {
            node,
            current_term,
            voted_for,
        } => {
            format!(
                "Node {} has restarted: currentTerm = {}, votedFor = {:?}",
                node, current_term, voted_for
            )
        }
    }
}

pub fn return_event_list(events: &[(u64, Event<u64>)]) -> String {
    let mut event_list = String::new();
    for (now_ms, event) in events.iter() {
        event_list += &format!(
            "The time is: {}\n{}\n",
            now_ms,
            return_formatted_event(event)
        );
    }
    event_list
}

#[derive(Debug, Clone, Copy)]
pub struct ScenarioConfig {
    max_delay: u64,
    drop_percent: u64,
    until: u64,
    max_steps: u32,
    preseed_size: usize,
    node_count: u32,
    seed_crashes: bool,
}

#[cfg(test)]
mod harness_tests {
    use super::*;

    #[test]
    fn event_formatting() {
        let events: Vec<Event<u64>> = vec![
            Event::MessageDelivered {
                from: 1,
                to: 2,
                msg: 10,
            },
            Event::TimerFired { node: 3 },
        ];
        let mut events_string: String = String::new();
        for i in events.iter() {
            events_string.push_str(&return_formatted_event(i));
        }
        assert_eq!(
            events_string,
            "A message was delivered from node 1 to node 2 with value 10. The timer has fired on node 3. "
        );
    }
}
