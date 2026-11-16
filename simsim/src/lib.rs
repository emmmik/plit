pub mod gossip;
use raft_core::{Action, Event, Protocol, RaftNode, Role};
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

pub struct World<N: Protocol> {
    now_ms: u64,
    next_event_id: u64,
    agenda: BinaryHeap<Reverse<(u64, u64, Event<N::Msg>)>>,
    trace: Vec<(u64, Event<N::Msg>)>,
    nodes: Vec<N>,
    last_timer_id: Vec<u64>,
    ghost_timer_cnt: Vec<u64>,
    // generator: XorShiftRng,
    node_timeouts: VecDeque<VecDeque<u64>>,
    delays: VecDeque<u64>,
    drops: VecDeque<bool>,
    seed: u64,
    config: ScenarioConfig,
}

impl<N: Protocol> World<N>
where
    N::Msg: Ord,
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

        Self {
            now_ms: 0,
            next_event_id: 0,
            agenda: BinaryHeap::new(),
            trace: vec![],
            nodes,
            last_timer_id: vec![0; n],
            ghost_timer_cnt: vec![0; n],
            // generator: XorShiftRng { state: seed },
            node_timeouts,
            delays,
            drops,
            seed,
            config,
        }
    }

    pub fn step(&mut self) -> Result<(), &'static str> {
        match self.agenda.is_empty() {
            false => {
                let Reverse(agenda_element) = self.agenda.pop().unwrap();
                let (due_time, id, next_event) = agenda_element;

                if let Event::TimerFired { node } = next_event {
                    if id < self.last_timer_id[node as usize] {
                        self.ghost_timer_cnt[node as usize] += 1;
                        return Ok(());
                    }
                }

                self.now_ms = due_time;

                let action_node = next_event.target_node();
                let new_actions = self.nodes[action_node as usize].handle(&next_event);
                for action in new_actions {
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

                self.trace.push((due_time, next_event));
                Ok(())
            }
            true => Err("Can't execute a step(): the agenda is empty!"),
        }
    }

    pub fn run(
        &mut self,
        until: u64,
        max_steps: u32,
        checker: &mut impl Checker<N>,
    ) -> Result<(), String> {
        let mut step_idx = 1;
        while !self.agenda.is_empty() && self.agenda.peek().unwrap().0.0 <= until {
            if step_idx > max_steps {
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
    }
}

pub fn return_event_list(events: &[(u64, Event<u64>)]) -> String {
    let mut event_list = String::new();
    for (now_ms, event) in events.iter() {
        // println!("The time is: {}", now_ms);
        // println!("{}", return_formatted_event(event));
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
    preseed_size: usize,
}

#[derive(Debug)]
pub struct LinearChecker {}

impl LinearChecker {
    pub fn new(world: &World<RaftNode>) -> Self {
        Self {}
    }
}
impl Checker<RaftNode> for LinearChecker {
    fn check(&mut self, world: &World<RaftNode>) -> Result<(), String> {
        let mut cnt_leaders = 0;
        for node in &world.nodes {
            if node.role == Role::Leader {
                cnt_leaders += 1;
            }
        }

        if cnt_leaders > 1 {
            return Err(format!("There are {} leaders!", cnt_leaders));
        }
        Ok(())
    }
}

pub fn build_raft_nodes() -> Vec<RaftNode> {
    vec![
        RaftNode::new(vec![1, 2]),
        RaftNode::new(vec![0, 2]),
        RaftNode::new(vec![0, 1]),
    ]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_election() {
        let config = ScenarioConfig {
            max_delay: 20,
            drop_percent: 0,
            preseed_size: 10000,
        };

        let mut world = World::new(43, config, build_raft_nodes());
        let mut checker = LinearChecker::new(&world);

        world.handle_action(
            0,
            Action::SetTimer {
                min_delay: 150,
                max_delay: 300,
            },
        );
        world.handle_action(
            1,
            Action::SetTimer {
                min_delay: 150,
                max_delay: 300,
            },
        );
        world.handle_action(
            2,
            Action::SetTimer {
                min_delay: 150,
                max_delay: 300,
            },
        );
        // world.insert_into_agenda(0, Event::TimerFired { node: 0 });
        // world.insert_into_agenda(1, Event::TimerFired { node: 1 });
        // world.insert_into_agenda(1, Event::TimerFired { node: 2 });

        let _ = world.run(2000, 100, &mut checker);

        println!("{:?}", world.trace);
        for node in world.nodes.iter() {
            println!("{:?}", node);
        }

        // world.nodes[0].role = Role::Leader;
        assert_eq!(checker.check(&world), Ok(()));

        let mut leader_cnt = 0;
        let mut leader_term = 0;

        for node in world.nodes.iter() {
            if node.role == Role::Leader {
                leader_cnt += 1;
                leader_term = node.current_term;
            }
        }

        assert_eq!(leader_cnt, 1);
        assert!(leader_term > 0);
    }
}
