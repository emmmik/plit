pub mod gossip;
use std::{cmp::Reverse, collections::BinaryHeap, fmt::Debug};

#[derive(Debug, PartialEq, Clone, Copy, Eq, Ord, PartialOrd)]
pub enum Event<M> {
    MessageDelivered { from: u32, to: u32, msg: M },
    TimerFired { node: u32 },
}

#[derive(Clone, Copy)]
pub enum Action<M> {
    Send { to: u32, msg: M },
}

pub trait Protocol {
    type Msg;

    fn handle(&mut self, e: &Event<Self::Msg>) -> Vec<Action<Self::Msg>>;
}

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
    generator: XorShiftRng,
    seed: u64,
    config: ScenarioConfig,
}

impl<N: Protocol> World<N>
where
    N::Msg: Ord,
{
    pub fn new(seed: u64, config: ScenarioConfig, nodes: Vec<N>) -> Self {
        Self {
            now_ms: 0,
            next_event_id: 0,
            agenda: BinaryHeap::new(),
            trace: vec![],
            nodes,
            generator: XorShiftRng { state: seed },
            seed,
            config,
        }
    }

    pub fn step(&mut self) -> Result<(), &'static str> {
        match self.agenda.is_empty() {
            false => {
                let Reverse(agenda_element) = self.agenda.pop().unwrap();
                let (due_time, _, next_event) = agenda_element;
                self.now_ms = due_time;

                let action_node = next_event.target_node();
                let new_actions = self.nodes[action_node as usize].handle(&next_event);
                for action in new_actions {
                    let is_dropped = self.generator.next_state() % 100;
                    if is_dropped >= self.config.drop_percent {
                        self.handle_action(action_node, action);
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
                let delay = self.generator.next_state() % self.config.max_delay + 1;
                self.insert_into_agenda(self.now_ms + delay, new_event);
            }
        }
    }
}

impl<T> Event<T> {
    pub fn target_node(&self) -> u32 {
        match self {
            Event::TimerFired { node } => *node,
            Event::MessageDelivered { from, to, msg } => *to,
        }
    }

    pub fn return_formatted_event(&self) -> String
    where
        T: Debug,
    {
        match self {
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
}

pub fn print_event_list(events: &[(u64, Event<u64>)]) {
    for (now_ms, event) in events.iter() {
        println!("The time is: {}", now_ms);
        println!("{}", event.return_formatted_event());
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ScenarioConfig {
    max_delay: u64,
    drop_percent: u64,
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
            events_string.push_str(&i.return_formatted_event());
        }
        assert_eq!(
            events_string,
            "A message was delivered from node 1 to node 2 with value 10. The timer has fired on node 3. "
        );
    }
}
