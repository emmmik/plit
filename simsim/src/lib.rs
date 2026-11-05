use std::{cmp::Reverse, collections::BinaryHeap};

#[derive(Debug, PartialEq, Clone, Copy, Eq, Ord, PartialOrd)]
pub enum Event {
    MessageDelivered { from: u32, to: u32, value: u64 },
    TimerFired { node: u32 },
}

#[derive(Clone, Copy)]
pub enum Action {
    Send { to: u32, value: u64 },
}

#[derive(Debug)]
pub struct Node {
    peers: Vec<u32>,
    value: u64,
}

impl Node {
    pub fn handle(&mut self, event: Event) -> Vec<Action> {
        let mut actions: Vec<Action> = Vec::new();
        match event {
            Event::MessageDelivered { from, to, value } => {
                if self.value < value {
                    self.value = value;
                    for &peer in &self.peers {
                        actions.push(Action::Send {
                            to: peer,
                            value: self.value,
                        });
                    }
                }
                actions
            }
            Event::TimerFired { node } => {
                for &peer in &self.peers {
                    actions.push(Action::Send {
                        to: peer,
                        value: self.value,
                    });
                }
                actions
            }
        }
    }
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

#[derive(Debug)]
pub struct World {
    now_ms: u64,
    next_event_id: u64,
    agenda: BinaryHeap<Reverse<(u64, u64, Event)>>,
    trace: Vec<(u64, Event)>,
    nodes: Vec<Node>,
    generator: XorShiftRng,
    seed: u64,
    config: ScenarioConfig,
}

#[derive(Debug)]
pub struct Checker {
    values: Vec<u64>,
}

impl Checker {
    pub fn new(world: &World) -> Self {
        let mut values = vec![];
        for node in &world.nodes {
            values.push(node.value);
        }
        Self { values }
    }

    pub fn check(&mut self, world: &World) -> Result<(), String> {
        let mut new_values = vec![];
        for node in &world.nodes {
            new_values.push(node.value);
        }
        if new_values.len() != self.values.len() {
            return Err(format!(
                "Different value vector lengths! {} vs {}",
                self.values.len(),
                new_values.len(),
            ));
        }
        for (node_idx, (&new_value, &old_value)) in new_values.iter().zip(&self.values).enumerate()
        {
            if new_value < old_value {
                return Err(format!(
                    "Node values decreas! Node {}: {} -> {}",
                    node_idx, old_value, new_value
                ));
            }
            if world.agenda.is_empty() && new_value != new_values[0] {
                return Err(format!(
                    "Agenda is empty and values are not equal! Node values: {:?}",
                    new_values
                ));
            }
        }
        self.values = new_values;
        Ok(())
    }
}

impl World {
    pub fn new(seed: u64, config: ScenarioConfig) -> Self {
        let nodes = vec![
            Node {
                peers: vec![1, 2],
                value: 0,
            },
            Node {
                peers: vec![0, 2],
                value: 0,
            },
            Node {
                peers: vec![0, 1],
                value: 5,
            },
        ];

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

    pub fn step(&mut self) -> Result<Event, &'static str> {
        match self.agenda.is_empty() {
            false => {
                let Reverse(agenda_element) = self.agenda.pop().unwrap();
                let (due_time, _, next_event) = agenda_element;
                self.now_ms = due_time;

                let new_actions = match next_event {
                    Event::TimerFired { node } => self.nodes[node as usize].handle(next_event),
                    Event::MessageDelivered { from, to, value } => {
                        self.nodes[to as usize].handle(next_event)
                    }
                };
                let action_node = match next_event {
                    Event::TimerFired { node } => node,
                    Event::MessageDelivered { from, to, value } => to,
                };

                for &action in new_actions.iter() {
                    let is_dropped = self.generator.next_state() % 100;
                    if is_dropped >= self.config.drop_percent {
                        self.handle_action(action_node, action);
                    }
                }

                self.trace.push((due_time, next_event));

                Ok(next_event)
            }
            true => Err("Can't execute a step(): the agenda is empty!"),
        }
    }

    pub fn run(&mut self, until: u64, max_steps: u32, checker: &mut Checker) -> Result<(), String> {
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

    pub fn insert_into_agenda(&mut self, due_time: u64, event: Event) {
        self.next_event_id += 1;
        self.agenda
            .push(Reverse((due_time, self.next_event_id, event)));
    }

    pub fn handle_action(&mut self, node: u32, action: Action) {
        match action {
            Action::Send { to, value } => {
                let new_event = Event::MessageDelivered {
                    from: node,
                    to,
                    value,
                };
                // A demo delay in delivery time is put for testing
                let delay = self.generator.next_state() % self.config.max_delay + 1;
                self.insert_into_agenda(self.now_ms + delay, new_event);
            }
        }
    }
}

impl Event {
    pub fn return_formatted_event(&self) -> String {
        match self {
            Event::MessageDelivered { from, to, value } => {
                format!(
                    "A message was delivered from node {} to node {} with value {}. ",
                    from, to, value
                )
            }
            Event::TimerFired { node } => {
                format!("The timer has fired on node {}. ", node)
            }
        }
    }
}

pub fn print_event_list(events: &[(u64, Event)]) {
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
mod tests {
    use super::*;

    #[test]
    fn event_formatting() {
        let events: Vec<Event> = vec![
            Event::MessageDelivered {
                from: 1,
                to: 2,
                value: 10,
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

    #[test]
    fn world_step() {
        let config = ScenarioConfig {
            max_delay: 10,
            drop_percent: 0,
        };
        let mut world = World::new(43, config);

        world.insert_into_agenda(
            100,
            Event::MessageDelivered {
                from: 1,
                to: 2,
                value: 10,
            },
        );

        world.insert_into_agenda(
            300,
            Event::MessageDelivered {
                from: 1,
                to: 2,
                value: 5,
            },
        );

        world.insert_into_agenda(200, Event::TimerFired { node: 2 });

        let _ = world.step();

        assert_eq!(world.now_ms, 100);
        assert_eq!(
            world.trace[0],
            (
                100,
                Event::MessageDelivered {
                    from: 1,
                    to: 2,
                    value: 10
                }
            )
        );

        _ = world.step();
        assert!(world.now_ms > 100 && world.now_ms <= 110);
    }

    #[test]
    fn world_run_determinism() {
        let config = ScenarioConfig {
            max_delay: 300,
            drop_percent: 20,
        };
        let mut worlds: Vec<World> = Vec::new();

        worlds.push(World::new(22, config));
        worlds.push(World::new(22, config));
        worlds.push(World::new(25, config));

        for world in &mut worlds {
            let mut checker = Checker::new(world);
            world.insert_into_agenda(100, Event::TimerFired { node: 2 });
            let _ = world.run(10000, 1000, &mut checker);
        }

        assert_eq!(worlds[0].trace, worlds[1].trace);
        assert_ne!(worlds[1].trace, worlds[2].trace);
        assert_ne!(worlds[0].trace, worlds[2].trace);
        assert!(worlds[0].agenda.is_empty());
    }

    #[test]
    fn world_run_convergence() {
        let config = ScenarioConfig {
            max_delay: 300,
            drop_percent: 0,
        };
        let mut world1 = World::new(42, config);
        world1.insert_into_agenda(100, Event::TimerFired { node: 0 });
        world1.insert_into_agenda(100, Event::TimerFired { node: 1 });
        world1.insert_into_agenda(100, Event::TimerFired { node: 2 });

        let mut checker = Checker::new(&world1);

        let outcome = world1.run(10000, 1000, &mut checker);
        assert!(outcome.is_ok(), "{}", outcome.unwrap_err());

        assert_eq!(world1.nodes[1].value, 5);
        assert_eq!(world1.nodes[2].value, 5);
        assert_eq!(world1.nodes[0].value, 5);
    }

    #[test]
    fn seed_sweep_no_drop() {
        let config = ScenarioConfig {
            max_delay: 300,
            drop_percent: 0,
        };
        for seed in 1..1000 {
            let mut world = World::new(seed, config);
            world.insert_into_agenda(100, Event::TimerFired { node: 2 });

            let mut checker = Checker::new(&world);

            let outcome = world.run(10000, 1000, &mut checker);
            assert!(outcome.is_ok(), "{}", outcome.unwrap_err());
        }
    }

    #[test]
    fn seed_sweep_high_drop() {
        let config = ScenarioConfig {
            max_delay: 300,
            drop_percent: 10,
        };

        let mut count_error_seeds = 0;

        for seed in 1..1000 {
            let mut world = World::new(seed, config);
            world.insert_into_agenda(100, Event::TimerFired { node: 2 });

            let mut checker = Checker::new(&world);

            let outcome = world.run(10000, 1000, &mut checker);
            if outcome.is_err() {
                count_error_seeds += 1;
            }
        }
        println!(
            "Percentage of seeds that produce an error on a {}% drop seed sweep with {} ms delay: {} / 1000 = {}%!",
            config.drop_percent,
            config.max_delay,
            count_error_seeds,
            (count_error_seeds as f64) / 1000.0
        );
        assert!(count_error_seeds > 0);
    }
}
