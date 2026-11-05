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

#[derive(Debug)]
pub struct World {
    now_ms: u64,
    next_event_id: u64,
    agenda: BinaryHeap<Reverse<(u64, u64, Event)>>,
    trace: Vec<(u64, Event)>,
    nodes: Vec<Node>,
}

impl World {
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
                    self.handle_action(action_node, action);
                }

                self.trace.push((due_time, next_event));

                Ok(next_event)
            }
            true => Err("Can't execute a step(): the agenda is empty!"),
        }
    }

    pub fn run(&mut self, until: u64) {
        while !self.agenda.is_empty() && self.agenda.peek().unwrap().0.0 <= until {
            let _ = self.step();
        }
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
                self.insert_into_agenda(self.now_ms + 10, new_event);
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

        let mut world = World {
            now_ms: 0,
            next_event_id: 0,
            agenda: BinaryHeap::new(),
            trace: Vec::new(),
            nodes,
        };

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

        // println!("{:#?}", world);
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
        assert_eq!(world.now_ms, 110);
    }

    #[test]
    fn world_run() {
        let nodes = vec![
            Node {
                peers: vec![1, 2],
                value: 5,
            },
            Node {
                peers: vec![0, 2],
                value: 0,
            },
            Node {
                peers: vec![0, 1],
                value: 0,
            },
        ];

        let mut world = World {
            now_ms: 0,
            next_event_id: 0,
            agenda: BinaryHeap::new(),
            trace: Vec::new(),
            nodes,
        };

        world.insert_into_agenda(100, Event::TimerFired { node: 0 });

        world.run(300);
        assert_eq!(world.nodes[0].value, 5);
        assert_eq!(world.nodes[1].value, 5);
        assert_eq!(world.nodes[2].value, 5);
        assert!(world.agenda.is_empty());
    }
}
