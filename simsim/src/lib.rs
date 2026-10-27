use std::{cmp::Reverse, collections::BinaryHeap};

#[derive(Debug, PartialEq, Clone, Copy, Eq, Ord, PartialOrd)]
pub enum Event {
    MessageDelivered { from: u32, to: u32 },
    TimeJumped { new_time: u64 },
    TimerFired { node: u32 },
}

pub struct World {
    now_ms: u64,
    next_event_id: u64,
    agenda: BinaryHeap<Reverse<(u64, u64, Event)>>,
    trace: Vec<Event>,
}

impl World {
    pub fn step(&mut self) -> Result<Event, &'static str> {
        match self.agenda.is_empty() {
            false => {
                let Reverse(agenda_element) = self.agenda.pop().unwrap();
                let (due_time, id, next_event) = agenda_element;
                self.trace.push(next_event);
                self.now_ms = due_time;

                // When an event triggers another event, increase next_event_id and push here

                Ok(next_event)
            }
            true => Err("Can't execute a step(): the agenda is empty!"),
        }
    }
}

impl Event {
    pub fn return_formatted_event(&self) -> String {
        match self {
            Event::MessageDelivered { from, to } => {
                format!(
                    "A message was delivered from node {} to node {}. ",
                    from, to
                )
            }
            Event::TimeJumped { new_time } => {
                format!("The time has jumped to {}. ", new_time)
            }
            Event::TimerFired { node } => {
                format!("The timer has fired on node {}. ", node)
            }
        }
    }
}

pub fn print_event_list(events: &[Event]) {
    for event in events.iter() {
        println!("{}", event.return_formatted_event());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_formatting() {
        let events: Vec<Event> = vec![
            Event::MessageDelivered { from: 1, to: 2 },
            Event::TimeJumped { new_time: 200 },
            Event::TimerFired { node: 3 },
        ];
        let mut events_string: String = String::new();
        for i in events.iter() {
            events_string.push_str(&i.return_formatted_event());
        }
        assert_eq!(
            events_string,
            "A message was delivered from node 1 to node 2. The time has jumped to 200. The timer has fired on node 3. "
        );
    }

    #[test]
    fn world_step() {
        let mut agenda_events: BinaryHeap<Reverse<(u64, u64, Event)>> = BinaryHeap::new(); /* = vec![
        Event::MessageDelivered { from: 1, to: 2 },
        Event::TimeJumped { new_time: 200 },
        Event::TimerFired { node: 3 },
        ];*/

        agenda_events.push(Reverse((
            100,
            1,
            Event::MessageDelivered { from: 1, to: 2 },
        )));

        agenda_events.push(Reverse((
            300,
            3,
            Event::MessageDelivered { from: 1, to: 2 },
        )));

        agenda_events.push(Reverse((200, 2, Event::TimerFired { node: 3 })));

        let mut world = World {
            now_ms: 0,
            next_event_id: 0,
            agenda: agenda_events,
            trace: Vec::new(),
        };

        world.step();
        assert_eq!(world.now_ms, 100);
        world.step();
        assert_eq!(world.agenda.len(), 1);
        assert_eq!(world.now_ms, 200);
        // world.step();
        // if let Err(e) = world.step() {
        //     println!("Error: {}", e);
        // }
    }
}
