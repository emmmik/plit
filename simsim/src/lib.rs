#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Event {
    MessageDelivered { from: u32, to: u32 },
    TimeJumped { new_time: u32 },
    TimerFired { node: u32 },
}

pub struct World {
    now_ms: u32,
    agenda: Vec<Event>,
    trace: Vec<Event>,
}

impl World {
    pub fn step(&mut self) {
        self.trace.push(self.agenda[0]);
        self.agenda.remove(0);

        if let Event::TimeJumped { new_time } = self.trace[self.trace.len() - 1] {
            self.now_ms = new_time;
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
        let events: Vec<Event> = vec![
            Event::MessageDelivered { from: 1, to: 2 },
            Event::TimeJumped { new_time: 200 },
            Event::TimerFired { node: 3 },
        ];

        let mut world = World {
            now_ms: 0,
            agenda: events,
            trace: Vec::new(),
        };

        world.step();
        assert_eq!(world.now_ms, 0);
        world.step();
        assert_eq!(world.agenda.len(), 1);
        assert_eq!(world.now_ms, 200);
    }
}
