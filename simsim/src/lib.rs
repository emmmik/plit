#[derive(Debug, PartialEq, Clone)]
pub enum Event {
    MessageDelivered { from: u32, to: u32 },
    TimeJumped { new_time: u32 },
    TimerFired { node: u32 },
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
    fn it_works() {
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
}
