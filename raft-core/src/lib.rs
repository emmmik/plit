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

impl<T> Event<T> {
    pub fn target_node(&self) -> u32 {
        match self {
            Event::TimerFired { node } => *node,
            Event::MessageDelivered { from, to, msg } => *to,
        }
    }
}
