use std::collections::VecDeque;

#[derive(Default)]
pub struct Queue {
    messages: VecDeque<String>,
}

impl Queue {
    pub fn add(&mut self, message: String) {
        self.messages.push_back(message);
    }

    pub fn pop(&mut self) -> Option<String> {
        self.messages.pop_front()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}
