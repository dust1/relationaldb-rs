use std::collections::VecDeque;

#[derive(Debug)]
pub struct ClockReplacer {
    queue: VecDeque<(bool, usize)>,
    num_pages: usize,
}

impl ClockReplacer {
    pub fn new(num_pages: usize) -> Self {
        Self { queue: VecDeque::new(), num_pages }
    }

    pub fn un_pin(&mut self, frame_id: usize) {
        for q in &mut self.queue {
            if q.1 == frame_id {
                q.0 = true;
                return;
            }
        }

        if self.num_pages == self.queue.len() {
            if let Some((idx, _)) = self.queue.iter().enumerate().find(|(_, item)| !item.0) {
                self.queue.drain(idx..=idx);
            } else {
                self.queue.pop_front();
            }
        }

        self.queue.push_back((true, frame_id));
    }

    pub fn pin(&mut self, frame_id: usize) {
        if let Some((idx, _)) = self.queue.iter().enumerate().find(|(_, item)| frame_id == item.1) {
            self.queue.drain(idx..=idx);
        }
    }

    pub fn victim(&mut self) -> Option<usize> {
        if self.queue.is_empty() {
            return None;
        }

        for q in &mut self.queue {
            if q.0 {
                q.0 = false;
                return Some(q.1);
            }
        }
        None
    }

    pub fn size(&self) -> usize {
        self.queue.iter().filter(|(flag, _)| *flag).count()
    }
}

#[cfg(test)]
mod test {
    use crate::storage::clock_replacer::ClockReplacer;
    #[test]
    fn test() {
        let mut clock_replacer = ClockReplacer::new(7);
        clock_replacer.un_pin(1);
        clock_replacer.un_pin(2);
        clock_replacer.un_pin(3);
        clock_replacer.un_pin(4);
        clock_replacer.un_pin(5);
        clock_replacer.un_pin(6);
        clock_replacer.un_pin(1);
        assert_eq!(6, clock_replacer.size());

        assert_eq!(Some(1), clock_replacer.victim());
        assert_eq!(Some(2), clock_replacer.victim());
        assert_eq!(Some(3), clock_replacer.victim());

        clock_replacer.pin(3);
        clock_replacer.pin(4);
        assert_eq!(2, clock_replacer.size());

        clock_replacer.un_pin(4);

        assert_eq!(Some(5), clock_replacer.victim());
        assert_eq!(Some(6), clock_replacer.victim());
        assert_eq!(Some(4), clock_replacer.victim());
    }
}
