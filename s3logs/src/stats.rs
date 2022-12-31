use std::time::Instant;

pub struct TimeStats {
    start: Instant,
}

impl TimeStats {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed(&mut self) -> String {
        let s = format!("{:#?}", self.start.elapsed());
        self.reset();
        s
    }

    pub fn reset(&mut self) {
        self.start = Instant::now();
    }
}

