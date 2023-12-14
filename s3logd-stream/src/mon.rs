use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::VecDeque;
use std::collections::HashMap;
use tokio::time::{sleep, Duration, Instant};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::sync::mpsc::error::TryRecvError;

pub(crate) enum DataType {
    Uninitialized,
    LogDownload,
    Max,
}

pub(crate) struct DataPoint {
    type_: DataType,
    value: usize,
}

//(value, count) pair
//(usize, usize)

struct Metric {
    last: Instant,
    inner: [(usize, usize); DataType::Max as usize],
    min: HashMap<usize, VecDeque<(usize, usize)>>,
}

impl Metric {
    fn new() -> Self {

        let mut min = HashMap::new();

        for i in 0..DataType::Max as usize {
            let mut v = VecDeque::with_capacity(15);
            for _ in 0..15 {
                v.push_back((0,0));
            }
            min.insert(i, v);
        }

        Self {
            last: Instant::now(),
            inner: [(0, 0); DataType::Max as usize],
            min: min,
        }
    }

    fn add(&mut self, dp: DataPoint) {
        let index = dp.type_ as usize;
        let (oldval, oldcount) = self.inner[index];
        self.inner[index] = (oldval+dp.value, oldcount+1);
    }

    fn update(&mut self) {
        let mut now = Instant::now();
        if now.duration_since(self.last) < Duration::new(60, 0) {
            return;
        }

        for i in 0..DataType::Max as usize {
            let (val, count) = self.inner[i];

            if let Some(vque) = self.min.get_mut(&i) {
                if let Some((_, _)) = vque.pop_front() {
                    vque.push_back((val, count));
                } else {
                    panic!("unable to get {} from min hash", i);
                }
            }
            assert!(self.min.len() == 15);
            self.inner[i] = (0, 0);
        }
        self.last = now;
    }

    fn get_stats(&self, t: DataType) -> usize {
        let (val, cnt) = self.inner[t as usize];
        val/cnt
    }

    fn get_min_stats(&self, t: DataType) -> (usize, usize) {
        let mut min5 = 0;
        let mut min15 = 0;
        let idx = t as usize;
        if let Some(vque) = self.min.get(&idx) {
            let vec5 = vque.range(10..).copied().collect::<Vec<_>>();
            let vec15 = vque.range(..).copied().collect::<Vec<_>>();

            let mut val: usize = 0;
            let mut cnt: usize = 0;
            for (v, c) in vec5.iter() {
                val += v; cnt += c;
            }
            min5 = val/cnt;

            val = 0;
            cnt = 0;
            for (v, c) in vec15.iter() {
                val += v; cnt += c;
            }
            min15 = val/cnt;
        }
        (min5, min15)
    }
}

pub(crate) async fn mon_task(quit: Arc<AtomicBool>, mut rx: UnboundedReceiver<DataPoint>) {

    let mut metric = Metric::new();

    while quit.load(Ordering::SeqCst) != true {

        match rx.try_recv() {
            Err(TryRecvError::Empty) => {
                sleep(Duration::from_millis(100)).await;
            },
            Err(TryRecvError::Disconnected) => {
                panic!("mon task channel disconnected");
            },
            Ok(dp) => {
                metric.add(dp);
            },
        }
        metric.update();
    }
}
