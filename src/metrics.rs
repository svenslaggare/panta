use std::ops::Index;
use std::time::Duration;
use fnv::FnvHashMap;

use crate::model::{MetricId, TimeInterval, TimePoint};

pub struct Metrics {
    max_keep: Duration,
    values: FnvHashMap<MetricId, (TimePoint, f64)>
}

impl Metrics {
    pub fn new(max_keep: TimeInterval) -> Metrics {
        Metrics {
            max_keep: max_keep.duration(),
            values: FnvHashMap::default()
        }
    }

    pub fn insert(&mut self, time: TimePoint, metric: MetricId, value: f64) {
        self.values.insert(metric, (time, value));
    }

    pub fn get(&self, metric: &MetricId) -> Option<&f64> {
        self.values.get(metric).map(|(_, value)| value)
    }

    pub fn keys(&self) -> impl Iterator<Item=&MetricId> {
        self.values.keys()
    }

    pub fn clear_old(&mut self, time: TimePoint) {
        self.values.retain(|_, (value_time, _)| time.duration_since(*value_time) < self.max_keep)
    }
}

impl Index<&MetricId> for Metrics {
    type Output = f64;

    fn index(&self, index: &MetricId) -> &Self::Output {
        self.get(index).unwrap()
    }
}