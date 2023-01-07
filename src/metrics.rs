use std::time::Duration;
use fnv::FnvHashMap;

use crate::model::{MetricId, TimeInterval, TimePoint};

pub struct MetricDefinitions {
    next_metric_id: MetricId,
    metrics: FnvHashMap<String, MetricId>,
    metric_id_to_name_mapping: FnvHashMap<MetricId, String>,
}

impl MetricDefinitions {
    pub fn new() -> MetricDefinitions {
        MetricDefinitions {
            next_metric_id: MetricId(1),
            metrics: FnvHashMap::default(),
            metric_id_to_name_mapping: FnvHashMap::default()
        }
    }

    pub fn define(&mut self, name: &str) -> MetricId {
        *self.metrics
            .entry(name.to_owned())
            .or_insert_with(|| {
                let metric_id = self.next_metric_id;
                self.metric_id_to_name_mapping.insert(metric_id, name.to_owned());
                self.next_metric_id.0 += 1;
                metric_id
            })
    }

    pub fn get_id(&self, name: &str) -> Option<MetricId> {
        self.metrics.get(name).cloned()
    }

    pub fn get_name(&self, id: MetricId) -> Option<&str> {
        self.metric_id_to_name_mapping.get(&id).map(|x| x.as_str())
    }
}

#[derive(Debug)]
pub struct MetricValues {
    max_keep: Duration,
    values: FnvHashMap<MetricId, (TimePoint, f64)>
}

impl MetricValues {
    pub fn new(max_keep: TimeInterval) -> MetricValues {
        MetricValues {
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

    pub fn iter(&self) -> impl Iterator<Item=(MetricId, f64)> + '_ {
        self.values.iter().map(|(id, (_, value))| (*id, *value))
    }

    pub fn clear_old(&mut self, time: TimePoint) {
        self.values.retain(|_, (value_time, _)| time.duration_since(*value_time) < self.max_keep)
    }
}