use std::collections::BTreeSet;
use std::time::Duration;

use fnv::{FnvHashMap};

use crate::model::{EventError, EventResult, MetricId, MetricName, TimeInterval, TimePoint};

pub struct MetricDefinitions {
    next_metric_id: MetricId,
    metrics: FnvHashMap<String, BTreeSet<MetricId>>,
    specific_metrics: FnvHashMap<MetricName, MetricId>,
    metric_id_to_specific_name_mapping: FnvHashMap<MetricId, MetricName>,
}

impl MetricDefinitions {
    pub fn new() -> MetricDefinitions {
        MetricDefinitions {
            next_metric_id: MetricId(1),
            metrics: FnvHashMap::default(),
            specific_metrics: FnvHashMap::default(),
            metric_id_to_specific_name_mapping: FnvHashMap::default()
        }
    }

    pub fn define(&mut self, metric: MetricName) -> MetricId {
        let metric_name = metric.name.clone();

        if metric.is_specific() {
            self.define_internal(metric.as_all());
            let metric_id_full = self.define_internal(metric);

            let sub_metrics = self.metrics
                .entry(metric_name)
                .or_insert_with(|| BTreeSet::default());

            sub_metrics.insert(metric_id_full);

            metric_id_full
        } else {
            let metric_id = self.define_internal(metric);

            self.metrics
                .entry(metric_name)
                .or_insert_with(|| BTreeSet::default());

            metric_id
        }
    }

    fn define_internal(&mut self, metric: MetricName) -> MetricId {
        *self.specific_metrics
            .entry(metric.clone())
            .or_insert_with(|| {
                let metric_id = self.next_metric_id;
                self.metric_id_to_specific_name_mapping.insert(metric_id, metric);
                self.next_metric_id.0 += 1;
                metric_id
            })
    }

    pub fn expand(&self, metric: &MetricName) -> EventResult<Vec<MetricId>> {
        if metric.is_specific() {
            Ok(vec![self.get_specific_id(metric)?])
        } else {
            let sub_metrics = self.metrics
                .get(&metric.name)
                .ok_or_else(|| EventError::MetricNotFound(metric.clone()))?;

            if sub_metrics.is_empty() {
                Ok(vec![self.get_specific_id(metric)?])
            } else {
                Ok(Vec::from_iter(sub_metrics.iter().cloned()))
            }
        }
    }

    pub fn get_specific_id(&self, metric: &MetricName) -> EventResult<MetricId> {
        self.specific_metrics.get(metric)
            .cloned()
            .ok_or_else(|| EventError::MetricNotFound(metric.clone()))
    }

    pub fn get_specific_name(&self, id: MetricId) -> Option<&MetricName> {
        self.metric_id_to_specific_name_mapping.get(&id)
    }

    pub fn print(&self) {
        for (metric, sub_metrics) in &self.metrics {
            println!("{}", metric);
            for sub_metric in sub_metrics {
                println!("\t{}", self.get_specific_name(*sub_metric).unwrap().sub.as_ref().map(|x| x.as_str()).unwrap_or(""));
            }
        }
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

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn insert(&mut self, time: TimePoint, metric: MetricId, value: f64) {
        self.values.insert(metric, (time, value));
    }

    pub fn extend(&mut self, other: MetricValues) {
        self.values.extend(other.values.into_iter())
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

#[test]
fn test_metric_definitions1() {
    let mut metric_definitions = MetricDefinitions::new();
    let metric_id = metric_definitions.define(MetricName::all("x"));
    assert_eq!(vec![metric_id], metric_definitions.expand(&MetricName::all("x")).unwrap());
}

#[test]
fn test_metric_definitions2() {
    let mut metric_definitions = MetricDefinitions::new();
    metric_definitions.define(MetricName::all("x"));
    let metric_id1 = metric_definitions.define(MetricName::sub("x", "1"));

    assert_eq!(vec![metric_id1], metric_definitions.expand(&MetricName::all("x")).unwrap());
    assert_eq!(vec![metric_id1], metric_definitions.expand(&MetricName::sub("x", "1")).unwrap());

    let metric_id2 = metric_definitions.define(MetricName::sub("x", "2"));
    assert_eq!(vec![metric_id1, metric_id2], metric_definitions.expand(&MetricName::all("x")).unwrap());
    assert_eq!(vec![metric_id1], metric_definitions.expand(&MetricName::sub("x", "1")).unwrap());
}

#[test]
fn test_metric_definitions3() {
    let mut metric_definitions = MetricDefinitions::new();
    let metric_id1 = metric_definitions.define(MetricName::sub("x", "1"));

    assert_eq!(vec![metric_id1], metric_definitions.expand(&MetricName::all("x")).unwrap());
    assert_eq!(vec![metric_id1], metric_definitions.expand(&MetricName::sub("x", "1")).unwrap());

    let metric_id2 = metric_definitions.define(MetricName::sub("x", "2"));
    assert_eq!(vec![metric_id1, metric_id2], metric_definitions.expand(&MetricName::all("x")).unwrap());
    assert_eq!(vec![metric_id1], metric_definitions.expand(&MetricName::sub("x", "1")).unwrap());
}