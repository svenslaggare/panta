use std::fmt::{Display};
use std::path::Path;
use std::time::{Duration, Instant};

use serde::de::{Error, Visitor};
use serde::{Serialize, Deserialize, Deserializer};

use crate::event::Event;
use crate::event_output::EventOutputDefinition;

#[derive(Debug, Deserialize)]
pub struct EventsDefinition {
    pub sampling_rate: f64,
    pub events: Vec<Event>,
    pub outputs: Vec<EventOutputDefinition>
}

impl EventsDefinition {
    pub fn load_from_file(path: &Path) -> EventResult<EventsDefinition> {
        let content = std::fs::read_to_string(path).map_err(|err| EventError::FailedToLoad(err.to_string()))?;
        serde_yaml::from_str(&content).map_err(|err| EventError::FailedToLoad(err.to_string()))
    }
}

pub type TimePoint = Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetricId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct MetricName {
    pub name: String,
    pub sub: Option<String>
}

impl MetricName {
    pub fn all(name: &str) -> MetricName {
        MetricName {
            name: name.to_owned(),
            sub: None
        }
    }

    pub fn sub(name: &str, sub: &str) -> MetricName {
        MetricName {
            name: name.to_owned(),
            sub: Some(sub.to_owned())
        }
    }

    pub fn new(name: &str, sub: Option<&str>) -> MetricName {
        MetricName {
            name: name.to_owned(),
            sub: sub.map(|sub| sub.to_owned())
        }
    }

    pub fn as_all(&self) -> MetricName {
        MetricName {
            name: self.name.clone(),
            sub: None
        }
    }

    pub fn is_specific(&self) -> bool {
        self.sub.is_some()
    }
}

impl Display for MetricName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(sub) = self.sub.as_ref() {
            write!(f, "{}:{}", self.name, sub)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

struct MetricNameVisitor;
impl<'de> Visitor<'de> for MetricNameVisitor {
    type Value = MetricName;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("string on the format key:value")
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> where E: Error {
        self.visit_str(&value)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> where E: Error {
        let parts = value.split(":").collect::<Vec<_>>();
        if parts.len() == 2 {
            Ok(MetricName::sub(parts[0], parts[1]))
        } else if parts.len() == 1 {
            Ok(MetricName::all(parts[0]))
        } else {
            Err(E::custom("string on the format key:value"))
        }
    }
}

impl<'de> Deserialize<'de> for MetricName {
    fn deserialize<D>(deserializer: D) -> Result<MetricName, D::Error> where D: Deserializer<'de> {
        deserializer.deserialize_string(MetricNameVisitor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
pub enum TimeInterval {
    Seconds(f64),
    Minutes(f64)
}

impl TimeInterval {
    pub fn duration(&self) -> Duration {
        match self {
            TimeInterval::Seconds(value) => Duration::from_secs_f64(*value),
            TimeInterval::Minutes(value) => Duration::from_secs_f64(value * 60.0)
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Float(f64),
    Bool(bool)
}

impl Value {
    pub fn float(&self) -> Option<f64> {
        if let Value::Float(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    pub fn bool(&self) -> Option<bool> {
        if let Value::Bool(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    pub fn convert_float(&self) -> f64 {
        match self {
            Value::Float(value) => *value,
            Value::Bool(value) => if *value {1.0} else {0.0}
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Float(value) => write!(f, "{}", value),
            Value::Bool(value) => write!(f, "{}", value),
        }
    }
}

#[derive(Debug)]
pub enum EventError {
    FailedToCollectSystemMetric(std::io::Error),
    FailedToCollectRabbitMQMetric(reqwest::Error),
    DockerError(bollard::errors::Error),
    FailedToCollectDockerMetric(String),
    MetricNotFound(MetricName),
    FailedToLoad(String),
    FailedToCreateFile(std::io::Error),
    FailedToWriteFile(std::io::Error)
}

pub type EventResult<T> = Result<T, EventError>;

impl From<bollard::errors::Error> for EventError {
    fn from(err: bollard::errors::Error) -> Self {
        EventError::DockerError(err)
    }
}

#[test]
fn test_deserialize_metric_name1() {
    assert_eq!(Some(MetricName::all("haha")), serde_json::from_str::<MetricName>("\"haha\"").ok());
    assert_eq!(Some(MetricName::sub("haha", "hoho")), serde_json::from_str::<MetricName>("\"haha:hoho\"").ok());
    assert_eq!(None, serde_json::from_str::<MetricName>("\"haha:hoho:hihi\"").ok());
}