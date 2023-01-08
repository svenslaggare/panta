use std::fmt::{Display, Formatter};
use std::time::{Duration, Instant};

pub type TimePoint = Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetricId(pub u64);

#[derive(Debug, Clone, Copy)]
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
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
    FailedToCompileMetric
}

pub type EventResult<T> = Result<T, EventError>;
