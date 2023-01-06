use std::time::Duration;

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

#[derive(Debug, Clone)]
pub enum EventError {
    FailedToCompileMetric
}

pub type EventResult<T> = Result<T, EventError>;
