use std::fmt::{Display, Formatter};

use crate::model::{MetricId, TimeInterval, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(pub u64);

impl Display for EventId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct Event {
    pub independent_metric: MetricId,
    pub dependent_metric: MetricId,
    pub query: EventQuery,
    pub outputs: Vec<(String, EventExpression)>
}

pub enum EventQuery {
    Expression(EventExpression),
    Bool { left: Box<EventQuery>, right: Box<EventQuery>, operation: BoolOperator },
    And { left: Box<EventQuery>, right: Box<EventQuery> },
    Or { left: Box<EventQuery>, right: Box<EventQuery> }
}

pub enum EventExpression {
    Value(ValueExpression),
    Average { value: ValueExpression, interval: TimeInterval },
    Variance { value: ValueExpression, interval: TimeInterval },
    Covariance { left: ValueExpression, right: ValueExpression, interval: TimeInterval },
    Arithmetic { left: Box<EventExpression>, right: Box<EventExpression>, operation: ArithmeticOperator },
}

pub enum ValueExpression {
    IndependentMetric,
    DependentMetric,
    Constant(f64),
    Arithmetic { left: Box<ValueExpression>, right: Box<ValueExpression>, operation: ArithmeticOperator },
}

#[derive(Debug)]
pub enum BoolOperator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual
}

impl BoolOperator {
    pub fn evaluate(&self, left: &Value, right: &Value) -> Option<bool> {
        match (left, right) {
            (Value::Float(left), Value::Float(right)) => {
                match self {
                    BoolOperator::Equal => Some(left == right),
                    BoolOperator::NotEqual => Some(left != right),
                    BoolOperator::GreaterThan => Some(left > right),
                    BoolOperator::GreaterThanOrEqual => Some(left >= right),
                    BoolOperator::LessThan => Some(left < right),
                    BoolOperator::LessThanOrEqual => Some(left <= right),
                }
            }
            (Value::Bool(left), Value::Bool(right)) => {
                match self {
                    BoolOperator::Equal => Some(left == right),
                    BoolOperator::NotEqual => Some(left != right),
                    _ => None
                }
            }
            _ => None
        }
    }
}

impl Display for BoolOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BoolOperator::Equal => write!(f, "=="),
            BoolOperator::NotEqual => write!(f, "!="),
            BoolOperator::GreaterThan => write!(f, ">"),
            BoolOperator::GreaterThanOrEqual => write!(f, ">="),
            BoolOperator::LessThan => write!(f, "<"),
            BoolOperator::LessThanOrEqual => write!(f, "<="),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide
}

impl ArithmeticOperator {
    pub fn evaluate(&self, left: f64, right: f64) -> f64 {
        match self {
            ArithmeticOperator::Add => left + right,
            ArithmeticOperator::Subtract => left - right,
            ArithmeticOperator::Multiply => left * right,
            ArithmeticOperator::Divide => left / right
        }
    }
}

impl Display for ArithmeticOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ArithmeticOperator::Add => write!(f, "+"),
            ArithmeticOperator::Subtract => write!(f, "-"),
            ArithmeticOperator::Multiply => write!(f, "*"),
            ArithmeticOperator::Divide => write!(f, "/"),
        }
    }
}