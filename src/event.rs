use std::fmt::{Display, Formatter};

use serde::{Serialize, Deserialize, Deserializer};
use serde::de::{Error, Visitor};

use crate::model::{MetricName, TimeInterval, Value};
use crate::parsing::{parse_event_expression, parse_event_query};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(pub u64);

impl Display for EventId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Deserialize)]
pub struct EventsDefinition {
    pub events: Vec<Event>
}

#[derive(Debug, Deserialize)]
pub struct Event {
    pub independent_metric: MetricName,
    pub dependent_metric: Vec<MetricName>,
    pub query: EventQuery,
    pub outputs: Vec<(EventOutputName, EventExpression)>
}

#[derive(Debug, PartialEq)]
pub enum EventQuery {
    Expression(EventExpression),
    Bool { operator: BoolOperator, left: Box<EventQuery>, right: Box<EventQuery> },
    And { left: Box<EventQuery>, right: Box<EventQuery> },
    Or { left: Box<EventQuery>, right: Box<EventQuery> }
}

struct EventQueryVisitor;
impl<'de> Visitor<'de> for EventQueryVisitor {
    type Value = EventQuery;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("expected event query")
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> where E: Error {
        self.visit_str(&value)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> where E: Error {
        parse_event_query(value)
            .map_err(|err| E::custom(
                format!(
                    "Failed to parse event query ({}:{}): {}",
                    err.location().line + 1,
                    err.location().column + 1,
                    err.to_string()
                )
            ))
    }
}

impl<'de> Deserialize<'de> for EventQuery {
    fn deserialize<D>(deserializer: D) -> Result<EventQuery, D::Error> where D: Deserializer<'de> {
        deserializer.deserialize_string(EventQueryVisitor)
    }
}

#[derive(Debug, PartialEq)]
pub enum EventExpression {
    Value(ValueExpression),
    Average { value: ValueExpression, interval: TimeInterval },
    Variance { value: ValueExpression, interval: TimeInterval },
    Covariance { left: ValueExpression, right: ValueExpression, interval: TimeInterval },
    Correlation { left: ValueExpression, right: ValueExpression, interval: TimeInterval },
    Arithmetic { operator: ArithmeticOperator, left: Box<EventExpression>, right: Box<EventExpression> },
    Function { function: Function, arguments: Vec<EventExpression> }
}

struct EventExpressionVisitor;
impl<'de> Visitor<'de> for EventExpressionVisitor {
    type Value = EventExpression;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("expected event expression")
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> where E: Error {
        self.visit_str(&value)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> where E: Error {
        parse_event_expression(value)
            .map_err(|err| E::custom(
                format!(
                    "Failed to parse event expression ({}:{}): {}",
                    err.location().line + 1,
                    err.location().column + 1,
                    err.to_string()
                )
            ))
    }
}

impl<'de> Deserialize<'de> for EventExpression {
    fn deserialize<D>(deserializer: D) -> Result<EventExpression, D::Error> where D: Deserializer<'de> {
        deserializer.deserialize_string(EventExpressionVisitor)
    }
}

#[derive(Debug, PartialEq, Deserialize)]
pub enum ValueExpression {
    IndependentMetric,
    DependentMetric,
    Constant(f64),
    Arithmetic { operator: ArithmeticOperator, left: Box<ValueExpression>, right: Box<ValueExpression> },
    Function { function: Function, arguments: Vec<ValueExpression> }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventOutputName {
    String(String),
    IndependentMetricName,
    DependentMetricName
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize,)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
pub enum Function {
    Abs,
    Sqrt,
    Square,
    Exp,
    LogE
}

impl Function {
    pub fn evaluate(&self, arguments: &Vec<f64>) -> Option<f64> {
        match self {
            Function::Abs if arguments.len() == 1 => Some(arguments[0].abs()),
            Function::Sqrt if arguments.len() == 1 => Some(arguments[0].sqrt()),
            Function::Square if arguments.len() == 1 => Some(arguments[0] * arguments[0]),
            Function::Exp if arguments.len() == 2 => Some(arguments[0].exp()),
            Function::LogE if arguments.len() == 1  => Some(arguments[0].ln()),
            _ => None
        }
    }
}

impl Display for Function {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Function::Abs => write!(f, "abs"),
            Function::Sqrt => write!(f, "sqrt"),
            Function::Square => write!(f, "square"),
            Function::Exp => write!(f, "exp"),
            Function::LogE => write!(f, "ln"),
        }
    }
}
