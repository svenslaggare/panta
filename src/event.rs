use std::fmt::{Display, Formatter};

use crate::model::{MetricId, TimeInterval, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(pub u64);

impl Display for EventId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct Event {
    pub independent_metric: MetricId,
    pub dependent_metric: MetricId,
    pub query: EventQuery,
    pub outputs: Vec<(EventOutputName, EventExpression)>
}

#[derive(Debug)]
pub enum EventQuery {
    Expression(EventExpression),
    Bool { operator: BoolOperator, left: Box<EventQuery>, right: Box<EventQuery> },
    And { left: Box<EventQuery>, right: Box<EventQuery> },
    Or { left: Box<EventQuery>, right: Box<EventQuery> }
}

#[derive(Debug)]
pub enum EventExpression {
    Value(ValueExpression),
    Average { value: ValueExpression, interval: TimeInterval },
    Variance { value: ValueExpression, interval: TimeInterval },
    Covariance { left: ValueExpression, right: ValueExpression, interval: TimeInterval },
    Correlation { left: ValueExpression, right: ValueExpression, interval: TimeInterval },
    Arithmetic { operator: ArithmeticOperator, left: Box<EventExpression>, right: Box<EventExpression> },
    Function { function: Function, arguments: Vec<EventExpression> }
}

#[derive(Debug)]
pub enum ValueExpression {
    IndependentMetric,
    DependentMetric,
    Constant(f64),
    Arithmetic { operator: ArithmeticOperator, left: Box<ValueExpression>, right: Box<ValueExpression> },
    Function { function: Function, arguments: Vec<ValueExpression> }
}

#[derive(Debug)]
pub enum EventOutputName {
    String(String),
    IndependentMetricName,
    DependentMetricName
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
