use std::collections::HashMap;

use lazy_static::lazy_static;

use crate::event::{BinaryArithmeticOperator, BoolOperator, EventExpression, EventQuery, Function, UnaryArithmeticOperator, ValueExpression};
use crate::model::TimeInterval;
use crate::parsing::operator::Operator;
use crate::parsing::parser::{parse_str, ParserExpressionTree, ParserExpressionTreeData};
use crate::parsing::tokenizer::TokenLocation;

#[derive(Debug, PartialEq)]
pub struct ConvertParserTreeError {
    pub location: TokenLocation,
    pub error: ConvertParserTreeErrorType
}

impl ConvertParserTreeError {
    pub fn new(location: TokenLocation, error: ConvertParserTreeErrorType) -> ConvertParserTreeError {
        ConvertParserTreeError {
            location,
            error
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ConvertParserTreeErrorType {
    NotSupported,
    UndefinedOperator(Operator),
    ExpectedArgument,
    IncorrectNumberOfArgument(usize, usize),
    UndefinedVariable(String),
    UndefinedFunction(String),
    NotTimeInterval
}

impl ConvertParserTreeErrorType {
    pub fn with_location(self, location: TokenLocation) -> ConvertParserTreeError {
        ConvertParserTreeError::new(location, self)
    }
}

impl std::fmt::Display for ConvertParserTreeErrorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConvertParserTreeErrorType::NotSupported => { write!(f, "Not supported") }
            ConvertParserTreeErrorType::UndefinedOperator(operator) => { write!(f, "The operator '{}' is not defined", operator) }
            ConvertParserTreeErrorType::ExpectedArgument => { write!(f, "Expected an argument") }
            ConvertParserTreeErrorType::IncorrectNumberOfArgument(expected, actual) => { write!(f, "Expected {} number of arguments but got {}", expected, actual) }
            ConvertParserTreeErrorType::UndefinedVariable(name) => { write!(f, "Variable '{}' is not defined", name) }
            ConvertParserTreeErrorType::UndefinedFunction(name) => { write!(f, "Undefined function: {}", name) }
            ConvertParserTreeErrorType::NotTimeInterval => { write!(f, "Not time interval") }
        }
    }
}

pub fn transform_into_event_query(tree: ParserExpressionTree) -> Result<EventQuery, ConvertParserTreeError> {
    match tree.tree {
        ParserExpressionTreeData::BinaryOperator { operator, left, right } => {
            let left = Box::new(transform_into_event_query(*left)?);
            let right = Box::new(transform_into_event_query(*right)?);
            match operator {
                Operator::Single('>') => Ok(EventQuery::Bool { operator: BoolOperator::GreaterThan, left, right }),
                Operator::Dual('>', '=') => Ok(EventQuery::Bool { operator: BoolOperator::GreaterThanOrEqual, left, right }),
                Operator::Single('<') => Ok(EventQuery::Bool { operator: BoolOperator::LessThan, left, right }),
                Operator::Dual('<', '=') => Ok(EventQuery::Bool { operator: BoolOperator::LessThanOrEqual, left, right }),
                Operator::Dual('=', '=') => Ok(EventQuery::Bool { operator: BoolOperator::Equal, left, right }),
                Operator::Dual('!', '=') => Ok(EventQuery::Bool { operator: BoolOperator::NotEqual, left, right }),
                Operator::Dual('&', '&') => Ok(EventQuery::And { left, right }),
                Operator::Dual('|', '|') => Ok(EventQuery::Or { left, right }),
                op => Err(ConvertParserTreeErrorType::UndefinedOperator(op).with_location(tree.location))
            }
        }
        ParserExpressionTreeData::UnaryOperator { operator, operand } => {
            let operand = Box::new(transform_into_event_query(*operand)?);
            match operator {
                Operator::Single('!') => Ok(EventQuery::Invert { operand }),
                op => Err(ConvertParserTreeErrorType::UndefinedOperator(op).with_location(tree.location))
            }
        }
        data => {
            Ok(EventQuery::Expression(transform_into_event_expression(data.with_location(tree.location))?))
        }
    }
}

pub fn transform_into_event_expression(tree: ParserExpressionTree) -> Result<EventExpression, ConvertParserTreeError> {
    match tree.tree {
        data @ ParserExpressionTreeData::Int(_) => Ok(EventExpression::Value(transform_into_value_expression(data.with_location(tree.location))?)),
        data @ ParserExpressionTreeData::Float(_) => Ok(EventExpression::Value(transform_into_value_expression(data.with_location(tree.location))?)),
        data @ ParserExpressionTreeData::String(_) => Ok(EventExpression::Value(transform_into_value_expression(data.with_location(tree.location))?)),
        data @ ParserExpressionTreeData::Bool(_) => Ok(EventExpression::Value(transform_into_value_expression(data.with_location(tree.location))?)),
        data @ ParserExpressionTreeData::Variable(_) => Ok(EventExpression::Value(transform_into_value_expression(data.with_location(tree.location))?)),
        ParserExpressionTreeData::TimeInterval(_) => Err(ConvertParserTreeErrorType::NotSupported.with_location(tree.location)),
        ParserExpressionTreeData::BinaryOperator { operator, left, right } => {
            let left = Box::new(transform_into_event_expression(*left)?);
            let right = Box::new(transform_into_event_expression(*right)?);
            Ok(EventExpression::BinaryArithmetic { operator: transform_into_binary_arithmetic_operator(operator, tree.location)?, left, right })
        }
        ParserExpressionTreeData::UnaryOperator { operator, operand } => {
            let operand = Box::new(transform_into_event_expression(*operand)?);
            Ok(EventExpression::UnaryArithmetic { operator: transform_into_unary_arithmetic_operator(operator, tree.location)?, operand })
        }
        ParserExpressionTreeData::Call { name, mut arguments } => {
            match name.as_str() {
                "avg" => {
                    if arguments.len() != 2 {
                        return Err(ConvertParserTreeErrorType::IncorrectNumberOfArgument(2, arguments.len()).with_location(tree.location));
                    }

                    Ok(
                        EventExpression::Average {
                            value: transform_into_value_expression(arguments.remove(0))?,
                            interval: transform_into_time_interval(arguments.remove(0))?
                        }
                    )
                }
                "var" => {
                    if arguments.len() != 2 {
                        return Err(ConvertParserTreeErrorType::IncorrectNumberOfArgument(2, arguments.len()).with_location(tree.location));
                    }

                    Ok(
                        EventExpression::Variance {
                            value: transform_into_value_expression(arguments.remove(0))?,
                            interval: transform_into_time_interval(arguments.remove(0))?
                        }
                    )
                }
                "cov" => {
                    if arguments.len() != 3 {
                        return Err(ConvertParserTreeErrorType::IncorrectNumberOfArgument(3, arguments.len()).with_location(tree.location));
                    }

                    Ok(
                        EventExpression::Covariance {
                            left: transform_into_value_expression(arguments.remove(0))?,
                            right: transform_into_value_expression(arguments.remove(0))?,
                            interval: transform_into_time_interval(arguments.remove(0))?
                        }
                    )
                }
                "corr" => {
                    if arguments.len() != 3 {
                        return Err(ConvertParserTreeErrorType::IncorrectNumberOfArgument(3, arguments.len()).with_location(tree.location));
                    }

                    Ok(
                        EventExpression::Correlation {
                            left: transform_into_value_expression(arguments.remove(0))?,
                            right: transform_into_value_expression(arguments.remove(0))?,
                            interval: transform_into_time_interval(arguments.remove(0))?
                        }
                    )
                }
                _ => {
                    let mut transformed_arguments = Vec::new();
                    for argument in arguments {
                        transformed_arguments.push(transform_into_event_expression(argument)?);
                    }

                    Ok(
                        EventExpression::Function {
                            function: transform_into_function(name, tree.location)?,
                            arguments: transformed_arguments
                        }
                    )
                }
            }
        }
    }
}

fn transform_into_value_expression(tree: ParserExpressionTree) -> Result<ValueExpression, ConvertParserTreeError> {
    match tree.tree {
        ParserExpressionTreeData::Int(value) => Ok(ValueExpression::Constant(value as f64)),
        ParserExpressionTreeData::Float(value) => Ok(ValueExpression::Constant(value)),
        ParserExpressionTreeData::String(_) => Err(ConvertParserTreeErrorType::NotSupported.with_location(tree.location)),
        ParserExpressionTreeData::Bool(_) => Err(ConvertParserTreeErrorType::NotSupported.with_location(tree.location)),
        ParserExpressionTreeData::TimeInterval(_) => Err(ConvertParserTreeErrorType::NotSupported.with_location(tree.location)),
        ParserExpressionTreeData::Variable(name) => {
            if name == "ind" || name == "independent" {
                Ok(ValueExpression::IndependentMetric)
            }  else if name == "dep" || name == "dependent" {
                Ok(ValueExpression::DependentMetric)
            } else {
                Err(ConvertParserTreeErrorType::UndefinedVariable(name).with_location(tree.location))
            }
        }
        ParserExpressionTreeData::BinaryOperator { operator, left, right } => {
            let left = Box::new(transform_into_value_expression(*left)?);
            let right = Box::new(transform_into_value_expression(*right)?);
            Ok(ValueExpression::BinaryArithmetic { operator: transform_into_binary_arithmetic_operator(operator, tree.location)?, left, right })
        }
        ParserExpressionTreeData::UnaryOperator { operator, operand } => {
            let operand = Box::new(transform_into_value_expression(*operand)?);
            Ok(ValueExpression::UnaryArithmetic { operator: transform_into_unary_arithmetic_operator(operator, tree.location)?, operand })
        }
        ParserExpressionTreeData::Call { name, arguments } => {
            let mut transformed_arguments = Vec::new();
            for argument in arguments {
                transformed_arguments.push(transform_into_value_expression(argument)?);
            }

            Ok(
                ValueExpression::Function {
                    function: transform_into_function(name, tree.location)?,
                    arguments: transformed_arguments
                }
            )
        }
    }
}

fn transform_into_time_interval(tree: ParserExpressionTree) -> Result<TimeInterval, ConvertParserTreeError> {
    match tree.tree {
        ParserExpressionTreeData::Int(value) => Ok(TimeInterval::Seconds(value as f64)),
        ParserExpressionTreeData::Float(value) => Ok(TimeInterval::Seconds(value)),
        ParserExpressionTreeData::TimeInterval(value) => Ok(value),
        _ => Err(ConvertParserTreeErrorType::NotTimeInterval.with_location(tree.location))
    }
}

fn transform_into_binary_arithmetic_operator(operator: Operator, location: TokenLocation) -> Result<BinaryArithmeticOperator, ConvertParserTreeError> {
    match operator {
        Operator::Single('+') => Ok(BinaryArithmeticOperator::Add),
        Operator::Single('-') => Ok(BinaryArithmeticOperator::Subtract),
        Operator::Single('*') => Ok(BinaryArithmeticOperator::Multiply),
        Operator::Single('/') => Ok(BinaryArithmeticOperator::Divide),
        op => Err(ConvertParserTreeErrorType::UndefinedOperator(op).with_location(location))
    }
}

fn transform_into_unary_arithmetic_operator(operator: Operator, location: TokenLocation) -> Result<UnaryArithmeticOperator, ConvertParserTreeError> {
    match operator {
        Operator::Single('-') => Ok(UnaryArithmeticOperator::Negate),
        op => Err(ConvertParserTreeErrorType::UndefinedOperator(op).with_location(location))
    }
}

fn transform_into_function(name: String, location: TokenLocation) -> Result<Function, ConvertParserTreeError>  {
    FUNCTIONS
        .get(&name)
        .cloned()
        .ok_or_else(|| ConvertParserTreeErrorType::UndefinedFunction(name).with_location(location))
}

lazy_static! {
    static ref FUNCTIONS: HashMap<String, Function> = HashMap::from_iter(
        vec![
            ("abs".to_owned(), Function::Abs),
            ("square".to_owned(), Function::Square),
            ("sqrt".to_owned(), Function::Sqrt),
            ("exp".to_owned(), Function::Exp),
            ("ln".to_owned(), Function::LogE),
        ].into_iter()
    );
}

#[test]
fn test_convert_event_query1() {
    let tree = parse_str("avg(ind, 5) < 10 && corr(ind, dep, 5) > 0.5").unwrap();

    assert_eq!(
        Ok(
            EventQuery::And {
                left: Box::new(
                    EventQuery::Bool {
                        operator: BoolOperator::LessThan,
                        left: Box::new(
                            EventQuery::Expression(
                                EventExpression::Average {
                                    value: ValueExpression::IndependentMetric,
                                    interval: TimeInterval::Seconds(5.0)
                                }
                            )
                        ),
                        right: Box::new(EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(10.0))))
                    }
                ),
                right: Box::new(
                    EventQuery::Bool {
                        operator: BoolOperator::GreaterThan,
                        left: Box::new(
                            EventQuery::Expression(
                                EventExpression::Correlation {
                                    left: ValueExpression::IndependentMetric,
                                    right: ValueExpression::DependentMetric,
                                    interval: TimeInterval::Seconds(5.0)
                                }
                            )
                        ),
                        right: Box::new(EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(0.5))))
                    }
                )
            }
        ),
        transform_into_event_query(tree)
    )
}

#[test]
fn test_convert_event_query2() {
    let tree = parse_str("avg(abs(ind), 0.5m) < 10 && corr(ind, dep, 0.5m) > 0.5").unwrap();

    assert_eq!(
        Ok(
            EventQuery::And {
                left: Box::new(
                    EventQuery::Bool {
                        operator: BoolOperator::LessThan,
                        left: Box::new(
                            EventQuery::Expression(
                                EventExpression::Average {
                                    value: ValueExpression::Function {
                                        function: Function::Abs,
                                        arguments: vec![ValueExpression::IndependentMetric]
                                    },
                                    interval: TimeInterval::Minutes(0.5)
                                }
                            )
                        ),
                        right: Box::new(EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(10.0))))
                    }
                ),
                right: Box::new(
                    EventQuery::Bool {
                        operator: BoolOperator::GreaterThan,
                        left: Box::new(
                            EventQuery::Expression(
                                EventExpression::Correlation {
                                    left: ValueExpression::IndependentMetric,
                                    right: ValueExpression::DependentMetric,
                                    interval: TimeInterval::Minutes(0.5)
                                }
                            )
                        ),
                        right: Box::new(EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(0.5))))
                    }
                )
            }
        ),
        transform_into_event_query(tree)
    )
}

#[test]
fn test_convert_event_query3() {
    let tree = parse_str("corr(ind, dep, 5s) > 0.5 && avg(ind, 5s) > 0.1 && avg(dep, 5s) > 0.0").unwrap();

    assert_eq!(
        Ok(
            EventQuery::And {
                left: Box::new(
                    EventQuery::And {
                        left: Box::new(
                            EventQuery::Bool {
                                operator: BoolOperator::GreaterThan,
                                left: Box::new(
                                    EventQuery::Expression(
                                        EventExpression::Correlation {
                                            left: ValueExpression::IndependentMetric,
                                            right: ValueExpression::DependentMetric,
                                            interval: TimeInterval::Seconds(5.0),
                                        }
                                    )
                                ),
                                right: Box::new(
                                    EventQuery::Expression(
                                        EventExpression::Value(ValueExpression::Constant(0.5), )
                                    )
                                )
                            }
                        ),
                        right: Box::new(
                            EventQuery::Bool {
                                operator: BoolOperator::GreaterThan,
                                left: Box::new(
                                    EventQuery::Expression(
                                        EventExpression::Average {
                                            value: ValueExpression::IndependentMetric,
                                            interval: TimeInterval::Seconds(5.0),
                                        },
                                    )
                                ),
                                right: Box::new(
                                    EventQuery::Expression(
                                        EventExpression::Value(ValueExpression::Constant(0.1))
                                    )
                                )
                            }
                        ),
                    }
                ),
                right: Box::new(
                    EventQuery::Bool {
                        operator: BoolOperator::GreaterThan,
                        left: Box::new(
                            EventQuery::Expression(
                                EventExpression::Average {
                                    value: ValueExpression::DependentMetric,
                                    interval: TimeInterval::Seconds(
                                        5.0,
                                    )
                                },
                            )
                        ),
                        right: Box::new(
                            EventQuery::Expression(
                                EventExpression::Value(ValueExpression::Constant(0.0))
                            )
                        )
                    }
                ),
            }
        ),
        transform_into_event_query(tree)
    )
}

#[test]
fn test_convert_event_query4() {
    let tree = parse_str("!(avg(ind, 5) < 10 && corr(ind, dep, 5) > 0.5)").unwrap();

    assert_eq!(
        Ok(
            EventQuery::Invert {
                operand: Box::new(
                    EventQuery::And {
                        left: Box::new(
                            EventQuery::Bool {
                                operator: BoolOperator::LessThan,
                                left: Box::new(
                                    EventQuery::Expression(
                                        EventExpression::Average {
                                            value: ValueExpression::IndependentMetric,
                                            interval: TimeInterval::Seconds(5.0)
                                        }
                                    )
                                ),
                                right: Box::new(EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(10.0))))
                            }
                        ),
                        right: Box::new(
                            EventQuery::Bool {
                                operator: BoolOperator::GreaterThan,
                                left: Box::new(
                                    EventQuery::Expression(
                                        EventExpression::Correlation {
                                            left: ValueExpression::IndependentMetric,
                                            right: ValueExpression::DependentMetric,
                                            interval: TimeInterval::Seconds(5.0)
                                        }
                                    )
                                ),
                                right: Box::new(EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(0.5))))
                            }
                        )
                    }
                )
            }
        ),
        transform_into_event_query(tree)
    )
}