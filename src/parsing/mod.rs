use crate::event::{EventExpression, EventQuery};
use crate::parsing::parser_tree_converter::ConvertParserTreeError;
use crate::parsing::tokenizer::{ParserError, TokenLocation};

pub mod tokenizer;
pub mod operator;
pub mod parser;
pub mod parser_tree_converter;

#[derive(Debug)]
pub enum CommonParserError {
    ParserError(ParserError),
    ConvertParserTreeError(ConvertParserTreeError)
}

impl CommonParserError {
    pub fn location(&self) -> &TokenLocation {
        match self {
            CommonParserError::ParserError(err) => &err.location,
            CommonParserError::ConvertParserTreeError(err) => &err.location
        }
    }
}

impl std::fmt::Display for CommonParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommonParserError::ParserError(err) => write!(f, "{}", err.error),
            CommonParserError::ConvertParserTreeError(err) => write!(f, "{}", err.error),
        }
    }
}

pub fn parse_event_query(line: &str) -> Result<EventQuery, CommonParserError> {
    let parse_tree = parser::parse_str(&line).map_err(|err| CommonParserError::ParserError(err))?;
    parser_tree_converter::transform_into_event_query(parse_tree).map_err(|err| CommonParserError::ConvertParserTreeError(err))
}

pub fn parse_event_expression(line: &str) -> Result<EventExpression, CommonParserError> {
    let parse_tree = parser::parse_str(&line).map_err(|err| CommonParserError::ParserError(err))?;
    parser_tree_converter::transform_into_event_expression(parse_tree).map_err(|err| CommonParserError::ConvertParserTreeError(err))
}