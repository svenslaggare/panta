use crate::model::TimeInterval;
use crate::parsing::operator::{BinaryOperators, Operator, UnaryOperators};
use crate::parsing::tokenizer::{ParserError, ParserErrorType, ParserToken, Token, tokenize, tokenize_simple, TokenLocation};

pub fn parse_str(text: &str) -> ParserResult<ParserExpressionTree> {
    let tokens = tokenize(text)?;

    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    Parser::new(&binary_operators, &unary_operators, tokens).parse()
}

#[derive(PartialEq, Debug, Clone)]
pub struct ParserExpressionTree {
    pub location: TokenLocation,
    pub tree: ParserExpressionTreeData
}

impl ParserExpressionTree {
    pub fn new(location: TokenLocation, tree: ParserExpressionTreeData) -> ParserExpressionTree {
        ParserExpressionTree {
            location,
            tree
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum ParserExpressionTreeData {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    TimeInterval(TimeInterval),
    Variable(String),
    BinaryOperator { operator: Operator, left: Box<ParserExpressionTree>, right: Box<ParserExpressionTree> },
    UnaryOperator { operator: Operator, operand: Box<ParserExpressionTree>},
    Call { name: String, arguments: Vec<ParserExpressionTree> },
}

impl ParserExpressionTreeData {
    pub fn with_location(self, location: TokenLocation) -> ParserExpressionTree {
        ParserExpressionTree {
            location,
            tree: self
        }
    }

    pub fn visit<'a, E, F: FnMut(&'a ParserExpressionTreeData) -> Result<(), E>>(&'a self, f: &mut F) -> Result<(), E> {
        match self {
            ParserExpressionTreeData::Int(_) => {}
            ParserExpressionTreeData::Float(_) => {}
            ParserExpressionTreeData::String(_) => {}
            ParserExpressionTreeData::Bool(_) => {}
            ParserExpressionTreeData::TimeInterval(_) => {}
            ParserExpressionTreeData::Variable(_) => {}
            ParserExpressionTreeData::BinaryOperator { left, right, .. } => {
                left.tree.visit(f)?;
                right.tree.visit(f)?;
            }
            ParserExpressionTreeData::UnaryOperator { operand, .. } => {
                operand.tree.visit(f)?;
            }
            ParserExpressionTreeData::Call { arguments, .. } => {
                for arg in arguments {
                    arg.tree.visit(f)?;
                }
            }
        }

        f(self)?;
        Ok(())
    }
}

pub type ParserResult<T> = Result<T, ParserError>;

pub struct Parser<'a> {
    tokens: Vec<ParserToken>,
    index: isize,
    binary_operators: &'a BinaryOperators,
    unary_operators: &'a UnaryOperators
}

impl<'a> Parser<'a> {
    pub fn new(binary_operators: &'a BinaryOperators,
               unary_operators: &'a UnaryOperators,
               tokens: Vec<ParserToken>) -> Parser<'a> {
        Parser {
            tokens,
            index: -1,
            binary_operators,
            unary_operators
        }
    }

    pub fn from_plain_tokens(binary_operators: &'a BinaryOperators,
                             unary_operators: &'a UnaryOperators,
                             tokens: Vec<Token>) -> Parser<'a> {
        Parser {
            tokens: tokens.into_iter().map(|token| ParserToken::new(0, 0, token)).collect(),
            index: -1,
            binary_operators,
            unary_operators
        }
    }

    pub fn parse(&mut self) -> ParserResult<ParserExpressionTree> {
        self.next()?;
        self.parse_expression_internal()
    }

    fn parse_expression_internal(&mut self) -> ParserResult<ParserExpressionTree> {
        let lhs = self.parse_unary_operator()?;
        self.parse_binary_operator_rhs(0, lhs)
    }

    fn parse_binary_operator_rhs(&mut self, precedence: i32, lhs: ParserExpressionTree) -> ParserResult<ParserExpressionTree> {
        let mut lhs = lhs;
        loop {
            let token_precedence = self.get_token_precedence()?;

            if token_precedence < precedence {
                return Ok(lhs);
            }

            let op_location = self.current_location();
            let op = self.current().clone();
            self.next()?;

            let mut rhs = self.parse_unary_operator()?;
            if token_precedence < self.get_token_precedence()? {
                rhs = self.parse_binary_operator_rhs(token_precedence + 1, rhs)?;
            }

            match op {
                Token::Operator(Operator::Single('.')) => {
                    match (lhs.tree, rhs.tree) {
                        (ParserExpressionTreeData::Variable(left), ParserExpressionTreeData::Variable(right)) => {
                            lhs = ParserExpressionTree::new(op_location, ParserExpressionTreeData::Variable(format!("{}.{}", left, right)));
                        }
                        _ => { return Err(ParserError::new(op_location, ParserErrorType::ExpectedVariableReference)); }
                    }
                }
                Token::Operator(op) => {
                    lhs = ParserExpressionTree::new(op_location, ParserExpressionTreeData::BinaryOperator { operator: op, left: Box::new(lhs), right: Box::new(rhs) });
                }
                _ => { return Err(ParserError::new(op_location, ParserErrorType::ExpectedOperator)); }
            }
        }
    }

    fn get_token_precedence(&self) -> ParserResult<i32> {
        match self.current() {
            Token::Operator(op) => {
                match self.binary_operators.get(op) {
                    Some(bin_op) => Ok(bin_op.precedence),
                    None => Err(self.create_error(ParserErrorType::NotDefinedBinaryOperator(op.clone())))
                }
            }
            _ => Ok(-1)
        }
    }

    fn parse_primary_expression(&mut self) -> ParserResult<ParserExpressionTree> {
        let token_location = self.current_location();
        match self.current().clone() {
            Token::Int(value) => {
                self.next()?;

                match self.current() {
                    Token::Identifier(id) if id == "s" => {
                        self.next()?;
                        Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::TimeInterval(TimeInterval::Seconds(value as f64))))
                    }
                    Token::Identifier(id) if id == "m" => {
                        self.next()?;
                        Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::TimeInterval(TimeInterval::Minutes(value as f64))))
                    }
                    _ => {
                        Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::Int(value)))
                    }
                }
            }
            Token::Float(value) => {
                self.next()?;

                match self.current() {
                    Token::Identifier(id) if id == "s" => {
                        self.next()?;
                        Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::TimeInterval(TimeInterval::Seconds(value))))
                    }
                    Token::Identifier(id) if id == "m" => {
                        self.next()?;
                        Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::TimeInterval(TimeInterval::Minutes(value))))
                    }
                    _ => {
                        Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::Float(value)))
                    }
                }
            }
            Token::String(value) => {
                self.next()?;
                Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::String(value)))
            }
            Token::True => {
                self.next()?;
                Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::Bool(true)))
            }
            Token::False => {
                self.next()?;
                Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::Bool(false)))
            }
            Token::Identifier(identifier) => self.parse_identifier_expression(identifier.clone()),
            Token::LeftParentheses => {
                self.next()?;
                let expression = self.parse_expression_internal();

                self.expect_and_consume_token(
                    Token::RightParentheses,
                    ParserErrorType::ExpectedRightParentheses
                )?;

                expression
            }
            _ => Err(self.create_error(ParserErrorType::ExpectedExpression))
        }
    }

    fn parse_identifier_expression(&mut self, identifier: String) -> ParserResult<ParserExpressionTree> {
        self.next()?;

        let token_location = self.current_location();
        match self.current() {
            Token::LeftParentheses => (),
            _ => return Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::Variable(identifier)))
        }

        self.next()?;

        let mut arguments = Vec::<ParserExpressionTree>::new();

        match self.current() {
            Token::RightParentheses => (),
            _ => {
                loop {
                    arguments.push(self.parse_expression_internal()?);
                    match self.current() {
                        Token::RightParentheses => { break; }
                        Token::Comma => {}
                        _ => return Err(self.create_error(ParserErrorType::ExpectedArgumentListContinuation))
                    }

                    self.next()?;
                }
            }
        }

        self.next()?;

        Ok(
            ParserExpressionTree::new(
                token_location,
                ParserExpressionTreeData::Call { name: identifier, arguments }
            )
        )
    }

    fn parse_unary_operator(&mut self) -> ParserResult<ParserExpressionTree> {
        match self.current() {
            Token::Operator(_) => {},
            _ => return self.parse_primary_expression()
        }

        let op_location = self.current_location();
        let op_token = self.current().clone();
        self.next()?;

        let operand = self.parse_unary_operator()?;
        match op_token {
            Token::Operator(op) => {
                if !self.unary_operators.exists(&op) {
                    return Err(ParserError::new(op_location, ParserErrorType::NotDefinedUnaryOperator(op)));
                }

                Ok(ParserExpressionTree::new(op_location,ParserExpressionTreeData::UnaryOperator { operator: op, operand: Box::new(operand) }))
            }
            _ => Err(ParserError::new(op_location, ParserErrorType::Unknown))
        }
    }

    fn expect_and_consume_token(&mut self, token: Token, error: ParserErrorType) -> ParserResult<()> {
        self.expect_token(token, error)?;
        self.next()?;
        Ok(())
    }

    fn expect_token(&self, token: Token, error: ParserErrorType) -> ParserResult<()> {
        if self.current() != &token {
            return Err(self.create_error(error));
        }

        Ok(())
    }

    fn current(&self) -> &Token {
        &self.tokens[self.index as usize].token
    }

    fn current_location(&self) -> TokenLocation {
        self.tokens[self.index as usize].location.clone()
    }

    fn create_error(&self, error: ParserErrorType) -> ParserError {
        ParserError::new(self.current_location(), error)
    }

    pub fn next(&mut self) -> ParserResult<&Token> {
        let next_index = self.index + 1;
        if next_index >= self.tokens.len() as isize {
            return Err(self.create_error(ParserErrorType::ReachedEndOfTokens));
        }

        self.index = next_index;
        Ok(&self.tokens[self.index as usize].token)
    }
}

fn parse_str_test(text: &str) -> ParserResult<ParserExpressionTree> {
    let tokens = tokenize_simple(text)?;

    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    Parser::from_plain_tokens(&binary_operators, &unary_operators, tokens).parse()
}

#[test]
fn test_operator1() {
    assert_eq!(
        Ok(
            ParserExpressionTreeData::BinaryOperator {
                operator: Operator::Single('+'),
                left: Box::new(
                    ParserExpressionTreeData::Variable("a".to_owned()).with_location(Default::default())
                ),
                right: Box::new(
                    ParserExpressionTreeData::Int(13).with_location(Default::default())
                )
            }
        ),
        parse_str_test("a + 13").map(|tree| tree.tree)
    )
}

#[test]
fn test_operator2() {
    assert_eq!(
        Ok(
            ParserExpressionTreeData::BinaryOperator {
                operator: Operator::Dual('&', '&'),
                left: Box::new(
                    ParserExpressionTreeData::BinaryOperator {
                        operator: Operator::Dual('>', '='),
                        left: Box::new(
                            ParserExpressionTreeData::Variable("a".to_owned()).with_location(Default::default())
                        ),
                        right: Box::new(
                            ParserExpressionTreeData::Int(5).with_location(Default::default())
                        )
                    }.with_location(Default::default())
                ),
                right: Box::new(
                    ParserExpressionTreeData::BinaryOperator {
                        operator: Operator::Single('<'),
                        left: Box::new(
                            ParserExpressionTreeData::Variable("b".to_owned()).with_location(Default::default())
                        ),
                        right: Box::new(
                            ParserExpressionTreeData::Int(2).with_location(Default::default())
                        )
                    }.with_location(Default::default())
                )
            }
        ),
        parse_str_test("a >= 5 && b < 2").map(|tree| tree.tree)
    )
}

#[test]
fn test_operator3() {
    assert_eq!(
        Ok(
            ParserExpressionTreeData::BinaryOperator {
                operator: Operator::Dual('|', '|'),
                left: Box::new(
                    ParserExpressionTreeData::UnaryOperator {
                        operator: Operator::Single('!'),
                        operand: Box::new(
                            ParserExpressionTreeData::Variable("a".to_owned()).with_location(Default::default())
                        )
                    }.with_location(Default::default())
                ),
                right: Box::new(
                    ParserExpressionTreeData::BinaryOperator {
                        operator: Operator::Single('<'),
                        left: Box::new(
                            ParserExpressionTreeData::Variable("b".to_owned()).with_location(Default::default())
                        ),
                        right: Box::new(
                            ParserExpressionTreeData::UnaryOperator {
                                operator: Operator::Single('-'),
                                operand: Box::new(
                                    ParserExpressionTreeData::Int(5).with_location(Default::default())
                                )
                            }.with_location(Default::default())
                        )
                    }.with_location(Default::default())
                )
            }
        ),
        parse_str_test("!a || b < -5").map(|tree| tree.tree)
    )
}

#[test]
fn test_call1() {
    assert_eq!(
        Ok(
            ParserExpressionTreeData::BinaryOperator {
                operator: Operator::Dual('&', '&'),
                left: Box::new(
                    ParserExpressionTreeData::BinaryOperator {
                        operator: Operator::Dual('>', '='),
                        left: Box::new(
                            ParserExpressionTreeData::Call {
                                name: "avg".to_string(),
                                arguments: vec![ParserExpressionTreeData::Variable("x".to_owned()).with_location(Default::default())]
                            }.with_location(Default::default())
                        ),
                        right: Box::new(
                            ParserExpressionTreeData::Float(10.0).with_location(Default::default())
                        )
                    }.with_location(Default::default())
                ),
                right: Box::new(
                    ParserExpressionTreeData::BinaryOperator {
                        operator: Operator::Single('<'),
                        left: Box::new(
                            ParserExpressionTreeData::Call {
                                name: "corr".to_string(),
                                arguments: vec![
                                    ParserExpressionTreeData::Variable("x".to_owned()).with_location(Default::default()),
                                    ParserExpressionTreeData::Variable("y".to_owned()).with_location(Default::default())
                                ]
                            }.with_location(Default::default())
                        ),
                        right: Box::new(
                            ParserExpressionTreeData::Float(0.5).with_location(Default::default())
                        )
                    }.with_location(Default::default())
                )
            }
        ),
        parse_str_test("avg(x) >= 10.0 && corr(x, y) < 0.5").map(|tree| tree.tree)
    )
}

#[test]
fn test_time_interval1() {
    assert_eq!(
        Ok(
            ParserExpressionTreeData::TimeInterval(TimeInterval::Minutes(5.0))
        ),
        parse_str_test("5m").map(|tree| tree.tree)
    )
}

#[test]
fn test_time_interval2() {
    assert_eq!(
        Ok(
            ParserExpressionTreeData::TimeInterval(TimeInterval::Seconds(0.5))
        ),
        parse_str_test("0.5s").map(|tree| tree.tree)
    )
}