use float_ord::FloatOrd;
use fnv::{FnvHashMap, FnvHashSet};

use crate::aggregator::{AggregateOperations, AverageAggregate, CorrelationAggregate, CovarianceAggregate, MaxAggregate, MinAggregate, VarianceAggregate};
use crate::event::{BinaryArithmeticOperator, BoolOperator, Event, EventExpression, EventId, EventOutputName, EventQuery, Function, UnaryArithmeticOperator, ValueExpression};
use crate::metrics::{MetricDefinitions, MetricValues};

use crate::model::{EventResult, MetricId, TimePoint, Value, ValueId};

pub struct EventEngine {
    next_event_id: EventId,
    next_value_id: ValueId,
    events: Vec<(EventId, Event)>,

    value_generators: FnvHashMap<ValueId, CompiledValueExpression>,
    expression_to_value_mapping: FnvHashMap<CompiledValueExpression, ValueId>,
    value_generators_for_metric: FnvHashMap<MetricId, Vec<ValueId>>,
    aggregators: AggregateOperations,

    compiled_events: Vec<CompiledEvent>
}

impl EventEngine {
    pub fn new() -> EventEngine {
        EventEngine {
            next_event_id: EventId(1),
            next_value_id: ValueId(1),
            events: Vec::new(),

            value_generators: FnvHashMap::default(),
            expression_to_value_mapping: FnvHashMap::default(),
            value_generators_for_metric: FnvHashMap::default(),
            aggregators: AggregateOperations::new(),

            compiled_events: Vec::new()
        }
    }

    pub fn add_event(&mut self, metric_definitions: &MetricDefinitions, event: Event) -> EventResult<EventId> {
        let event_id = self.next_event_id;
        self.compile_event(metric_definitions, event_id, &event)?;
        self.events.push((event_id, event));
        self.next_event_id.0 += 1;
        Ok(event_id)
    }

    pub fn recompile_events(&mut self, metric_definitions: &MetricDefinitions) -> EventResult<()> {
        self.compiled_events.clear();

        let events = std::mem::take(&mut self.events);
        for (event_id, event) in &events {
            self.compile_event(metric_definitions, *event_id, event)?;
        }
        self.events = events;

        // TODO: remove not used values

        Ok(())
    }

    fn compile_event(&mut self, metric_definitions: &MetricDefinitions, event_id: EventId, event: &Event) -> EventResult<()> {
        let mut sub_event_id = EventId(1);
        for dependent_metric in event.dependent_metric.iter() {
            for (independent_metric_id, dependent_metric_id) in itertools::iproduct!(metric_definitions.expand(&event.independent_metric)?,
                                                                                     metric_definitions.expand(dependent_metric)?) {
                let mut value_generators = Vec::new();

                let context = CompileEventContext {
                    independent_metric: independent_metric_id,
                    dependent_metric: dependent_metric_id,
                };

                let query = self.compile_query(
                    &context,
                    &mut value_generators,
                    &event.query,
                )?;
                // println!("{:#?}", query);

                let outputs = self.compile_output(
                    &context,
                    &mut value_generators,
                    &event.outputs,
                )?;
                // println!("{:#?}", outputs);

                self.compiled_events.push(
                    CompiledEvent {
                        id: event_id,
                        sub_id: sub_event_id,
                        name: event.name.clone(),
                        independent_metric: independent_metric_id,
                        dependent_metric: dependent_metric_id,
                        query,
                        outputs,
                        output_rate: event.output_rate,
                        last_event_time: None
                    }
                );

                for (value_id, used_metrics) in value_generators {
                    // println!("{:?}: {:?}", self.value_generators[&value_id], used_metrics);

                    if !used_metrics.is_empty() {
                        for metric in used_metrics {
                            self.value_generators_for_metric
                                .entry(metric)
                                .or_insert_with(|| Vec::new())
                                .push(value_id);
                        }
                    } else {
                        self.value_generators_for_metric
                            .entry(independent_metric_id)
                            .or_insert_with(|| Vec::new())
                            .push(value_id);

                        self.value_generators_for_metric
                            .entry(dependent_metric_id)
                            .or_insert_with(|| Vec::new())
                            .push(value_id);
                    }
                }

                sub_event_id.0 += 1;
            }
        }

        Ok(())
    }

    fn compile_query(&mut self,
                     context: &CompileEventContext,
                     value_generators: &mut Vec<(ValueId, Vec<MetricId>)>,
                     query: &EventQuery) -> EventResult<CompiledEventQuery> {
        match query {
            EventQuery::Expression(expression) => {
                Ok(
                    CompiledEventQuery::Expression(
                        self.compile_expression(context, value_generators, expression)?
                    )
                )
            }
            EventQuery::Bool { operator, left, right } => {
                let left = self.compile_query(context, value_generators, left)?;
                let right = self.compile_query(context, value_generators, right)?;

                Ok(
                    CompiledEventQuery::Bool {
                        left: Box::new(left),
                        right: Box::new(right),
                        operator: *operator
                    }
                )
            }
            EventQuery::Invert { operand } => {
                let operand = self.compile_query(context, value_generators, operand)?;

                Ok(
                    CompiledEventQuery::Invert {
                        operand: Box::new(operand)
                    }
                )
            }
            EventQuery::And { left, right } => {
                let left = self.compile_query(context, value_generators, left)?;
                let right = self.compile_query(context, value_generators, right)?;

                Ok(
                    CompiledEventQuery::And {
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                )
            }
            EventQuery::Or { left, right } => {
                let left = self.compile_query(context, value_generators, left)?;
                let right = self.compile_query(context, value_generators, right)?;

                Ok(
                    CompiledEventQuery::Or {
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                )
            }
        }
    }

    fn compile_output(&mut self,
                      context: &CompileEventContext,
                      value_generators: &mut Vec<(ValueId, Vec<MetricId>)>,
                      outputs: &Vec<(EventOutputName, EventExpression)>) -> EventResult<Vec<(EventOutputName, CompiledEventExpression)>> {
        let mut compiled_output = Vec::new();
        for (name, expression) in outputs {
            compiled_output.push((name.clone(), self.compile_expression(context, value_generators, expression)?));
        }

        Ok(compiled_output)
    }

    fn compile_expression(&mut self,
                          context: &CompileEventContext,
                          value_generators: &mut Vec<(ValueId, Vec<MetricId>)>,
                          expression: &EventExpression) -> EventResult<CompiledEventExpression> {
        match expression {
            EventExpression::Value(value) => {
                let value_id = self.compile_value(context, value, value_generators)?;
                Ok(CompiledEventExpression::Value(value_id))
            }
            EventExpression::Average { value, interval } => {
                let value_id = self.compile_value(context, value, value_generators)?;
                let aggregate = self.aggregators.add_average(value_id, *interval);
                Ok(CompiledEventExpression::Average(aggregate))
            }
            EventExpression::Variance { value, interval } => {
                let value_id = self.compile_value(context, value, value_generators)?;
                let aggregate = self.aggregators.add_variance(value_id, *interval);
                Ok(CompiledEventExpression::Variance(aggregate))
            }
            EventExpression::StandardDeviation { value, interval } => {
                let value_id = self.compile_value(context, value, value_generators)?;
                let aggregate = self.aggregators.add_variance(value_id, *interval);
                Ok(CompiledEventExpression::StandardDeviation(aggregate))
            }
            EventExpression::Covariance { left, right, interval } => {
                let mut left_context = CompiledValueExpressionContext::new(context);
                let left = CompiledValueExpression::compile(&left, &mut left_context)?;

                let mut right_context = CompiledValueExpressionContext::new(context);
                let right = CompiledValueExpression::compile(&right, &mut right_context)?;

                let left_value_id = self.add_value(left_context.used_metrics, left, value_generators);
                let right_value_id = self.add_value(right_context.used_metrics, right, value_generators);

                let aggregate = self.aggregators.add_covariance(left_value_id, right_value_id, *interval);
                Ok(CompiledEventExpression::Covariance(aggregate))
            }
            EventExpression::Correlation { left, right, interval } => {
                let mut left_context = CompiledValueExpressionContext::new(context);
                let left = CompiledValueExpression::compile(&left, &mut left_context)?;

                let mut right_context = CompiledValueExpressionContext::new(context);
                let right = CompiledValueExpression::compile(&right, &mut right_context)?;

                let left_value_id = self.add_value(left_context.used_metrics, left, value_generators);
                let right_value_id = self.add_value(right_context.used_metrics, right, value_generators);

                let aggregate = self.aggregators.add_correlation(left_value_id, right_value_id, *interval);
                Ok(CompiledEventExpression::Correlation(aggregate))
            }
            EventExpression::Min { value, interval } => {
                let value_id = self.compile_value(context, value, value_generators)?;
                let aggregate = self.aggregators.add_min(value_id, *interval);
                Ok(CompiledEventExpression::Min(aggregate))
            }
            EventExpression::Max { value, interval } => {
                let value_id = self.compile_value(context, value, value_generators)?;
                let aggregate = self.aggregators.add_max(value_id, *interval);
                Ok(CompiledEventExpression::Max(aggregate))
            }
            EventExpression::BinaryArithmetic { operator, left, right } => {
                let left = self.compile_expression(context, value_generators, left)?;
                let right = self.compile_expression(context, value_generators, right)?;

                Ok(
                    CompiledEventExpression::BinaryArithmetic {
                        left: Box::new(left),
                        right: Box::new(right),
                        operator: *operator
                    }
                )
            }
            EventExpression::UnaryArithmetic { operator, operand } => {
                let operand = self.compile_expression(context, value_generators, operand)?;

                Ok(
                    CompiledEventExpression::UnaryArithmetic {
                        operator: *operator,
                        operand: Box::new(operand)
                    }
                )
            }
            EventExpression::Function { function, arguments } => {
                let mut compiled_arguments = Vec::new();
                for argument in arguments {
                    compiled_arguments.push(self.compile_expression(context, value_generators, argument)?);
                }

                Ok(
                    CompiledEventExpression::Function {
                        function: *function,
                        arguments: compiled_arguments
                    }
                )
            }
        }
    }

    fn compile_value(&mut self,
                     context: &CompileEventContext,
                     value: &ValueExpression,
                     value_generators: &mut Vec<(ValueId, Vec<MetricId>)>) ->  EventResult<ValueId> {
        let mut context = CompiledValueExpressionContext::new(context);
        let value = CompiledValueExpression::compile(&value, &mut context)?;
        Ok(self.add_value(context.used_metrics, value, value_generators))
    }

    fn add_value(&mut self,
                 used_metrics: FnvHashSet<MetricId>,
                 value: CompiledValueExpression,
                 value_generators: &mut Vec<(ValueId, Vec<MetricId>)>) -> ValueId {
        let used_metrics = used_metrics.into_iter().collect();
        if let Some(existing_value_id) = self.expression_to_value_mapping.get(&value) {
            value_generators.push((*existing_value_id, used_metrics));
            return *existing_value_id;
        }

        let value_id = self.next_value_id;
        value_generators.push((value_id, used_metrics));
        self.value_generators.insert(value_id, value.clone());
        self.expression_to_value_mapping.insert(value, value_id);
        self.next_value_id.0 += 1;
        value_id
    }

    pub fn handle_values<F: FnMut(EventId, EventId, &str, Vec<(String, Value)>)>(&mut self,
                                                                                 metric_definitions: &MetricDefinitions,
                                                                                 time: TimePoint,
                                                                                 metrics: &MetricValues,
                                                                                 mut on_event: F) {
        let mut values_to_compute = FnvHashSet::default();
        for metric in metrics.keys() {
            if let Some(generators) = self.value_generators_for_metric.get(&metric) {
                values_to_compute.extend(generators);
            }
        }

        let mut values = FnvHashMap::default();
        for value_id in values_to_compute {
            let generator = &self.value_generators[value_id];
            if let Some(generated) = generator.evaluate(&metrics) {
                *values.entry(*value_id).or_insert(generated) = generated;
            }
        }

        self.aggregators.handle_values(time, &values);

        for event_index in 0..self.compiled_events.len() {
            let event = &self.compiled_events[event_index];
            let query = &event.query;
            // println!("Event #{}.{}: {}", event.id, event.sub_id, self.query_to_string(&values, query).unwrap_or("N/A".to_owned()));
            if let Some(accept) = self.evaluate_query(&values, query).map(|value| value.bool()).flatten() {
                if accept {
                    if event.should_generate_event(time) {
                        on_event(
                            event.id,
                            event.sub_id,
                            &event.name,
                            self.evaluate_outputs(metric_definitions, &event, &values).collect()
                        );
                        self.compiled_events[event_index].last_event_time = Some(time);
                    }
                }
            }
        }

        // println!()
    }

    fn evaluate_query(&self,
                      values: &Values,
                      query: &CompiledEventQuery) -> Option<Value> {
        match query {
            CompiledEventQuery::Expression(expression) => {
                self.evaluate_expression(values, expression)
            }
            CompiledEventQuery::Bool { operator, left, right } => {
                let left = self.evaluate_query(values, left)?;
                let right = self.evaluate_query(values, right)?;
                Some(Value::Bool(operator.evaluate(&left, &right)?))
            }
            CompiledEventQuery::Invert { operand } => {
                let operand = self.evaluate_query(values, operand)?;
                Some(Value::Bool(!operand.bool()?))
            }
            CompiledEventQuery::And { left, right } => {
                Some(Value::Bool(self.evaluate_query(values, left)?.bool()? && self.evaluate_query(values, right)?.bool()?))
            }
            CompiledEventQuery::Or { left, right } => {
                Some(Value::Bool(self.evaluate_query(values, left)?.bool()? || self.evaluate_query(values, right)?.bool()?))
            }
        }
    }

    fn evaluate_outputs<'a>(&'a self,
                            metric_definitions: &'a MetricDefinitions,
                            event: &'a CompiledEvent,
                            values: &'a Values) -> impl Iterator<Item=(String, Value)> + 'a {
        event.outputs
            .iter()
            .map(|(name, expression)| {
                let name = match name {
                    EventOutputName::Text(str) => str.clone(),
                    EventOutputName::IndependentMetricName => metric_definitions.get_specific_name(event.independent_metric).unwrap().to_string(),
                    EventOutputName::DependentMetricName => metric_definitions.get_specific_name(event.dependent_metric).unwrap().to_string(),
                };

                self.evaluate_expression(values, expression).map(|value| (name, value))
            })
            .flatten()
    }

    fn evaluate_expression(&self,
                           values: &Values,
                           expression: &CompiledEventExpression) -> Option<Value> {
        match expression {
            CompiledEventExpression::Value(value) => Some(Value::Float(values.get(&value).cloned()?)),
            CompiledEventExpression::Average(aggregate) => Some(Value::Float(self.aggregators.average(aggregate)?)),
            CompiledEventExpression::Variance(aggregate) => Some(Value::Float(self.aggregators.variance(aggregate)?)),
            CompiledEventExpression::StandardDeviation(aggregate) => Some(Value::Float(self.aggregators.standard_deviation(aggregate)?)),
            CompiledEventExpression::Covariance(aggregate) => Some(Value::Float(self.aggregators.covariance(aggregate)?)),
            CompiledEventExpression::Correlation(aggregate) => Some(Value::Float(self.aggregators.correlation(aggregate)?)),
            CompiledEventExpression::Min(aggregate) => Some(Value::Float(self.aggregators.min(aggregate)?)),
            CompiledEventExpression::Max(aggregate) => Some(Value::Float(self.aggregators.max(aggregate)?)),
            CompiledEventExpression::BinaryArithmetic { operator, left, right } => {
                let left = self.evaluate_expression(values, left)?.float()?;
                let right = self.evaluate_expression(values, right)?.float()?;
                Some(Value::Float(operator.evaluate(left, right)))
            }
            CompiledEventExpression::UnaryArithmetic { operator, operand } => {
                let operand = self.evaluate_expression(values, operand)?.float()?;
                Some(Value::Float(operator.evaluate(operand)))
            }
            CompiledEventExpression::Function { function, arguments } => {
                let mut evaluated_arguments = Vec::new();
                for argument in arguments {
                    evaluated_arguments.push(self.evaluate_expression(values, argument)?.float()?);
                }

                Some(Value::Float(function.evaluate(&evaluated_arguments)?))
            }
        }
    }

    fn query_to_string(&self,
                       values: &Values,
                       query: &CompiledEventQuery) -> Option<String> {
        match query {
            CompiledEventQuery::Expression(expression) => {
                Some(self.expression_to_string(values, expression)?)
            }
            CompiledEventQuery::Bool { operator, left, right } => {
                let left = self.query_to_string(values, left)?;
                let right = self.query_to_string(values, right)?;
                Some(format!("{} {} {}", left, operator, right))
            }
            CompiledEventQuery::Invert { operand } => {
                let operand = self.query_to_string(values, operand)?;
                Some(format!("!{}", operand))
            }
            CompiledEventQuery::And { left, right } => {
                let left = self.query_to_string(values, left)?;
                let right = self.query_to_string(values, right)?;
                Some(format!("{}) && {}", left, right))
            }
            CompiledEventQuery::Or { left, right } => {
                let left = self.query_to_string(values, left)?;
                let right = self.query_to_string(values, right)?;
                Some(format!("{} || {}", left, right))
            }
        }
    }

    fn expression_to_string(&self,
                            values: &FnvHashMap<ValueId, f64>,
                            expression: &CompiledEventExpression) -> Option<String> {
        match expression {
            CompiledEventExpression::Value(value) => Some(format!("{}", values.get(&value).cloned()?)),
            CompiledEventExpression::Average(aggregate) => Some(format!("avg({})", self.aggregators.average(aggregate)?)),
            CompiledEventExpression::Variance(aggregate) => Some(format!("var({})", self.aggregators.variance(aggregate)?)),
            CompiledEventExpression::StandardDeviation(aggregate) => Some(format!("std({})", self.aggregators.standard_deviation(aggregate)?)),
            CompiledEventExpression::Covariance(aggregate) => Some(format!("cov({})", self.aggregators.covariance(aggregate)?)),
            CompiledEventExpression::Correlation(aggregate) => Some(format!("corr({})", self.aggregators.correlation(aggregate)?)),
            CompiledEventExpression::Min(aggregate) => Some(format!("min({})", self.aggregators.min(aggregate)?)),
            CompiledEventExpression::Max(aggregate) => Some(format!("max({})", self.aggregators.max(aggregate)?)),
            CompiledEventExpression::BinaryArithmetic { operator, left, right } => {
                let left = self.expression_to_string(values, left)?;
                let right = self.expression_to_string(values, right)?;
                Some(format!("{} {} {}", left, operator, right))
            }
            CompiledEventExpression::UnaryArithmetic { operator, operand } => {
                let operand = self.expression_to_string(values, operand)?;
                Some(format!("{}{}", operator, operand))
            }
            CompiledEventExpression::Function { function, arguments } => {
                let mut transformed_arguments = Vec::new();
                for argument in arguments {
                    transformed_arguments.push(self.expression_to_string(values, argument)?);
                }
                Some(format!("{}({})", function, transformed_arguments.join(", ")))
            }
        }
    }
}

type Values = FnvHashMap<ValueId, f64>;

#[derive(Debug)]
struct CompiledEvent {
    id: EventId,
    sub_id: EventId,
    name: String,
    independent_metric: MetricId,
    dependent_metric: MetricId,
    query: CompiledEventQuery,
    outputs: Vec<(EventOutputName, CompiledEventExpression)>,
    output_rate: Option<f64>,
    last_event_time: Option<TimePoint>
}

impl CompiledEvent {
    pub fn should_generate_event(&self, time: TimePoint) -> bool {
        match (self.output_rate, self.last_event_time) {
            (Some(output_rate), Some(last_event_time)) => {
                (time - last_event_time).as_secs_f64() > 1.0 / output_rate
            }
            _ => true
        }
    }
}

#[derive(Debug)]
enum CompiledEventQuery {
    Expression(CompiledEventExpression),
    Bool { left: Box<CompiledEventQuery>, right: Box<CompiledEventQuery>, operator: BoolOperator },
    Invert { operand: Box<CompiledEventQuery> },
    And { left: Box<CompiledEventQuery>, right: Box<CompiledEventQuery> },
    Or { left: Box<CompiledEventQuery>, right: Box<CompiledEventQuery> }
}

struct CompileEventContext {
    independent_metric: MetricId,
    dependent_metric: MetricId
}

#[derive(Debug)]
enum CompiledEventExpression {
    Value(ValueId),
    Average(AverageAggregate),
    Variance(VarianceAggregate),
    StandardDeviation(VarianceAggregate),
    Covariance(CovarianceAggregate),
    Correlation(CorrelationAggregate),
    Min(MinAggregate),
    Max(MaxAggregate),
    BinaryArithmetic { operator: BinaryArithmeticOperator, left: Box<CompiledEventExpression>, right: Box<CompiledEventExpression> },
    UnaryArithmetic { operator: UnaryArithmeticOperator, operand: Box<CompiledEventExpression> },
    Function { function: Function, arguments: Vec<CompiledEventExpression> }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CompiledValueExpression {
    Metric(MetricId),
    Constant(FloatOrd<f64>),
    BinaryArithmetic { operator: BinaryArithmeticOperator, left: Box<CompiledValueExpression>, right: Box<CompiledValueExpression> },
    UnaryArithmetic { operator: UnaryArithmeticOperator, operand: Box<CompiledValueExpression> },
    Function { function: Function, arguments: Vec<CompiledValueExpression> }
}

impl CompiledValueExpression {
    pub fn compile(expression: &ValueExpression, context: &mut CompiledValueExpressionContext) -> EventResult<CompiledValueExpression> {
        match expression {
            ValueExpression::IndependentMetric => {
                context.used_metrics.insert(context.independent_metric);
                Ok(CompiledValueExpression::Metric(context.independent_metric))
            }
            ValueExpression::DependentMetric => {
                context.used_metrics.insert(context.dependent_metric);
                Ok(CompiledValueExpression::Metric(context.dependent_metric))
            }
            ValueExpression::Constant(value) => {
                Ok(CompiledValueExpression::Constant(FloatOrd(*value)))
            }
            ValueExpression::BinaryArithmetic { operator, left, right } => {
                let left = CompiledValueExpression::compile(left, context)?;
                let right = CompiledValueExpression::compile(right, context)?;

                Ok(
                    CompiledValueExpression::BinaryArithmetic {
                        operator: operator.clone(),
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                )
            }
            ValueExpression::UnaryArithmetic { operator, operand } => {
                let operand = CompiledValueExpression::compile(operand, context)?;

                Ok(
                    CompiledValueExpression::UnaryArithmetic {
                        operator: operator.clone(),
                        operand: Box::new(operand)
                    }
                )
            }
            ValueExpression::Function { function, arguments } => {
                let mut compiled_arguments = Vec::new();
                for argument in arguments {
                    compiled_arguments.push(CompiledValueExpression::compile(argument, context)?);
                }

                Ok(
                    CompiledValueExpression::Function {
                        function: function.clone(),
                        arguments: compiled_arguments
                    }
                )
            }
        }
    }

    pub fn evaluate(&self, metrics: &MetricValues) -> Option<f64> {
        match self {
            CompiledValueExpression::Metric(metric) => metrics.get(metric).cloned(),
            CompiledValueExpression::Constant(value) => Some(value.0),
            CompiledValueExpression::BinaryArithmetic { left, right, operator } => {
                let left = left.evaluate(metrics)?;
                let right = right.evaluate(metrics)?;
                Some(operator.evaluate(left, right))
            }
            CompiledValueExpression::UnaryArithmetic { operator, operand } => {
                let operand = operand.evaluate(metrics)?;
                Some(operator.evaluate(operand))
            }
            CompiledValueExpression::Function { function, arguments } => {
                let mut evaluated_arguments = Vec::new();
                for argument in arguments {
                    evaluated_arguments.push(argument.evaluate(metrics)?);
                }

                function.evaluate(&evaluated_arguments)
            }
        }
    }
}

struct CompiledValueExpressionContext {
    independent_metric: MetricId,
    dependent_metric: MetricId,
    used_metrics: FnvHashSet<MetricId>
} 

impl CompiledValueExpressionContext {
    pub fn new(context: &CompileEventContext) -> CompiledValueExpressionContext {
        CompiledValueExpressionContext {
            independent_metric: context.independent_metric,
            dependent_metric: context.dependent_metric,
            used_metrics: FnvHashSet::default()
        }
    }
}

#[test]
fn test_event_engine1() {
    use std::cell::RefCell;
    use std::ops::Add;
    use std::rc::Rc;
    use std::time::{Duration};

    use assert_approx_eq::assert_approx_eq;

    use crate::model::{MetricName, TimeInterval};

    let mut metric_definitions = MetricDefinitions::new();
    let x = metric_definitions.define(MetricName::all("x"));
    let y = metric_definitions.define(MetricName::all("y"));

    let mut engine = EventEngine::new();

    engine.add_event(
        &metric_definitions,
        Event {
            name: "test".to_owned(),
            independent_metric: MetricName::all("x"),
            dependent_metric: vec![MetricName::all("y")],
            query: EventQuery::And {
                left: Box::new(
                    EventQuery::Bool {
                        left: Box::new(
                            EventQuery::Expression(
                                EventExpression::Covariance {
                                    left: ValueExpression::IndependentMetric,
                                    right: ValueExpression::DependentMetric,
                                    interval: TimeInterval::Seconds(10.0)
                                }
                            )
                        ),
                        right: Box::new(
                            EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(0.0)))
                        ),
                        operator: BoolOperator::GreaterThan
                    }
                ),
                right: Box::new(
                    EventQuery::Bool {
                        left: Box::new(
                            EventQuery::Expression(
                                EventExpression::Average {
                                    value: ValueExpression::IndependentMetric,
                                    interval: TimeInterval::Seconds(10.0)
                                }
                            )
                        ),
                        right: Box::new(
                            EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(0.0)))
                        ),
                        operator: BoolOperator::GreaterThan
                    }
                )
            },
            outputs: vec![
                (
                    EventOutputName::IndependentMetricName,
                    EventExpression::Average { value: ValueExpression::IndependentMetric, interval: TimeInterval::Seconds(10.0) }
                ),
                (
                    EventOutputName::DependentMetricName,
                    EventExpression::Average { value: ValueExpression::DependentMetric, interval: TimeInterval::Seconds(10.0) }
                ),
                (
                    EventOutputName::Text("cov".to_owned()),
                    EventExpression::Covariance {
                        left: ValueExpression::IndependentMetric,
                        right: ValueExpression::DependentMetric,
                        interval: TimeInterval::Seconds(10.0)
                    }
                )
            ],
            output_rate: None
        }
    ).unwrap();
    engine.recompile_events(&metric_definitions).unwrap();

    println!();

    let events = Rc::new(RefCell::new(Vec::new()));
    let on_event = |event_id, _, name: &str, outputs: Vec<(String, Value)>| {
        events.borrow_mut().push(outputs.clone());
        print_output_for_test(event_id, name, &outputs);
    };

    let mut values = MetricValues::new(TimeInterval::Minutes(1.0));

    let t0 = TimePoint::now();
    values.insert(t0, x, 1.0);
    values.insert(t0, y, 10.0);
    engine.handle_values(&metric_definitions, t0, &values, on_event);

    assert_eq!(0, events.borrow_mut().len());
    events.borrow_mut().clear();


    let t1 = t0.add(Duration::from_secs_f64(2.0));
    values.insert(t1, x, 2.0);
    values.insert(t1, y, 20.0);
    engine.handle_values(&metric_definitions, t1, &values, on_event);

    assert_eq!(1, events.borrow_mut().len());
    let outputs = events.borrow_mut().remove(0);
    assert_eq!("x", outputs[0].0); assert_approx_eq!(1.5, outputs[0].1.float().unwrap());
    assert_eq!("y", outputs[1].0); assert_approx_eq!(15.0, outputs[1].1.float().unwrap());
    assert_eq!("cov", outputs[2].0); assert_approx_eq!(2.5, outputs[2].1.float().unwrap());
    events.borrow_mut().clear();


    let t2 = t0.add(Duration::from_secs_f64(4.0));
    values.insert(t2, x, 4.0);
    values.insert(t2, y, 40.0);
    engine.handle_values(&metric_definitions, t2, &values, on_event);

    assert_eq!(1, events.borrow_mut().len());
    let outputs = events.borrow_mut().remove(0);
    assert_eq!("x", outputs[0].0); assert_approx_eq!(2.3333333, outputs[0].1.float().unwrap());
    assert_eq!("y", outputs[1].0); assert_approx_eq!(23.333333, outputs[1].1.float().unwrap());
    assert_eq!("cov", outputs[2].0); assert_approx_eq!(15.555555, outputs[2].1.float().unwrap());
    events.borrow_mut().clear();
}

#[test]
fn test_event_engine2() {
    use std::cell::RefCell;
    use std::ops::Add;
    use std::rc::Rc;
    use std::time::{Duration};

    use assert_approx_eq::assert_approx_eq;

    use crate::model::{MetricName, TimeInterval};

    let mut metric_definitions = MetricDefinitions::new();
    let x = metric_definitions.define(MetricName::all("x"));
    let y = metric_definitions.define(MetricName::all("y"));

    let mut engine = EventEngine::new();

    engine.add_event(
        &metric_definitions,
        Event {
            name: "test".to_owned(),
            independent_metric: MetricName::all("x"),
            dependent_metric: vec![MetricName::all("y")],
            query: EventQuery::And {
                left: Box::new(
                    EventQuery::Bool {
                        left: Box::new(
                            EventQuery::Expression(
                                EventExpression::Covariance {
                                    left: ValueExpression::IndependentMetric,
                                    right: ValueExpression::DependentMetric,
                                    interval: TimeInterval::Seconds(10.0)
                                }
                            )
                        ),
                        right: Box::new(
                            EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(0.0)))
                        ),
                        operator: BoolOperator::GreaterThan
                    }
                ),
                right: Box::new(
                    EventQuery::Bool {
                        left: Box::new(
                            EventQuery::Expression(
                                EventExpression::Average {
                                    value: ValueExpression::IndependentMetric,
                                    interval: TimeInterval::Seconds(10.0)
                                }
                            )
                        ),
                        right: Box::new(
                            EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(0.0)))
                        ),
                        operator: BoolOperator::GreaterThan
                    }
                )
            },
            outputs: vec![
                (
                    EventOutputName::IndependentMetricName,
                    EventExpression::Average { value: ValueExpression::IndependentMetric, interval: TimeInterval::Seconds(10.0) }
                ),
                (
                    EventOutputName::DependentMetricName,
                    EventExpression::Average { value: ValueExpression::DependentMetric, interval: TimeInterval::Seconds(10.0) }
                ),
                (
                    EventOutputName::Text("cov".to_owned()),
                    EventExpression::Covariance {
                        left: ValueExpression::IndependentMetric,
                        right: ValueExpression::DependentMetric,
                        interval: TimeInterval::Seconds(10.0)
                    }
                )
            ],
            output_rate: None
        }
    ).unwrap();

    println!();

    let events = Rc::new(RefCell::new(Vec::new()));
    let on_event = |event_id, _, name: &str, outputs: Vec<(String, Value)>| {
        events.borrow_mut().push(outputs.clone());
        print_output_for_test(event_id, name, &outputs);
    };

    let mut values = MetricValues::new(TimeInterval::Minutes(1.0));

    let t0 = TimePoint::now();
    values.insert(t0, x, 1.0);
    values.insert(t0, y, 10.0);
    engine.handle_values(&metric_definitions, t0, &values, on_event);

    assert_eq!(0, events.borrow_mut().len());
    events.borrow_mut().clear();


    let t1 = t0.add(Duration::from_secs_f64(2.0));
    values.insert(t1, x, 2.0);
    values.insert(t1, y, 20.0);
    engine.handle_values(&metric_definitions, t1, &values, on_event);

    assert_eq!(1, events.borrow_mut().len());
    let outputs = events.borrow_mut().remove(0);
    assert_eq!("x", outputs[0].0); assert_approx_eq!(1.5, outputs[0].1.float().unwrap());
    assert_eq!("y", outputs[1].0); assert_approx_eq!(15.0, outputs[1].1.float().unwrap());
    assert_eq!("cov", outputs[2].0); assert_approx_eq!(2.5, outputs[2].1.float().unwrap());
    events.borrow_mut().clear();


    let t2 = t0.add(Duration::from_secs_f64(4.0));
    values.insert(t2, x, 4.0);
    values.insert(t2, y, 40.0);
    engine.handle_values(&metric_definitions, t2, &values, on_event);

    assert_eq!(1, events.borrow_mut().len());
    let outputs = events.borrow_mut().remove(0);
    assert_eq!("x", outputs[0].0); assert_approx_eq!(2.3333333, outputs[0].1.float().unwrap());
    assert_eq!("y", outputs[1].0); assert_approx_eq!(23.333333, outputs[1].1.float().unwrap());
    assert_eq!("cov", outputs[2].0); assert_approx_eq!(15.555555, outputs[2].1.float().unwrap());
    events.borrow_mut().clear();
}

#[cfg(test)]
fn print_output_for_test(event_id: EventId, name: &str, outputs: &Vec<(String, Value)>) {
    let output_string = crate::event_output::join_event_output(outputs);
    println!("Event generated for {} (#{}), {}", event_id, name, output_string);
}