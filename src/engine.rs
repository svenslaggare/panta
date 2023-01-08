use std::ops::Add;
use std::time::{Duration};

use float_ord::FloatOrd;
use fnv::{FnvHashMap, FnvHashSet};

use crate::aggregator::{AggregateOperations, AverageAggregate, CorrelationAggregate, CovarianceAggregate, VarianceAggregate};
use crate::event::{ArithmeticOperator, BoolOperator, Event, EventExpression, EventId, EventOutputName, EventQuery, Function, ValueExpression};
use crate::metrics::{MetricDefinitions, MetricValues};

use crate::model::{EventResult, MetricId, TimeInterval, TimePoint, Value, ValueId};

pub struct EventEngine {
    next_event_id: EventId,
    next_value_id: ValueId,

    value_generators: FnvHashMap<ValueId, CompiledValueExpression>,
    expression_to_value_mapping: FnvHashMap<CompiledValueExpression, ValueId>,
    value_generators_for_metric: FnvHashMap<MetricId, Vec<ValueId>>,
    aggregators: AggregateOperations,

    events: Vec<CompiledEvent>
}

impl EventEngine {
    pub fn new() -> EventEngine {
        EventEngine {
            next_event_id: EventId(1),
            next_value_id: ValueId(1),

            value_generators: FnvHashMap::default(),
            expression_to_value_mapping: FnvHashMap::default(),
            value_generators_for_metric: FnvHashMap::default(),
            aggregators: AggregateOperations::new(),

            events: Vec::new()
        }
    }

    pub fn add_event(&mut self, event: Event) -> EventResult<EventId> {
        let mut value_generators = Vec::new();

        let context = CompileEventContext::new(&event);
        let query = self.compile_query(
            &context,
            &mut value_generators,
            event.query
        )?;
        println!("{:#?}", query);

        let outputs = self.compile_output(
            &context,
            &mut value_generators,
            event.outputs
        )?;
        println!("{:#?}", outputs);

        let event_id = self.next_event_id;
        self.next_event_id.0 += 1;

        self.events.push(
            CompiledEvent {
                id: event_id,
                independent_metric: event.independent_metric,
                dependent_metric: event.dependent_metric,
                query,
                outputs
            }
        );

        for (value_id, used_metrics) in value_generators {
            println!("{:?}: {:?}", self.value_generators[&value_id], used_metrics);

            if !used_metrics.is_empty() {
                for metric in used_metrics {
                    self.value_generators_for_metric
                        .entry(metric)
                        .or_insert_with(|| Vec::new())
                        .push(value_id);
                }
            } else {
                self.value_generators_for_metric
                    .entry(event.independent_metric)
                    .or_insert_with(|| Vec::new())
                    .push(value_id);

                self.value_generators_for_metric
                    .entry(event.dependent_metric)
                    .or_insert_with(|| Vec::new())
                    .push(value_id);
            }
        }

        Ok(event_id)
    }

    fn compile_query(&mut self,
                     context: &CompileEventContext,
                     value_generators: &mut Vec<(ValueId, Vec<MetricId>)>,
                     query: EventQuery) -> EventResult<CompiledEventQuery> {
        match query {
            EventQuery::Expression(expression) => {
                Ok(
                    CompiledEventQuery::Expression(
                        self.compile_expression(context, value_generators, expression)?
                    )
                )
            }
            EventQuery::Bool { operator, left, right, } => {
                let left = self.compile_query(context, value_generators, *left)?;
                let right = self.compile_query(context, value_generators, *right)?;

                Ok(
                    CompiledEventQuery::Bool {
                        left: Box::new(left),
                        right: Box::new(right),
                        operator
                    }
                )
            }
            EventQuery::And { left, right } => {
                let left = self.compile_query(context, value_generators, *left)?;
                let right = self.compile_query(context, value_generators, *right)?;

                Ok(
                    CompiledEventQuery::And {
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                )
            }
            EventQuery::Or { left, right } => {
                let left = self.compile_query(context, value_generators, *left)?;
                let right = self.compile_query(context, value_generators, *right)?;

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
                      outputs: Vec<(EventOutputName, EventExpression)>) -> EventResult<Vec<(EventOutputName, CompiledEventExpression)>> {
        let mut compiled_output = Vec::new();
        for (name, expression) in outputs {
            compiled_output.push((name, self.compile_expression(context, value_generators, expression)?));
        }

        Ok(compiled_output)
    }

    fn compile_expression(&mut self,
                          context: &CompileEventContext,
                          value_generators: &mut Vec<(ValueId, Vec<MetricId>)>,
                          expression: EventExpression) -> EventResult<CompiledEventExpression> {
        match expression {
            EventExpression::Value(value) => {
                let value_id = self.compile_value(context, value, value_generators)?;
                Ok(CompiledEventExpression::Value(value_id))
            }
            EventExpression::Average { value, interval } => {
                let value_id = self.compile_value(context, value, value_generators)?;
                let aggregate = self.aggregators.add_average(value_id, interval);
                Ok(CompiledEventExpression::Average(aggregate))
            }
            EventExpression::Variance { value, interval } => {
                let value_id = self.compile_value(context, value, value_generators)?;
                let aggregate = self.aggregators.add_variance(value_id, interval);
                Ok(CompiledEventExpression::Variance(aggregate))
            }
            EventExpression::Covariance { left, right, interval } => {
                let mut left_context = CompiledValueExpressionContext::new(context);
                let left = CompiledValueExpression::compile(&left, &mut left_context)?;

                let mut right_context = CompiledValueExpressionContext::new(context);
                let right = CompiledValueExpression::compile(&right, &mut right_context)?;

                let left_value_id = self.add_value(left_context.used_metrics, left, value_generators);
                let right_value_id = self.add_value(right_context.used_metrics, right, value_generators);

                let aggregate = self.aggregators.add_covariance(left_value_id, right_value_id, interval);
                Ok(CompiledEventExpression::Covariance(aggregate))
            }
            EventExpression::Correlation { left, right, interval } => {
                let mut left_context = CompiledValueExpressionContext::new(context);
                let left = CompiledValueExpression::compile(&left, &mut left_context)?;

                let mut right_context = CompiledValueExpressionContext::new(context);
                let right = CompiledValueExpression::compile(&right, &mut right_context)?;

                let left_value_id = self.add_value(left_context.used_metrics, left, value_generators);
                let right_value_id = self.add_value(right_context.used_metrics, right, value_generators);

                let aggregate = self.aggregators.add_correlation(left_value_id, right_value_id, interval);
                Ok(CompiledEventExpression::Correlation(aggregate))
            }
            EventExpression::Arithmetic { operator, left, right } => {
                let left = self.compile_expression(context, value_generators, *left)?;
                let right = self.compile_expression(context, value_generators, *right)?;

                Ok(
                    CompiledEventExpression::Arithmetic {
                        left: Box::new(left),
                        right: Box::new(right),
                        operator
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
                        function,
                        arguments: compiled_arguments
                    }
                )
            }
        }
    }

    fn compile_value(&mut self,
                     context: &CompileEventContext,
                     value: ValueExpression,
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

    pub fn handle_values<F: Fn(EventId, Vec<(String, Value)>)>(&mut self,
                                                               metric_definitions: &MetricDefinitions,
                                                               time: TimePoint,
                                                               metrics: &MetricValues,
                                                               on_event: F) {
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

        for event in self.events.iter() {
            let query = &event.query;
            // println!("Event #{}: {}", event.id, self.query_to_string(&values, query).unwrap_or("N/A".to_owned()));
            if let Some(accept) = self.evaluate_query(&values, query).map(|value| value.bool()).flatten() {
                if accept {
                    on_event(
                        event.id,
                        self.evaluate_outputs(metric_definitions, &event, &values).collect()
                    );
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
                    EventOutputName::String(str) => str.clone(),
                    EventOutputName::IndependentMetricName => metric_definitions.get_name(event.independent_metric).unwrap().to_owned(),
                    EventOutputName::DependentMetricName => metric_definitions.get_name(event.dependent_metric).unwrap().to_owned(),
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
            CompiledEventExpression::Covariance(aggregate) => Some(Value::Float(self.aggregators.covariance(aggregate)?)),
            CompiledEventExpression::Correlation(aggregate) => Some(Value::Float(self.aggregators.correlation(aggregate)?)),
            CompiledEventExpression::Arithmetic { operator, left, right } => {
                let left = self.evaluate_expression(values, left)?.float()?;
                let right = self.evaluate_expression(values, right)?.float()?;
                Some(Value::Float(operator.evaluate(left, right)))
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
            CompiledEventExpression::Value(value) => Some(format!("Value({})", values.get(&value).cloned()?)),
            CompiledEventExpression::Average(aggregate) => Some(format!("Avg({})", self.aggregators.average(aggregate)?)),
            CompiledEventExpression::Variance(aggregate) => Some(format!("Var({})", self.aggregators.variance(aggregate)?)),
            CompiledEventExpression::Covariance(aggregate) => Some(format!("Cov({})", self.aggregators.covariance(aggregate)?)),
            CompiledEventExpression::Correlation(aggregate) => Some(format!("Corr({})", self.aggregators.correlation(aggregate)?)),
            CompiledEventExpression::Arithmetic { left, right, operator: operation } => {
                let left = self.expression_to_string(values, left)?;
                let right = self.expression_to_string(values, right)?;
                Some(format!("{} {} {}", left, operation, right))
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
    independent_metric: MetricId,
    dependent_metric: MetricId,
    query: CompiledEventQuery,
    outputs: Vec<(EventOutputName, CompiledEventExpression)>
}

#[derive(Debug)]
enum CompiledEventQuery {
    Expression(CompiledEventExpression),
    Bool { left: Box<CompiledEventQuery>, right: Box<CompiledEventQuery>, operator: BoolOperator },
    And { left: Box<CompiledEventQuery>, right: Box<CompiledEventQuery> },
    Or { left: Box<CompiledEventQuery>, right: Box<CompiledEventQuery> }
}

struct CompileEventContext {
    independent_metric: MetricId,
    dependent_metric: MetricId
}

impl CompileEventContext {
    pub fn new(event: &Event) -> CompileEventContext {
        CompileEventContext {
            independent_metric: event.independent_metric,
            dependent_metric: event.dependent_metric
        }
    }
}

#[derive(Debug)]
enum CompiledEventExpression {
    Value(ValueId),
    Average(AverageAggregate),
    Variance(VarianceAggregate),
    Covariance(CovarianceAggregate),
    Correlation(CorrelationAggregate),
    Arithmetic { operator: ArithmeticOperator, left: Box<CompiledEventExpression>, right: Box<CompiledEventExpression> },
    Function { function: Function, arguments: Vec<CompiledEventExpression> }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CompiledValueExpression {
    Metric(MetricId),
    Constant(FloatOrd<f64>),
    Arithmetic { operator: ArithmeticOperator, left: Box<CompiledValueExpression>, right: Box<CompiledValueExpression> },
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
            ValueExpression::Arithmetic { operator, left, right } => {
                let left = CompiledValueExpression::compile(left, context)?;
                let right = CompiledValueExpression::compile(right, context)?;

                Ok(
                    CompiledValueExpression::Arithmetic {
                        left: Box::new(left),
                        right: Box::new(right),
                        operator: operator.clone()
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
            CompiledValueExpression::Arithmetic { left, right, operator: operation } => {
                let left = left.evaluate(metrics)?;
                let right = right.evaluate(metrics)?;
                Some(operation.evaluate(left, right))
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
    let mut metric_definitions = MetricDefinitions::new();
    let x = metric_definitions.define("x");
    let y = metric_definitions.define("y");

    let mut engine = EventEngine::new();

    engine.add_event(
        Event {
            independent_metric: x,
            dependent_metric: y,
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
                    EventOutputName::String("cov".to_owned()),
                    EventExpression::Covariance {
                        left: ValueExpression::IndependentMetric,
                        right: ValueExpression::DependentMetric,
                        interval: TimeInterval::Seconds(10.0)
                    }
                )
            ]
        }
    ).unwrap();

    println!();

    let on_event = |event_index, outputs: Vec<(String, Value)>| {
        let mut output_string = String::new();
        let mut is_first = true;
        for (name, value) in outputs {
            if !is_first {
                output_string += ", ";
            } else {
                is_first = false;
            }

            output_string += &name;
            output_string += "=";
            output_string += &value.to_string();
        }

        println!("Event generated for #{}, {}", event_index, output_string);
    };

    let mut values = MetricValues::new(TimeInterval::Minutes(1.0));

    let t0 = TimePoint::now();
    values.insert(t0, x, 1.0);
    values.insert(t0, y, 10.0);
    engine.handle_values(&metric_definitions, t0, &values, on_event);

    let t1 = t0.add(Duration::from_secs_f64(2.0));
    values.insert(t1, x, 2.0);
    values.insert(t1, y, 20.0);
    engine.handle_values(&metric_definitions, t1, &values, on_event);

    let t2 = t0.add(Duration::from_secs_f64(4.0));
    values.insert(t2, x, 4.0);
    values.insert(t2, y, 40.0);
    engine.handle_values(&metric_definitions, t2, &values, on_event);
}