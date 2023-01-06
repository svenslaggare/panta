use std::ops::Add;
use std::time::{Duration, Instant};

use float_ord::FloatOrd;
use fnv::{FnvHashMap, FnvHashSet};

use crate::aggregator::{AggregateOperations, AverageAggregate, CovarianceAggregate, VarianceAggregate};
use crate::event::{ArithmeticOperator, BoolOperator, Event, EventExpression, EventQuery, ValueExpression};

use crate::model::{ EventResult, MetricId, TimeInterval, Value, ValueId};

pub struct EventEngine {
    next_metric_id: MetricId,
    next_value_id: ValueId,
    metrics: FnvHashMap<String, MetricId>,
    metric_id_to_name_mapping: FnvHashMap<MetricId, String>,

    value_generators: FnvHashMap<ValueId, CompiledValueExpression>,
    expression_to_value_mapping: FnvHashMap<CompiledValueExpression, ValueId>,
    value_generators_for_metric: FnvHashMap<MetricId, Vec<ValueId>>,
    aggregators: AggregateOperations,

    events: Vec<CompiledEvent>
}

impl EventEngine {
    pub fn new() -> EventEngine {
        EventEngine {
            next_metric_id: MetricId(1),
            next_value_id: ValueId(1),

            metrics: FnvHashMap::default(),
            metric_id_to_name_mapping: FnvHashMap::default(),

            value_generators: FnvHashMap::default(),
            expression_to_value_mapping: FnvHashMap::default(),
            value_generators_for_metric: FnvHashMap::default(),
            aggregators: AggregateOperations::new(),

            events: Vec::new()
        }
    }

    pub fn define_metric(&mut self, name: &str) -> MetricId {
        *self.metrics
            .entry(name.to_owned())
            .or_insert_with(|| {
                let metric_id = self.next_metric_id;
                self.metric_id_to_name_mapping.insert(metric_id, name.to_owned());
                self.next_metric_id.0 += 1;
                metric_id
            })
    }

    pub fn add_event(&mut self, event: Event) -> EventResult<()> {
        let mut value_generators = Vec::new();

        let query = self.compile_query(
            &CompileEventQueryContext::new(&event),
            &mut value_generators,
            event.query
        )?;
        println!("{:#?}", query);
        self.events.push(
            CompiledEvent {
                independent_metric: event.independent_metric,
                dependent_metric: event.dependent_metric,
                query
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

        Ok(())
    }

    fn compile_query(&mut self,
                     context: &CompileEventQueryContext,
                     value_generators: &mut Vec<(ValueId, Vec<MetricId>)>,
                     query: EventQuery) -> EventResult<CompiledEventQuery> {
        match query {
            EventQuery::Expression(expression) => {
                self.compile_expression(context, value_generators, expression)
            }
            EventQuery::Bool { left, right, operation } => {
                let left = self.compile_query(context, value_generators, *left)?;
                let right = self.compile_query(context, value_generators, *right)?;

                Ok(
                    CompiledEventQuery::Bool {
                        left: Box::new(left),
                        right: Box::new(right),
                        operation
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

    fn compile_expression(&mut self,
                          context: &CompileEventQueryContext,
                          value_generators: &mut Vec<(ValueId, Vec<MetricId>)>,
                          expression: EventExpression) -> EventResult<CompiledEventQuery> {
        match expression {
            EventExpression::Value(value) => {
                let value_id = self.compile_value(context, value, value_generators)?;
                Ok(CompiledEventQuery::Value(value_id))
            }
            EventExpression::Average { value, time_interval } => {
                let value_id = self.compile_value(context, value, value_generators)?;
                let aggregate = self.aggregators.add_average(value_id, time_interval);
                Ok(CompiledEventQuery::Average(aggregate))
            }
            EventExpression::Variance { value, time_interval } => {
                let value_id = self.compile_value(context, value, value_generators)?;
                let aggregate = self.aggregators.add_variance(value_id, time_interval);
                Ok(CompiledEventQuery::Variance(aggregate))
            }
            EventExpression::Covariance { left, right, time_interval } => {
                let mut left_context = CompiledValueExpressionContext::new(context);
                let left = CompiledValueExpression::compile(&left, &mut left_context)?;

                let mut right_context = CompiledValueExpressionContext::new(context);
                let right = CompiledValueExpression::compile(&right, &mut right_context)?;

                let left_value_id = self.add_value(left_context.used_metrics, left, value_generators);
                let right_value_id = self.add_value(right_context.used_metrics, right, value_generators);

                let aggregate = self.aggregators.add_covariance(left_value_id, right_value_id, time_interval);
                Ok(CompiledEventQuery::Covariance(aggregate))
            }
            EventExpression::Arithmetic { left, right, operation } => {
                let left = self.compile_expression(context, value_generators, *left)?;
                let right = self.compile_expression(context, value_generators, *right)?;

                Ok(
                    CompiledEventQuery::Arithmetic {
                        left: Box::new(left),
                        right: Box::new(right),
                        operation
                    }
                )
            }
        }
    }

    fn compile_value(&mut self,
                     context: &CompileEventQueryContext,
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

    pub fn handle_values(&mut self,
                         time: Instant,
                         metrics: &FnvHashMap<MetricId, f64>) {
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

        for (query_index, event) in self.events.iter().enumerate() {
            let query = &event.query;
            println!("Event #{}: {}", query_index, self.query_to_string(&values, query).unwrap_or("N/A".to_owned()));
            if let Some(value) = self.evaluate_query(&values, query).map(|value| value.bool()).flatten() {
                if value {
                    println!(
                        "Event generated for #{}, {}={}, {}={} [{}]",
                        query_index,
                        self.metric_id_to_name_mapping[&event.independent_metric],
                        metrics[&event.independent_metric],
                        self.metric_id_to_name_mapping[&event.dependent_metric],
                        metrics[&event.dependent_metric],
                        self.query_to_string(&values, query).unwrap_or("N/A".to_owned())
                    );
                }
            }
        }

        println!()
    }

    fn evaluate_query(&self,
                      values: &FnvHashMap<ValueId, f64>,
                      query: &CompiledEventQuery) -> Option<Value> {
        match query {
            CompiledEventQuery::Value(value) => Some(Value::Float(values.get(&value).cloned()?)),
            CompiledEventQuery::Average(aggregate) => Some(Value::Float(self.aggregators.average(aggregate)?)),
            CompiledEventQuery::Variance(aggregate) => Some(Value::Float(self.aggregators.variance(aggregate)?)),
            CompiledEventQuery::Covariance(aggregate) => Some(Value::Float(self.aggregators.covariance(aggregate)?)),
            CompiledEventQuery::Arithmetic { left, right, operation } => {
                let left = self.evaluate_query(values, left)?.float()?;
                let right = self.evaluate_query(values, right)?.float()?;
                Some(Value::Float(operation.evaluate(left, right)))
            }
            CompiledEventQuery::Bool { left, right, operation } => {
                let left = self.evaluate_query(values, left)?;
                let right = self.evaluate_query(values, right)?;
                Some(Value::Bool(operation.evaluate(&left, &right)?))
            }
            CompiledEventQuery::And { left, right } => {
                Some(Value::Bool(self.evaluate_query(values, left)?.bool()? && self.evaluate_query(values, right)?.bool()?))
            }
            CompiledEventQuery::Or { left, right } => {
                Some(Value::Bool(self.evaluate_query(values, left)?.bool()? || self.evaluate_query(values, right)?.bool()?))
            }
        }
    }

    fn query_to_string(&self,
                       values: &FnvHashMap<ValueId, f64>,
                       query: &CompiledEventQuery) -> Option<String> {
        match query {
            CompiledEventQuery::Value(value) => Some(format!("Value({})", values.get(&value).cloned()?)),
            CompiledEventQuery::Average(aggregate) => Some(format!("Avg({})", self.aggregators.average(aggregate)?)),
            CompiledEventQuery::Variance(aggregate) => Some(format!("Var({})", self.aggregators.variance(aggregate)?)),
            CompiledEventQuery::Covariance(aggregate) => Some(format!("Cov({})", self.aggregators.covariance(aggregate)?)),
            CompiledEventQuery::Arithmetic { left, right, operation } => {
                let left = self.query_to_string(values, left)?;
                let right = self.query_to_string(values, right)?;
                Some(format!("{} {} {}", left, operation, right))
            }
            CompiledEventQuery::Bool { left, right, operation } => {
                let left = self.query_to_string(values, left)?;
                let right = self.query_to_string(values, right)?;
                Some(format!("{} {} {}", left, operation, right))
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
}

#[derive(Debug)]
struct CompiledEvent {
    independent_metric: MetricId,
    dependent_metric: MetricId,
    query: CompiledEventQuery
}

#[derive(Debug)]
enum CompiledEventQuery {
    Value(ValueId),
    Average(AverageAggregate),
    Variance(VarianceAggregate),
    Covariance(CovarianceAggregate),
    Arithmetic { left: Box<CompiledEventQuery>, right: Box<CompiledEventQuery>, operation: ArithmeticOperator },
    Bool { left: Box<CompiledEventQuery>, right: Box<CompiledEventQuery>, operation: BoolOperator },
    And { left: Box<CompiledEventQuery>, right: Box<CompiledEventQuery> },
    Or { left: Box<CompiledEventQuery>, right: Box<CompiledEventQuery> }
}

struct CompileEventQueryContext {
    independent_metric: MetricId,
    dependent_metric: MetricId
}

impl CompileEventQueryContext {
    pub fn new(event: &Event) -> CompileEventQueryContext {
        CompileEventQueryContext {
            independent_metric: event.independent_metric,
            dependent_metric: event.dependent_metric
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CompiledValueExpression {
    Metric(MetricId),
    Constant(FloatOrd<f64>),
    Arithmetic { left: Box<CompiledValueExpression>, right: Box<CompiledValueExpression>, operation: ArithmeticOperator },
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
            ValueExpression::Arithmetic { left, right, operation } => {
                let left = CompiledValueExpression::compile(left, context)?;
                let right = CompiledValueExpression::compile(right, context)?;

                Ok(
                    CompiledValueExpression::Arithmetic {
                        left: Box::new(left),
                        right: Box::new(right),
                        operation: operation.clone()
                    }
                )
            }
        }
    }

    pub fn evaluate(&self, metrics: &FnvHashMap<MetricId, f64>) -> Option<f64> {
        match self {
            CompiledValueExpression::Metric(metric) => metrics.get(metric).cloned(),
            CompiledValueExpression::Constant(value) => Some(value.0),
            CompiledValueExpression::Arithmetic { left, right, operation } => {
                let left = left.evaluate(metrics)?;
                let right = right.evaluate(metrics)?;
                Some(operation.evaluate(left, right))
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
    pub fn new(context: &CompileEventQueryContext) -> CompiledValueExpressionContext {
        CompiledValueExpressionContext {
            independent_metric: context.independent_metric,
            dependent_metric: context.dependent_metric,
            used_metrics: FnvHashSet::default()
        }
    }
}

#[test]
fn test_event_engine1() {
    let mut engine = EventEngine::new();
    let x = engine.define_metric("x");
    let y = engine.define_metric("y");

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
                                    time_interval: TimeInterval::Seconds(10.0)
                                }
                            )
                        ),
                        right: Box::new(
                            EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(0.0)))
                        ),
                        operation: BoolOperator::GreaterThan
                    }
                ),
                right: Box::new(
                    EventQuery::Bool {
                        left: Box::new(
                            EventQuery::Expression(
                                EventExpression::Average {
                                    value: ValueExpression::IndependentMetric,
                                    time_interval: TimeInterval::Seconds(10.0)
                                }
                            )
                        ),
                        right: Box::new(
                            EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(0.0)))
                        ),
                        operation: BoolOperator::GreaterThan
                    }
                )
            }
        }
    ).unwrap();

    println!();

    let t0 = Instant::now();
    let mut values = FnvHashMap::default();
    values.insert(x, 1.0);
    values.insert(y, 10.0);
    engine.handle_values(t0, &values);

    let t1 = t0.add(Duration::from_secs_f64(2.0));
    let mut values = FnvHashMap::default();
    values.insert(x, 2.0);
    values.insert(y, 20.0);
    engine.handle_values(t1, &values);

    let t1 = t0.add(Duration::from_secs_f64(4.0));
    let mut values = FnvHashMap::default();
    values.insert(x, 4.0);
    values.insert(y, 40.0);
    engine.handle_values(t1, &values);
}