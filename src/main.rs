mod model;
mod event;
mod metrics;
mod aggregator;
mod engine;
mod system_metric_collectors;

use std::time::{Duration, Instant};
use crate::engine::EventEngine;
use crate::event::{BoolOperator, Event, EventExpression, EventOutputName, EventQuery, ValueExpression};
use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{TimeInterval, TimePoint, Value};
use crate::system_metric_collectors::SystemMetricsCollector;

fn main() {
    let mut metric_definitions = MetricDefinitions::new();
    let mut engine = EventEngine::new();
    let mut system_metrics_collector = SystemMetricsCollector::new(&mut metric_definitions).unwrap();
    let sampling_rate = 5.0;

    add_events(&metric_definitions, &mut engine);

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

    let mut values = MetricValues::new(TimeInterval::Minutes(0.5));

    loop {
        let now = TimePoint::now();
        system_metrics_collector.collect(now, &mut values).unwrap();
        engine.handle_values(&metric_definitions, now, &values, on_event);
        values.clear_old(now);
        let elapsed = (Instant::now() - now).as_secs_f64();
        std::thread::sleep(Duration::from_secs_f64((1.0 / sampling_rate - elapsed).max(0.0)));
    }
}

fn add_events(metric_definitions: &MetricDefinitions, engine: &mut EventEngine) {
    engine.add_event(
        Event {
            independent_metric: metric_definitions.get_id("cpu_usage:all").unwrap(),
            dependent_metric: metric_definitions.get_id("used_memory").unwrap(),
            query: EventQuery::And {
                left: Box::new(
                    EventQuery::Bool {
                        left: Box::new(
                            EventQuery::Expression(
                                EventExpression::Correlation {
                                    left: ValueExpression::IndependentMetric,
                                    right: ValueExpression::DependentMetric,
                                    interval: TimeInterval::Seconds(10.0)
                                }
                            )
                        ),
                        right: Box::new(
                            EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(0.5)))
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
                            EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(0.1)))
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
                    EventOutputName::String("corr".to_owned()),
                    EventExpression::Correlation {
                        left: ValueExpression::IndependentMetric,
                        right: ValueExpression::DependentMetric,
                        interval: TimeInterval::Seconds(10.0)
                    }
                )
            ]
        }
    ).unwrap();
}