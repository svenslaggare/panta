mod model;
mod event;
mod metrics;
mod aggregator;
mod engine;
mod system_metrics_collectors;
mod rabbitmq_metrics_collector;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use log::{info, trace};

use tokio::task;

use crate::engine::EventEngine;
use crate::event::{BoolOperator, Event, EventExpression, EventOutputName, EventQuery, ValueExpression};
use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{TimeInterval, TimePoint, Value};
use crate::rabbitmq_metrics_collector::RabbitMQStatsCollector;
use crate::system_metrics_collectors::SystemMetricsCollector;

#[tokio::main]
async fn main() {
    let local = task::LocalSet::new();
    setup_logger().unwrap();

    local.run_until(async move {
        let sampling_rate = 5.0;

        let mut metric_definitions = MetricDefinitions::new();
        let mut engine = EventEngine::new();

        let mut system_metrics_collector = SystemMetricsCollector::new(&mut metric_definitions).unwrap();
        let mut rabbitmq_metrics_collector = RabbitMQStatsCollector::new(
            "http://localhost:15672", "guest", "guest",
            &mut metric_definitions
        ).await.unwrap();
        let rabbitmq_metrics_collector = Rc::new(RefCell::new(rabbitmq_metrics_collector));

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

            info!("Event generated for #{}, {}", event_index, output_string);
        };

        let mut values = MetricValues::new(TimeInterval::Minutes(0.5));

        loop {
            let now = TimePoint::now();
            system_metrics_collector.collect(now, &mut values).unwrap();

            let rabbitmq_metrics_collector_clone = rabbitmq_metrics_collector.clone();
            let rabbitmq_task = task::spawn_local(async move {
                let mut rabbitmq_values = MetricValues::new(TimeInterval::Seconds(0.0));
                rabbitmq_metrics_collector_clone.borrow_mut().collect(now, &mut rabbitmq_values).await.unwrap();
                rabbitmq_values
            });

            engine.handle_values(&metric_definitions, now, &values, on_event);
            values.extend(rabbitmq_task.await.unwrap());
            values.clear_old(now);

            let elapsed = (Instant::now() - now).as_secs_f64();
            trace!("Elapsed time: {:.3} ms, metrics: {}", elapsed * 1000.0, values.len());
            tokio::time::sleep(Duration::from_secs_f64((1.0 / sampling_rate - elapsed).max(0.0))).await;
        }
    }).await;
}

fn setup_logger() -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        .chain(std::io::stdout())
        .apply()?;
    Ok(())
}

fn add_events(metric_definitions: &MetricDefinitions, engine: &mut EventEngine) {
    let interval = TimeInterval::Seconds(5.0);

    engine.add_event(
        Event {
            independent_metric: metric_definitions.get_id("system.cpu_usage:all").unwrap(),
            dependent_metric: vec![
                metric_definitions.get_id("system.used_memory").unwrap(),
                metric_definitions.get_id("system.disk_read_bytes:sda2").unwrap(),
                metric_definitions.get_id("system.disk_write_bytes:sda2").unwrap(),
                metric_definitions.get_id("rabbitmq.publish_rate:panta_test_queue").unwrap()
            ],
            query: EventQuery::And {
                left: Box::new(
                    EventQuery::Bool {
                        left: Box::new(
                            EventQuery::Expression(
                                EventExpression::Correlation {
                                    left: ValueExpression::IndependentMetric,
                                    right: ValueExpression::DependentMetric,
                                    interval
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
                    EventQuery::And {
                        left: Box::new(
                            EventQuery::Bool {
                                left: Box::new(
                                    EventQuery::Expression(
                                        EventExpression::Average {
                                            value: ValueExpression::IndependentMetric,
                                            interval
                                        }
                                    )
                                ),
                                right: Box::new(
                                    EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(0.1)))
                                ),
                                operator: BoolOperator::GreaterThan
                            }
                        ),
                        right: Box::new(
                            EventQuery::Bool {
                                left: Box::new(
                                    EventQuery::Expression(
                                        EventExpression::Average {
                                            value: ValueExpression::DependentMetric,
                                            interval
                                        }
                                    )
                                ),
                                right: Box::new(
                                    EventQuery::Expression(EventExpression::Value(ValueExpression::Constant(0.0)))
                                ),
                                operator: BoolOperator::GreaterThan
                            }
                        ),
                    }
                )
            },
            outputs: vec![
                (
                    EventOutputName::IndependentMetricName,
                    EventExpression::Average { value: ValueExpression::IndependentMetric, interval }
                ),
                (
                    EventOutputName::DependentMetricName,
                    EventExpression::Average { value: ValueExpression::DependentMetric, interval }
                ),
                (
                    EventOutputName::String("corr".to_owned()),
                    EventExpression::Correlation {
                        left: ValueExpression::IndependentMetric,
                        right: ValueExpression::DependentMetric,
                        interval
                    }
                )
            ]
        }
    ).unwrap();
}