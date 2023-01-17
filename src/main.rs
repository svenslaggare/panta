mod model;
mod event;
mod metrics;
mod aggregator;
mod engine;
mod system_metrics_collectors;
mod rabbitmq_metrics_collector;
mod custom_metrics_collector;
mod event_output;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use log::{error, trace};

use tokio::sync::mpsc;
use tokio::task;

use crate::custom_metrics_collector::{CustomMetric, CustomMetricsCollector};

use crate::engine::EventEngine;
use crate::event::{BoolOperator, Event, EventExpression, EventOutputName, EventQuery, ValueExpression};
use crate::event_output::{ConsoleEventOutputHandler, EventOutputHandlers};
use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{MetricName, TimeInterval, TimePoint, Value};
use crate::rabbitmq_metrics_collector::RabbitMQStatsCollector;
use crate::system_metrics_collectors::SystemMetricsCollector;

#[tokio::main]
async fn main() {
    setup_logger().unwrap();

    let local = task::LocalSet::new();

    local.run_until(async move {
        let sampling_rate = 5.0;

        let mut metric_definitions = MetricDefinitions::new();
        let mut engine = EventEngine::new();

        let mut system_metrics_collector = SystemMetricsCollector::new(&mut metric_definitions).unwrap();
        let rabbitmq_metrics_collector = RabbitMQStatsCollector::new(
            "http://localhost:15672", "guest", "guest",
            &mut metric_definitions
        ).await.unwrap();
        let rabbitmq_metrics_collector = Rc::new(RefCell::new(rabbitmq_metrics_collector));

        add_events(&metric_definitions, &mut engine);

        let (custom_metrics_sender, mut custom_metrics_receiver) = mpsc::unbounded_channel();
        let custom_metrics_collector = CustomMetricsCollector::new(custom_metrics_sender).unwrap();
        task::spawn_local(async move {
            custom_metrics_collector.collect().await.unwrap();
        });

        let mut event_output_handlers = EventOutputHandlers::new();
        event_output_handlers.add_handler(Box::new(ConsoleEventOutputHandler::new()));

        let mut values = MetricValues::new(TimeInterval::Minutes(0.5));

        loop {
            let metric_time = TimePoint::now();

            let rabbitmq_metrics_collector_clone = rabbitmq_metrics_collector.clone();
            let rabbitmq_result = task::spawn_local(async move {
                let mut rabbitmq_values = MetricValues::new(TimeInterval::Seconds(0.0));
                let rabbitmq_result = rabbitmq_metrics_collector_clone.borrow_mut().collect(
                    metric_time,
                    &mut rabbitmq_values
                ).await;
                rabbitmq_result.map(|_| rabbitmq_values)
            });

            system_metrics_collector.collect(metric_time, &mut values).unwrap();

            match rabbitmq_result.await.unwrap() {
                Ok(rabbitmq_values) => {
                    values.extend(rabbitmq_values);
                }
                Err(err) => {
                    error!("Failed to collect RabbitMQ metrics: {:?}", err);
                }
            }

            while let Ok(custom_metric) = custom_metrics_receiver.try_recv() {
                match custom_metric {
                    CustomMetric::Gauge { name, value, sub } => {
                        let metric_id = metric_definitions.define(MetricName::new(&name, sub.as_ref().map(|x| x.as_str())));
                        values.insert(metric_time, metric_id, value);
                    }
                }
            }

            engine.handle_values(
                &metric_definitions,
                metric_time,
                &values,
                |event_id, values: Vec<(String, Value)>| {
                    if let Err(err) = event_output_handlers.handle_output(&event_id, &values) {
                        error!("Failed generating output due to: {:?}", err);
                    }
                }
            );

            values.clear_old(metric_time);

            let elapsed = (Instant::now() - metric_time).as_secs_f64();
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
        metric_definitions,
        Event {
            independent_metric: MetricName::sub("system.cpu_usage", "all"),
            dependent_metric: vec![
                MetricName::all("system.used_memory"),
                MetricName::all("system.disk_read_bytes"),
                MetricName::all("system.disk_write_bytes"),
                MetricName::all("rabbitmq.publish_rate")
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