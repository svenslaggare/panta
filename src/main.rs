mod model;
mod event;
mod metrics;
mod aggregator;
mod engine;
mod collectors;
mod event_output;

use std::time::{Duration, Instant};

use log::{error, trace};

use tokio::task;

use crate::collectors::manager::CollectorsManager;

use crate::engine::EventEngine;
use crate::event::{BoolOperator, Event, EventExpression, EventOutputName, EventQuery, ValueExpression};
use crate::event_output::{ConsoleEventOutputHandler, EventOutputHandlers};
use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{MetricName, TimeInterval, TimePoint, Value};

#[tokio::main]
async fn main() {
    setup_logger().unwrap();

    let local = task::LocalSet::new();

    local.run_until(async move {
        let sampling_rate = 5.0;

        let mut metric_definitions = MetricDefinitions::new();
        let mut engine = EventEngine::new();
        let mut collectors_manager = CollectorsManager::new(&mut metric_definitions).await.unwrap();

        metric_definitions.print();

        add_events(&metric_definitions, &mut engine);

        let mut event_output_handlers = EventOutputHandlers::new();
        event_output_handlers.add_handler(Box::new(ConsoleEventOutputHandler::new()));

        let mut values = MetricValues::new(TimeInterval::Minutes(0.5));

        loop {
            let metric_time = TimePoint::now();

            collectors_manager.collect(
                &mut metric_definitions,
                metric_time,
                &mut values
            ).await.unwrap();

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