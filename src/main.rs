mod model;
mod event;
mod metrics;
mod aggregator;
mod engine;
mod collectors;
mod event_output;
mod parsing;
mod config;

use std::path::Path;
use std::time::{Duration, Instant};

use log::{error, trace};

use tokio::task;

use crate::collectors::manager::CollectorsManager;
use crate::config::Config;

use crate::engine::EventEngine;
use crate::event::{BoolOperator, Event, EventExpression, EventOutputName, EventQuery, ValueExpression};
use crate::event_output::{EventOutputHandlers};
use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{EventsDefinition, MetricName, TimeInterval, TimePoint, Value};

#[tokio::main]
async fn main() {
    setup_logger().unwrap();

    let config = Config::default();
    let events_def = EventsDefinition::load_from_file(Path::new("data/events.yaml")).unwrap();
    let sampling_rate = events_def.sampling_rate;

    let local = task::LocalSet::new();
    local.run_until(async move {
        let mut metric_definitions = MetricDefinitions::new();
        let mut engine = EventEngine::new();
        let mut collectors_manager = CollectorsManager::new(
            &config,
            &mut metric_definitions
        ).await.unwrap();

        metric_definitions.print();

        for event in events_def.events {
            engine.add_event(&metric_definitions, event).unwrap();
        }

        let mut event_output_handlers = EventOutputHandlers::new();
        for event_output_def in events_def.outputs {
            event_output_handlers.add_handler(event_output_def.create().unwrap());
        }

        let mut values = MetricValues::new(TimeInterval::Minutes(0.5));

        loop {
            let metric_time = TimePoint::now();

            collectors_manager.collect(
                &mut metric_definitions,
                metric_time,
                &mut values
            ).await.unwrap();

            let computation_start = Instant::now();
            engine.handle_values(
                &metric_definitions,
                metric_time,
                &values,
                |event_id, _, name, outputs: Vec<(String, Value)>| {
                    if let Err(err) = event_output_handlers.handle_output(&event_id, name, &outputs) {
                        error!("Failed generating output due to: {:?}", err);
                    }
                }
            );

            values.clear_old(metric_time);

            let elapsed = (Instant::now() - metric_time).as_secs_f64();
            let elapsed_computation = (Instant::now() - computation_start).as_secs_f64();
            trace!("Elapsed time: {:.3} ms (computation: {:.3} ms), metrics: {}", elapsed * 1000.0, elapsed_computation * 1000.0, values.len());
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
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .apply()?;
    Ok(())
}

fn add_events(metric_definitions: &MetricDefinitions, engine: &mut EventEngine) {
    let interval = TimeInterval::Seconds(5.0);

    engine.add_event(
        metric_definitions,
        Event {
            name: "cpu_usage".to_owned(),
            independent_metric: MetricName::sub("system.cpu_usage", "all"),
            dependent_metric: vec![
                MetricName::all("system.used_memory_bytes"),
                MetricName::sub("system.disk.read_bytes.rate", "sda2"),
                MetricName::sub("system.disk.write_bytes.rate", "sda2"),
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
                    EventOutputName::Text("corr".to_owned()),
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