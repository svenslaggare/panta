mod model;
mod event;
mod metrics;
mod aggregator;
mod engine;
mod collectors;
mod event_output;
mod parsing;
mod config;

use std::path::{ PathBuf};
use std::time::{Duration, Instant};

use log::{error, trace};

use structopt::StructOpt;

use tokio::task;

use crate::collectors::manager::CollectorsManager;
use crate::config::Config;
use crate::engine::EventEngine;
use crate::event_output::{EventOutputHandlers};
use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{EventsDefinition, TimeInterval, TimePoint, Value};

#[tokio::main]
async fn main() {
    let command_line_config: CommandLineConfig = CommandLineConfig::from_args();

    let config = match command_line_config.config_file {
        None => Config::default(),
        Some(path) => Config::load_from_file(&path).unwrap()
    };

    let events_def = EventsDefinition::load_from_file(&command_line_config.events_file).unwrap();

    setup_logger(&config).unwrap();

    let local = task::LocalSet::new();
    local.run_until(async move {
        let sampling_rate = events_def.sampling_rate;

        let mut metric_definitions = MetricDefinitions::new();
        let mut engine = EventEngine::new();
        let mut collectors_manager = CollectorsManager::new(
            &config,
            &mut metric_definitions
        ).await.unwrap();

        if command_line_config.show_metrics {
            metric_definitions.print();
        }

        for event in events_def.events {
            engine.add_event(&metric_definitions, event).unwrap();
        }

        let mut event_output_handlers = EventOutputHandlers::new();
        for event_output_def in events_def.outputs {
            event_output_handlers.add_handler(event_output_def.create().unwrap());
        }

        let mut values = MetricValues::new(TimeInterval::Minutes(0.5));

        loop {
            if collectors_manager.try_discover(&mut metric_definitions).await.unwrap() {
                engine.recompile_events(&metric_definitions).unwrap();
            }

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

fn setup_logger(config: &Config) -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S.%f]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .level_for("panta", config.log_level)
        .chain(std::io::stdout())
        .apply()?;
    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt(name="panta", about="Panta - performance analysis tool")]
struct CommandLineConfig {
    /// The event definitions file
    events_file: PathBuf,
    /// The config file
    #[structopt(long="config")]
    config_file: Option<PathBuf>,
    /// Shows the defined metrics
    #[structopt(long)]
    show_metrics: bool
}