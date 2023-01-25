use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;
use log::{debug, error};

use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task;
use tokio::task::JoinHandle;

use crate::collectors::custom::{CustomMetric, CustomMetricsCollector};
use crate::collectors::docker::DockerStatsCollector;
use crate::collectors::postgres::PostgresStatsCollector;
use crate::collectors::rabbitmq::RabbitMQStatsCollector;
use crate::collectors::system::SystemMetricsCollector;
use crate::config::Config;
use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{EventResult, MetricName, TimeInterval, TimePoint};

pub struct CollectorsManager {
    last_discover: Instant,
    rediscover_rate: f64,
    system_metrics_collector: SystemMetricsCollector,
    rabbitmq_metrics_collector: Option<Rc<RefCell<RabbitMQStatsCollector>>>,
    docker_metrics_collector: Option<Rc<RefCell<DockerStatsCollector>>>,
    postgres_metrics_collector: Option<Rc<RefCell<PostgresStatsCollector>>>,
    custom_metrics_receiver: UnboundedReceiver<CustomMetric>
}

macro_rules! collect_async {
    ($collector:expr, $metric_time:expr) => {
        {
           let moved_collector = $collector;
           task::spawn_local(async move {
                let mut values = MetricValues::new(TimeInterval::Seconds(0.0));
                let result = moved_collector.borrow_mut().collect(
                    $metric_time,
                    &mut values
                ).await;
                result.map(|_| values)
            })
        }
    };
}

impl CollectorsManager {
    pub async fn new(config: &Config,
                     metric_definitions: &mut MetricDefinitions) -> EventResult<CollectorsManager> {
        let system_metrics_collector = SystemMetricsCollector::new(metric_definitions)?;

        let rabbitmq_metrics_collector = match &config.rabbitmq {
            Some(rabbitmq) => {
                let rabbitmq_metrics_collector = RabbitMQStatsCollector::new(
                    rabbitmq,
                    metric_definitions
                ).await?;
                Some(Rc::new(RefCell::new(rabbitmq_metrics_collector)))
            }
            None => None
        };

        let docker_metrics_collector = match &config.docker {
            Some(docker) => {
                let docker_metrics_collector = DockerStatsCollector::new(
                    docker,
                    metric_definitions
                ).await?;
                Some(Rc::new(RefCell::new(docker_metrics_collector)))
            }
            None => None
        };

        let postgres_metrics_collector = match &config.postgres {
            Some(postgres) => {
                let postgres_metrics_collector = PostgresStatsCollector::new(
                    postgres,
                    metric_definitions
                ).await?;
                Some(Rc::new(RefCell::new(postgres_metrics_collector)))
            }
            None => None
        };

        let (custom_metrics_sender, custom_metrics_receiver) = mpsc::unbounded_channel();
        let custom_metrics_collector = CustomMetricsCollector::new(&config.custom_metrics, custom_metrics_sender).unwrap();
        task::spawn_local(async move {
            custom_metrics_collector.collect().await.unwrap();
        });

        Ok(
            CollectorsManager {
                last_discover: Instant::now(),
                rediscover_rate: config.rediscover_rate,
                system_metrics_collector,
                rabbitmq_metrics_collector,
                docker_metrics_collector,
                postgres_metrics_collector,
                custom_metrics_receiver
            }
        )
    }

    pub async fn collect(&mut self,
                         metric_definitions: &mut MetricDefinitions,
                         metric_time: TimePoint,
                         values: &mut MetricValues) -> EventResult<()> {
        let rabbitmq_result = if let Some(rabbitmq_metrics_collector) = self.rabbitmq_metrics_collector.as_ref() {
            Some(collect_async!(rabbitmq_metrics_collector.clone(), metric_time))
        } else {
            None
        };

        let docker_result = if let Some(docker_metrics_collector) = self.docker_metrics_collector.as_ref() {
            Some(collect_async!(docker_metrics_collector.clone(), metric_time))
        } else {
            None
        };

        let postgres_result = if let Some(postgres_metrics_collector) = self.postgres_metrics_collector.as_ref() {
            Some(collect_async!(postgres_metrics_collector.clone(), metric_time))
        } else {
            None
        };

        self.system_metrics_collector.collect(metric_time, values)?;

        if let Some(rabbitmq_result) = rabbitmq_result {
            handle_task_result("RabbitMQ", rabbitmq_result, values).await;
        }

        if let Some(docker_result) = docker_result {
            handle_task_result("Docker", docker_result, values).await;
        }

        if let Some(postgres_result) = postgres_result {
            handle_task_result("Postgres", postgres_result, values).await;
        }

        while let Ok(custom_metric) = self.custom_metrics_receiver.try_recv() {
            match custom_metric {
                CustomMetric::Gauge { name, value, sub } => {
                    let metric_id = metric_definitions.define(MetricName::new(&name, sub.as_ref().map(|x| x.as_str())));
                    values.insert(metric_time, metric_id, value);
                }
            }
        }

        Ok(())
    }

    pub async fn try_discover(&mut self, metric_definitions: &mut MetricDefinitions) -> EventResult<bool> {
        let time_now = Instant::now();
        if (time_now - self.last_discover).as_secs_f64() >= 1.0 / self.rediscover_rate {
            let changed = self.discover(metric_definitions).await?;
            debug!("Discover time: {:.3} ms", (Instant::now() - time_now).as_secs_f64());
            self.last_discover = time_now;
            Ok(changed)
        } else {
            Ok(false)
        }
    }

    async fn discover(&mut self, metric_definitions: &mut MetricDefinitions) -> EventResult<bool> {
        let mut changed = false;

        if let Some(docker_metrics_collector) = self.docker_metrics_collector.as_ref() {
            changed |= docker_metrics_collector.borrow_mut().discover(metric_definitions).await?;
        }

        if let Some(rabbitmq_metrics_collector) = self.rabbitmq_metrics_collector.as_ref() {
            changed |= rabbitmq_metrics_collector.borrow_mut().discover(metric_definitions).await?;
        }

        Ok(changed)
    }
}

async fn handle_task_result(name: &str,
                            task_result: JoinHandle<EventResult<MetricValues>>,
                            values: &mut MetricValues) {
    match task_result.await.unwrap() {
        Ok(task_values) => {
            values.extend(task_values);
        }
        Err(err) => {
            error!("Failed to collect {} metrics: {:?}", name, err);
        }
    }
}