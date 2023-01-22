use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;
use log::{debug, error};

use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task;

use crate::collectors::custom::{CustomMetric, CustomMetricsCollector};
use crate::collectors::docker::DockerStatsCollector;
use crate::collectors::rabbitmq::RabbitMQStatsCollector;
use crate::collectors::system::SystemMetricsCollector;
use crate::config::Config;
use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{EventResult, MetricName, TimeInterval, TimePoint};

pub struct CollectorsManager {
    last_discover: Instant,
    rediscover_every_nth: f64,
    system_metrics_collector: SystemMetricsCollector,
    rabbitmq_metrics_collector: Rc<RefCell<RabbitMQStatsCollector>>,
    docker_metrics_collector: Rc<RefCell<DockerStatsCollector>>,
    custom_metrics_receiver: UnboundedReceiver<CustomMetric>
}

impl CollectorsManager {
    pub async fn new(config: &Config,
                     metric_definitions: &mut MetricDefinitions) -> EventResult<CollectorsManager> {
        let system_metrics_collector = SystemMetricsCollector::new(metric_definitions)?;

        let rabbitmq_metrics_collector = RabbitMQStatsCollector::new(
            &config.rabbitmq_metrics,
            metric_definitions
        ).await?;
        let rabbitmq_metrics_collector = Rc::new(RefCell::new(rabbitmq_metrics_collector));

        let docker_metrics_collector = DockerStatsCollector::new(
            metric_definitions
        ).await?;
        let docker_metrics_collector = Rc::new(RefCell::new(docker_metrics_collector));

        let (custom_metrics_sender, custom_metrics_receiver) = mpsc::unbounded_channel();
        let custom_metrics_collector = CustomMetricsCollector::new(&config.custom_metrics, custom_metrics_sender).unwrap();
        task::spawn_local(async move {
            custom_metrics_collector.collect().await.unwrap();
        });

        Ok(
            CollectorsManager {
                last_discover: Instant::now(),
                rediscover_every_nth: config.rediscover_every_nth,
                system_metrics_collector,
                rabbitmq_metrics_collector,
                docker_metrics_collector,
                custom_metrics_receiver
            }
        )
    }

    pub async fn collect(&mut self,
                         metric_definitions: &mut MetricDefinitions,
                         metric_time: TimePoint,
                         values: &mut MetricValues) -> EventResult<()> {
        let rabbitmq_metrics_collector_clone = self.rabbitmq_metrics_collector.clone();
        let rabbitmq_result = task::spawn_local(async move {
            let mut rabbitmq_values = MetricValues::new(TimeInterval::Seconds(0.0));
            let rabbitmq_result = rabbitmq_metrics_collector_clone.borrow_mut().collect(
                metric_time,
                &mut rabbitmq_values
            ).await;
            rabbitmq_result.map(|_| rabbitmq_values)
        });

        let docker_metrics_collector_clone = self.docker_metrics_collector.clone();
        let docker_result = task::spawn_local(async move {
            let mut docker_values = MetricValues::new(TimeInterval::Seconds(0.0));
            let docker_result = docker_metrics_collector_clone.borrow_mut().collect(
                metric_time,
                &mut docker_values
            ).await;
            docker_result.map(|_| docker_values)
        });

        self.system_metrics_collector.collect(metric_time, values)?;

        match rabbitmq_result.await.unwrap() {
            Ok(rabbitmq_values) => {
                values.extend(rabbitmq_values);
            }
            Err(err) => {
                error!("Failed to collect RabbitMQ metrics: {:?}", err);
            }
        }

        match docker_result.await.unwrap() {
            Ok(docker_values) => {
                values.extend(docker_values);
            }
            Err(err) => {
                error!("Failed to collect Docker metrics: {:?}", err);
            }
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
        if (time_now - self.last_discover).as_secs_f64() >= self.rediscover_every_nth {
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
        changed |= self.docker_metrics_collector.borrow_mut().discover(metric_definitions).await?;
        changed |= self.rabbitmq_metrics_collector.borrow_mut().discover(metric_definitions).await?;
        Ok(changed)
    }
}