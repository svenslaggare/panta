use std::cell::RefCell;
use std::rc::Rc;
use log::error;

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
            let rabbitmq_result = docker_metrics_collector_clone.borrow_mut().collect(
                metric_time,
                &mut docker_values
            ).await;
            rabbitmq_result.map(|_| docker_values)
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
}