use fnv::{FnvHashMap, FnvHashSet};
use log::debug;
use serde::Deserialize;

use crate::config::RabbitMQMetricsConfig;
use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{EventError, EventResult, MetricId, MetricName, TimePoint};

pub struct RabbitMQStatsCollector {
    base_url: String,
    username: String,
    password: String,
    virtual_host: String,
    queues: FnvHashMap<String, QueueEntry>,
    client: reqwest::Client
}

impl RabbitMQStatsCollector {
    pub async fn new(config: &RabbitMQMetricsConfig,
                     metric_definitions: &mut MetricDefinitions) -> EventResult<RabbitMQStatsCollector> {
        let mut collector = RabbitMQStatsCollector {
            base_url: config.base_url.to_owned(),
            username: config.username.to_owned(),
            password: config.password.to_owned(),
            virtual_host: "%2F".to_owned(),
            client: reqwest::Client::new(),
            queues: FnvHashMap::default()
        };

        collector.discover(metric_definitions).await?;

        Ok(collector)
    }

    pub async fn discover(&mut self, metric_definitions: &mut MetricDefinitions) -> EventResult<bool> {
        let mut added = false;
        let mut found = FnvHashSet::default();

        for queue in self.get_queues().await.map_err(|err| EventError::FailedToCollectRabbitMQMetric(err))? {
            found.insert(queue.name.clone());

            if !self.queues.contains_key(&queue.name) {
                added = true;
                self.queues.insert(
                    queue.name.clone(),
                    QueueEntry {
                        name: queue.name.clone(),

                        message_ready_count_metric: metric_definitions.define(MetricName::sub("rabbitmq.message_ready.count", &queue.name)),
                        message_ready_rate_metric: metric_definitions.define(MetricName::sub("rabbitmq.message_ready.rate", &queue.name)),

                        publish_count_metric: metric_definitions.define(MetricName::sub("rabbitmq.publish.count", &queue.name)),
                        publish_rate_metric: metric_definitions.define(MetricName::sub("rabbitmq.publish.rate", &queue.name)),

                        ack_count_metric: metric_definitions.define(MetricName::sub("rabbitmq.ack.count", &queue.name)),
                        ack_rate_metric: metric_definitions.define(MetricName::sub("rabbitmq.ack.rate", &queue.name)),

                        deliver_count_metric: metric_definitions.define(MetricName::sub("rabbitmq.deliver.count", &queue.name)),
                        deliver_rate_metric: metric_definitions.define(MetricName::sub("rabbitmq.deliver.rate", &queue.name)),

                        redeliver_count_metric: metric_definitions.define(MetricName::sub("rabbitmq.redeliver.count", &queue.name)),
                        redeliver_rate_metric: metric_definitions.define(MetricName::sub("rabbitmq.redeliver.rate", &queue.name)),

                        unacknowledged_count_metric: metric_definitions.define(MetricName::sub("rabbitmq.unacknowledged.count", &queue.name)),
                        unacknowledged_rate_metric: metric_definitions.define(MetricName::sub("rabbitmq.unacknowledged.rate", &queue.name)),

                        consumer_utilisation_metric: metric_definitions.define(MetricName::sub("rabbitmq.consumer_utilisation", &queue.name))
                    }
                );
            }
        }

        let all = FnvHashSet::from_iter(self.queues.keys().cloned());
        for old in all.difference(&found) {
            self.queues.remove(old);
        }

        debug!("Queues:");
        for queue in &self.queues {
            debug!("\t{}", queue.1.name);
        }

        Ok(added)
    }

    pub async fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()> {
        let queue_infos = self.collect_all_queue_info().await.map_err(|err| EventError::FailedToCollectRabbitMQMetric(err))?;

        for queue_info in queue_infos {
            if let Some(queue_entry) = self.queues.get(&queue_info.name) {
                metrics.insert(time, queue_entry.message_ready_count_metric, queue_info.messages_ready.unwrap_or(0) as f64);
                metrics.insert(time, queue_entry.message_ready_rate_metric, queue_info.messages_ready_details.map(|d| d.rate).unwrap_or(0.0));

                let message_stats = queue_info.message_stats.unwrap_or_else(|| RabbitMQMessageStats::default());
                metrics.insert(time, queue_entry.publish_count_metric, message_stats.publish.unwrap_or(0) as f64);
                metrics.insert(time, queue_entry.publish_rate_metric, message_stats.publish_details.map(|d| d.rate).unwrap_or(0.0));

                metrics.insert(time, queue_entry.ack_count_metric, message_stats.ack.unwrap_or(0) as f64);
                metrics.insert(time, queue_entry.ack_rate_metric, message_stats.ack_details.map(|d| d.rate).unwrap_or(0.0));

                metrics.insert(time, queue_entry.deliver_count_metric, message_stats.deliver.unwrap_or(0) as f64);
                metrics.insert(time, queue_entry.deliver_rate_metric, message_stats.deliver_details.map(|d| d.rate).unwrap_or(0.0));

                metrics.insert(time, queue_entry.redeliver_count_metric, message_stats.redeliver.unwrap_or(0) as f64);
                metrics.insert(time, queue_entry.redeliver_rate_metric, message_stats.redeliver_details.map(|d| d.rate).unwrap_or(0.0));

                metrics.insert(time, queue_entry.unacknowledged_count_metric, queue_info.messages_unacknowledged.unwrap_or(0) as f64);
                metrics.insert(time, queue_entry.unacknowledged_rate_metric, queue_info.messages_unacknowledged_details.map(|d| d.rate).unwrap_or(0.0));

                metrics.insert(time, queue_entry.consumer_utilisation_metric, queue_info.consumer_utilisation.unwrap_or(0.0));
            }
        }

        Ok(())
    }

    async fn get_queues(&self) -> Result<Vec<RabbitMQQueue>, reqwest::Error> {
        self.client.get(format!("{}/api/queues/{}", self.base_url, self.virtual_host))
            .basic_auth(&self.username, Some(&self.password))
            .send().await?
            .error_for_status()?
            .json().await
    }

    async fn collect_all_queue_info(&self) -> Result<Vec<RabbitMQQueueInfo>, reqwest::Error> {
        self.client.get(format!("{}/api/queues/{}", self.base_url, self.virtual_host))
            .basic_auth(&self.username, Some(&self.password))
            .send().await?
            .error_for_status()?
            .json().await
    }
}

struct QueueEntry {
    name: String,
    message_ready_count_metric: MetricId,
    message_ready_rate_metric: MetricId,

    publish_count_metric: MetricId,
    publish_rate_metric: MetricId,

    ack_count_metric: MetricId,
    ack_rate_metric: MetricId,

    deliver_count_metric: MetricId,
    deliver_rate_metric: MetricId,

    redeliver_count_metric: MetricId,
    redeliver_rate_metric: MetricId,

    unacknowledged_count_metric: MetricId,
    unacknowledged_rate_metric: MetricId,

    consumer_utilisation_metric: MetricId
}

#[derive(Debug, Deserialize)]
struct RabbitMQQueue {
    name: String
}

#[derive(Debug, Deserialize)]
struct RabbitMQQueueInfo {
    name: String,
    messages: Option<u64>,
    messages_details: Option<RabbitMQDetails>,
    messages_ready: Option<u64>,
    messages_ready_details: Option<RabbitMQDetails>,
    messages_unacknowledged: Option<u64>,
    messages_unacknowledged_details: Option<RabbitMQDetails>,
    message_stats: Option<RabbitMQMessageStats>,
    consumer_utilisation: Option<f64>
}

#[derive(Debug, Deserialize)]
struct RabbitMQMessageStats {
    publish: Option<u64>,
    publish_details: Option<RabbitMQDetails>,
    ack: Option<u64>,
    ack_details: Option<RabbitMQDetails>,
    deliver: Option<u64>,
    deliver_details: Option<RabbitMQDetails>,
    redeliver: Option<u64>,
    redeliver_details: Option<RabbitMQDetails>,
}

impl Default for RabbitMQMessageStats {
    fn default() -> Self {
        RabbitMQMessageStats {
            publish: None,
            publish_details: None,
            ack: None,
            ack_details: None,
            deliver: None,
            deliver_details: None,
            redeliver: None,
            redeliver_details: None
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RabbitMQDetails {
    rate: f64
}

#[tokio::test(flavor="multi_thread", worker_threads=1)]
async fn test_collect1() {
    use crate::model::TimeInterval;

    let mut metric_definitions = MetricDefinitions::new();
    let mut rabbitmq_stats_collector = RabbitMQStatsCollector::new(
        &RabbitMQMetricsConfig::default(),
        &mut metric_definitions
    ).await.unwrap();

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    rabbitmq_stats_collector.collect(TimePoint::now(), &mut metrics).await.unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_specific_name(metric).unwrap(), value)
    }
}