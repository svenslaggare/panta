use fnv::FnvHashMap;
use serde::Deserialize;

use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{EventError, EventResult, MetricId, TimeInterval, TimePoint};

pub struct RabbitMQStatsCollector {
    base_url: String,
    username: String,
    password: String,
    virtual_host: String,
    queues: FnvHashMap<String, QueueEntry>,
    client: reqwest::Client
}

impl RabbitMQStatsCollector {
    pub async fn new(base_url: &str, username: &str, password: &str,
                     metric_definitions: &mut MetricDefinitions) -> EventResult<RabbitMQStatsCollector> {
        let mut collector = RabbitMQStatsCollector {
            base_url: base_url.to_owned(),
            username: username.to_owned(),
            password: password.to_owned(),
            virtual_host: "%2F".to_owned(),
            client: reqwest::Client::new(),
            queues: FnvHashMap::default()
        };

        let queues = collector.get_queues().await.map_err(|err| EventError::FailedToCollectRabbitMQMetric(err))?;
        collector.queues = FnvHashMap::from_iter(
            queues
                .into_iter()
                .map(|queue|
                    (
                        queue.name.clone(),
                        QueueEntry {
                            name: queue.name.clone(),

                            message_ready_count_metric: metric_definitions.define(&format!("rabbitmq.message_ready_count:{}", queue.name)),
                            message_ready_rate_metric: metric_definitions.define(&format!("rabbitmq.message_ready_rate:{}", queue.name)),

                            publish_count_metric: metric_definitions.define(&format!("rabbitmq.publish_count:{}", queue.name)),
                            publish_rate_metric: metric_definitions.define(&format!("rabbitmq.publish_rate:{}", queue.name)),

                            ack_count_metric: metric_definitions.define(&format!("rabbitmq.ack_count:{}", queue.name)),
                            ack_rate_metric: metric_definitions.define(&format!("rabbitmq.ack_rate:{}", queue.name)),

                            deliver_count_metric: metric_definitions.define(&format!("rabbitmq.deliver_count:{}", queue.name)),
                            deliver_rate_metric: metric_definitions.define(&format!("rabbitmq.deliver_rate:{}", queue.name)),

                            redeliver_count_metric: metric_definitions.define(&format!("rabbitmq.redeliver_count:{}", queue.name)),
                            redeliver_rate_metric: metric_definitions.define(&format!("rabbitmq.redeliver_rate:{}", queue.name)),

                            unacknowledged_count_metric: metric_definitions.define(&format!("rabbitmq.unacknowledged_count:{}", queue.name)),
                            unacknowledged_rate_metric: metric_definitions.define(&format!("rabbitmq.unacknowledged_rate:{}", queue.name)),

                            consumer_utilisation_metric: metric_definitions.define(&format!("rabbitmq.consumer_utilisation:{}", queue.name))
                        }
                    )
                )
        );

        Ok(collector)
    }

    pub async fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()> {
        let queue_infos = self.collect_all_queue_info().await.map_err(|err| EventError::FailedToCollectRabbitMQMetric(err))?;

        for queue_info in queue_infos {
            if let Some(queue_entry) = self.queues.get(&queue_info.name) {
                metrics.insert(time, queue_entry.message_ready_count_metric, queue_info.messages_ready.unwrap_or(0) as f64);
                metrics.insert(time, queue_entry.message_ready_rate_metric, queue_info.messages_ready_details.map(|d| d.rate).unwrap_or(0.0));

                metrics.insert(time, queue_entry.publish_count_metric, queue_info.message_stats.publish.unwrap_or(0) as f64);
                metrics.insert(time, queue_entry.publish_rate_metric, queue_info.message_stats.publish_details.map(|d| d.rate).unwrap_or(0.0));

                metrics.insert(time, queue_entry.ack_count_metric, queue_info.message_stats.ack.unwrap_or(0) as f64);
                metrics.insert(time, queue_entry.ack_rate_metric, queue_info.message_stats.ack_details.map(|d| d.rate).unwrap_or(0.0));

                metrics.insert(time, queue_entry.deliver_count_metric, queue_info.message_stats.deliver.unwrap_or(0) as f64);
                metrics.insert(time, queue_entry.deliver_rate_metric, queue_info.message_stats.deliver_details.map(|d| d.rate).unwrap_or(0.0));

                metrics.insert(time, queue_entry.redeliver_count_metric, queue_info.message_stats.redeliver.unwrap_or(0) as f64);
                metrics.insert(time, queue_entry.redeliver_rate_metric, queue_info.message_stats.redeliver_details.map(|d| d.rate).unwrap_or(0.0));

                metrics.insert(time, queue_entry.unacknowledged_count_metric, queue_info.messages_unacknowledged.unwrap_or(0) as f64);
                metrics.insert(time, queue_entry.unacknowledged_rate_metric, queue_info.messages_unacknowledged_details.map(|d| d.rate).unwrap_or(0.0));

                metrics.insert(time, queue_entry.consumer_utilisation_metric, queue_info.consumer_utilisation);
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

    async fn collect_queue_info(&self, queue: &str) -> Result<RabbitMQQueueInfo, reqwest::Error> {
        self.client.get(format!("{}/api/queues/{}/{}", self.base_url, self.virtual_host, queue))
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
    message_stats: RabbitMQMessageStats,
    consumer_utilisation: f64
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

#[derive(Debug, Deserialize)]
pub struct RabbitMQDetails {
    rate: f64
}

#[tokio::test(flavor="multi_thread", worker_threads=1)]
async fn test_collect1() {
    let mut metric_definitions = MetricDefinitions::new();
    let mut rabbitmq_stats_collector = RabbitMQStatsCollector::new(
        "http://localhost:15672", "guest", "guest",
        &mut metric_definitions
    ).await.unwrap();

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    rabbitmq_stats_collector.collect(TimePoint::now(), &mut metrics).await.unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_name(metric).unwrap(), value)
    }
}