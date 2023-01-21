use std::collections::HashMap;

use fnv::FnvHashMap;

use futures_util::future::join_all;
use futures_util::stream::StreamExt;

use bollard::container::ListContainersOptions;
use bollard::Docker;

use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{EventResult, MetricId, MetricName, TimePoint};

pub struct DockerStatsCollector {
    docker: Docker,
    prev_stats: FnvHashMap<String, DockerStatsEntry>
}

impl DockerStatsCollector {
    pub async fn new(metric_definitions: &mut MetricDefinitions) -> EventResult<DockerStatsCollector> {
        let mut collector = DockerStatsCollector {
            docker: Docker::connect_with_socket_defaults()?,
            prev_stats: FnvHashMap::default()
        };

        let mut filters = HashMap::new();
        filters.insert(String::from("status"), vec![String::from("running")]);
        let containers = &collector.docker
            .list_containers(
                Some(
                    ListContainersOptions {
                        all: true,
                        filters,
                        ..Default::default()
                    }
                ))
            .await?;

        for container in containers {
            if let Some(id) = container.id.as_ref() {
                let name = container.names.as_ref().map(|names| names.get(0)).flatten().unwrap_or(id);
                let name = name.replace("/", "");

                collector.prev_stats.insert(
                    id.to_owned(),
                    DockerStatsEntry {
                        name: name.clone(),
                        stats: collector.get_container_docker_stats(id).await.unwrap(),
                        cpu_usage_metric: metric_definitions.define(MetricName::sub("docker.container.cpu_usage", &name)),
                        used_memory_bytes_metric: metric_definitions.define(MetricName::sub("docker.container.used_memory_bytes", &name)),
                        used_memory_ratio_metric: metric_definitions.define(MetricName::sub("docker.container.used_memory_ratio", &name)),
                        total_memory_bytes_metric: metric_definitions.define(MetricName::sub("docker.container.total_memory_bytes", &name))
                    }
                );
            }
        }

        Ok(collector)
    }

    pub async fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()> {
        let memory_scale = 1.0 / (1024.0 * 1024.0);

        let docker_stats_futures = self.prev_stats.keys().map(|id| self.get_container_docker_stats(id));
        let docker_stats = join_all(docker_stats_futures).await;

        for (entry, new_stats) in self.prev_stats.values_mut().zip(docker_stats) {
            if let Some(new_stats) = new_stats {
                let prev_stats = &mut entry.stats;

                let diff_usage = new_stats.cpu_stats.cpu_usage.total_usage - prev_stats.cpu_stats.cpu_usage.total_usage;
                let diff_system = new_stats.cpu_stats.system_cpu_usage.unwrap_or(0) - prev_stats.cpu_stats.system_cpu_usage.unwrap_or(0);
                let cpu_usage = diff_usage as f64 / diff_system as f64;

                let used_memory = new_stats.memory_stats.usage.unwrap_or(0);
                let total_memory = new_stats.memory_stats.limit.unwrap_or(0);

                metrics.insert(time, entry.cpu_usage_metric, cpu_usage);
                metrics.insert(time, entry.used_memory_bytes_metric, used_memory as f64 * memory_scale);
                metrics.insert(time, entry.used_memory_ratio_metric, used_memory as f64 / total_memory as f64);
                metrics.insert(time, entry.total_memory_bytes_metric, total_memory as f64 * memory_scale);

                *prev_stats = new_stats;
            }
        }

        Ok(())
    }

    async fn get_container_docker_stats(&self, id: &str) -> Option<bollard::container::Stats> {
        let mut stream = self.docker.stats(id, None).take(1);

        while let Some(Ok(stats)) = stream.next().await {
            return Some(stats);
        }

        None
    }
}

struct DockerStatsEntry {
    name: String,
    stats: bollard::container::Stats,
    cpu_usage_metric: MetricId,
    used_memory_bytes_metric: MetricId,
    used_memory_ratio_metric: MetricId,
    total_memory_bytes_metric: MetricId,
}

#[tokio::test(flavor="multi_thread", worker_threads=1)]
async fn test_collect1() {
    use crate::model::TimeInterval;

    let mut metric_definitions = MetricDefinitions::new();
    let mut docker_stats_collector = DockerStatsCollector::new(&mut metric_definitions).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs_f64(0.25)).await;

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    docker_stats_collector.collect(TimePoint::now(), &mut metrics).await.unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_specific_name(metric).unwrap(), value)
    }
}