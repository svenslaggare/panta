use std::collections::HashMap;
use std::str::FromStr;

use fnv::{FnvHashMap, FnvHashSet};

use bollard::container::{ListContainersOptions};
use bollard::Docker;
use bollard::models::ContainerSummary;
use log::debug;

use crate::collectors::system::{CpuUsageCollector, MemoryUsageCollector};
use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{EventError, EventResult, MetricId, MetricName, TimePoint};

pub struct DockerStatsCollector {
    docker: Docker,
    system_stats: DockerStats,
    container_stats: FnvHashMap<String, DockerStatsEntry>
}

impl DockerStatsCollector {
    pub async fn new(metric_definitions: &mut MetricDefinitions) -> EventResult<DockerStatsCollector> {
        let mut collector = DockerStatsCollector {
            docker: Docker::connect_with_socket_defaults()?,
            system_stats: DockerStatsCollector::get_system_stats()?,
            container_stats: FnvHashMap::default()
        };

        collector.discover(metric_definitions).await?;

        Ok(collector)
    }

    pub async fn discover(&mut self, metric_definitions: &mut MetricDefinitions) -> EventResult<bool> {
        let mut found = FnvHashSet::default();
        let mut added = false;
        for container in DockerStatsCollector::get_containers(&self.docker).await? {
            if let Some(id) = container.id.as_ref() {
                found.insert(id.clone());

                if !self.container_stats.contains_key(id) {
                    let name = container.names.as_ref().map(|names| names.get(0)).flatten().unwrap_or(id);
                    let name = name.replace("/", "");

                    added = true;
                    self.container_stats.insert(
                        id.clone(),
                        DockerStatsEntry {
                            id: id.clone(),
                            name: name.clone(),
                            stats: DockerStatsCollector::get_container_docker_stats(id)?,
                            cpu_usage_metric: metric_definitions.define(MetricName::sub("docker.container.cpu_usage", &name)),
                            used_memory_bytes_metric: metric_definitions.define(MetricName::sub("docker.container.used_memory_bytes", &name)),
                            used_memory_ratio_metric: metric_definitions.define(MetricName::sub("docker.container.used_memory_ratio", &name)),
                            total_memory_bytes_metric: metric_definitions.define(MetricName::sub("docker.container.total_memory_bytes", &name))
                        }
                    );
                }
            }
        }

        let all = FnvHashSet::from_iter(self.container_stats.keys().cloned());
        for old in all.difference(&found) {
            self.container_stats.remove(old);
        }

        debug!("Containers:");
        for container in &self.container_stats {
            debug!("\t{}", container.1.name);
        }

        Ok(added)
    }

    pub async fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()> {
        let memory_scale = 1.0 / (1024.0 * 1024.0);

        let new_system_stats = DockerStatsCollector::get_system_stats()?;
        for entry in self.container_stats.values_mut() {
            if let Ok(new_stats) = DockerStatsCollector::get_container_docker_stats(&entry.id) {
                let prev_stats = &mut entry.stats;

                let diff_usage = new_stats.total_cpu_usage - prev_stats.total_cpu_usage;
                let diff_system = new_system_stats.total_cpu_usage - self.system_stats.total_cpu_usage;
                let cpu_usage = diff_usage as f64 / diff_system as f64;

                let used_memory = new_stats.used_memory_bytes;
                let total_memory = new_stats.total_memory_bytes.or(new_system_stats.total_memory_bytes).unwrap_or(0);

                metrics.insert(time, entry.cpu_usage_metric, cpu_usage);
                metrics.insert(time, entry.used_memory_bytes_metric, used_memory as f64 * memory_scale);
                metrics.insert(time, entry.used_memory_ratio_metric, used_memory as f64 / total_memory as f64);
                metrics.insert(time, entry.total_memory_bytes_metric, total_memory as f64 * memory_scale);

                *prev_stats = new_stats;
            }
        }
        self.system_stats = new_system_stats;

        Ok(())
    }

    fn get_system_stats() -> EventResult<DockerStats> {
        let (_, (cpu_usage, _)) = CpuUsageCollector::get_cpu_values(&CpuUsageCollector::get_cpu_content()?).next().unwrap();
        let memory_usage = MemoryUsageCollector::get_memory_usage()?;

        Ok(
            DockerStats {
                total_cpu_usage: cpu_usage * (1_000_000_000 / 100), // convert to nanoseconds
                used_memory_bytes: 0,
                total_memory_bytes: Some((memory_usage.total_memory * (1024.0 * 1024.0)) as i64)
            }
        )
    }

    fn get_container_docker_stats(id: &str) -> EventResult<DockerStats> {
        let read_to_string = |path: String| {
            std::fs::read_to_string(path).map_err(|err| EventError::FailedToCollectDockerMetric(err.to_string()))
        };

        let content = read_to_string(format!("/sys/fs/cgroup/cpu/docker/{}/cpuacct.usage", id))?;
        let total_cpu_usage = i64::from_str(content.trim_end()).map_err(|err| EventError::FailedToCollectDockerMetric(err.to_string()))?;

        let content = read_to_string(format!("/sys/fs/cgroup/memory/docker/{}/memory.usage_in_bytes", id))?;
        let used_memory_bytes = i64::from_str(content.trim_end()).map_err(|err| EventError::FailedToCollectDockerMetric(err.to_string()))?;

        let content = read_to_string(format!("/sys/fs/cgroup/memory/docker/{}/memory.limit_in_bytes", id))?;
        let total_memory_bytes = i64::from_str(content.trim_end()).map_err(|err| EventError::FailedToCollectDockerMetric(err.to_string()))?;
        let total_memory_bytes = if total_memory_bytes < (i64::MAX / 4096) * 4096 {Some(total_memory_bytes)} else {None};

        Ok(
            DockerStats {
                total_cpu_usage,
                used_memory_bytes,
                total_memory_bytes
            }
        )
    }

    async fn get_containers(docker: &Docker) -> EventResult<Vec<ContainerSummary>> {
        let mut filters = HashMap::new();
        filters.insert(String::from("status"), vec![String::from("running")]);
        let containers = docker
            .list_containers(
                Some(
                    ListContainersOptions {
                        all: true,
                        filters,
                        ..Default::default()
                    }
                ))
            .await?;
        Ok(containers)
    }
}

struct DockerStatsEntry {
    id: String,
    name: String,
    stats: DockerStats,
    cpu_usage_metric: MetricId,
    used_memory_bytes_metric: MetricId,
    used_memory_ratio_metric: MetricId,
    total_memory_bytes_metric: MetricId,
}

struct DockerStats {
    total_cpu_usage: i64,
    used_memory_bytes: i64,
    total_memory_bytes: Option<i64>
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