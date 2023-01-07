use std::str::FromStr;
use fnv::FnvHashMap;

use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{EventError, EventResult, MetricId, TimeInterval, TimePoint};

pub trait SystemMetricCollector {
    fn new(metric_definitions: &mut MetricDefinitions) -> EventResult<Self> where Self: Sized;
    fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()>;
}

pub struct CpuUsageCollector {
    prev_values: FnvHashMap<String, (MetricId, i64, i64)>
}

impl SystemMetricCollector for CpuUsageCollector {
    fn new(metric_definitions: &mut MetricDefinitions) -> EventResult<CpuUsageCollector> {
        let mut prev_values = FnvHashMap::default();
        let content = CpuUsageCollector::get_cpu_content()?;
        for (core_name, (total, idle)) in CpuUsageCollector::get_cpu_values(&content) {
            let metric_id = metric_definitions.define(core_name);
            prev_values.insert(core_name.to_owned(), (metric_id, total, idle));
        }

        Ok(
            CpuUsageCollector {
                prev_values
            }
        )
    }

    fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()> {
        let content = CpuUsageCollector::get_cpu_content()?;
        for (core_name, (total, idle)) in CpuUsageCollector::get_cpu_values(&content) {
            if let Some((metric_id, prev_total, prev_idle)) = self.prev_values.get_mut(core_name) {
                let diff_total = total - *prev_total;
                let diff_idle = idle - *prev_idle;
                let cpu_usage = if diff_total > 0 {1.0 - diff_idle as f64 / diff_total as f64} else {0.0};
                *prev_total = total;
                *prev_idle = idle;
                metrics.insert(time, *metric_id, cpu_usage);
            }
        }

        Ok(())
    }

}

impl CpuUsageCollector {
    fn get_cpu_values<'a>(content: &'a str) -> impl Iterator<Item=(&'a str, (i64, i64))> + 'a {
        content
            .lines()
            .map(|line| {
                let parts = line.split(" ").collect::<Vec<_>>();

                if parts[0].starts_with("cpu") {
                    let core_name = parts[0];
                    let int_parts = parts.iter().skip(1).map(|x| i64::from_str(x)).flatten().collect::<Vec<_>>();
                    let total = int_parts.iter().sum::<i64>();
                    let idle = int_parts[3];

                    Some((core_name, (total, idle)))
                } else {
                    None
                }
            })
            .flatten()
    }

    fn get_cpu_content() -> EventResult<String> {
        std::fs::read_to_string("/proc/stat")
            .map_err(|err| EventError::FailedToCollectSystemMetric(err))
    }
}

struct MemoryUsageCollector {
    total_memory_metric: MetricId,
    used_memory_metric: MetricId,
    used_memory_ratio_metric: MetricId
}

impl SystemMetricCollector for MemoryUsageCollector {
    fn new(metric_definitions: &mut MetricDefinitions) -> EventResult<MemoryUsageCollector> {
        Ok(
            MemoryUsageCollector {
                total_memory_metric: metric_definitions.define("total_memory"),
                used_memory_metric: metric_definitions.define("used_memory"),
                used_memory_ratio_metric: metric_definitions.define("used_memory_ratio"),
            }
        )
    }

    fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()> {
        let mut total_memory = 0.0;
        let mut used_memory = 0.0;

        for line in std::fs::read_to_string("/proc/meminfo").map_err(|err| EventError::FailedToCollectSystemMetric(err))?.lines() {
            let parts = line.split(":").collect::<Vec<_>>();
            let name = parts[0];
            let value = f64::from_str(parts[1].trim().split(" ").next().unwrap()).unwrap() / 1024.0;
            match name {
                "MemTotal" => {
                    total_memory = value;
                }
                "MemAvailable" => {
                    used_memory = total_memory - value;
                }
                _ => {}
            }
        }

        metrics.insert(time, self.total_memory_metric, total_memory);
        metrics.insert(time, self.used_memory_metric, used_memory);
        metrics.insert(time, self.used_memory_ratio_metric, used_memory / total_memory);

        Ok(())
    }
}

#[test]
fn test_cpu_collector1() {
    let mut metric_definitions = MetricDefinitions::new();
    let mut cpu_usage_collector = CpuUsageCollector::new(&mut metric_definitions).unwrap();

    std::thread::sleep(std::time::Duration::from_secs_f64(0.1));

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    cpu_usage_collector.collect(TimePoint::now(), &mut metrics).unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_name(metric).unwrap(), value)
    }

    assert_ne!(Some(&0.0), metrics.get(&metric_definitions.get_id("cpu").unwrap()));
}

#[test]
fn test_memory_collector1() {
    let mut metric_definitions = MetricDefinitions::new();
    let mut memory_usage_collector = MemoryUsageCollector::new(&mut metric_definitions).unwrap();

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    memory_usage_collector.collect(TimePoint::now(), &mut metrics).unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_name(metric).unwrap(), value)
    }

    assert_ne!(Some(&0.0), metrics.get(&metric_definitions.get_id("used_memory_ratio").unwrap()));
}