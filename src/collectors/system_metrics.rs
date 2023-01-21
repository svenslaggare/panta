use std::ffi::CString;
use std::io::ErrorKind;
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;
use fnv::FnvHashMap;

use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{EventError, EventResult, MetricId, MetricName, TimePoint};

pub trait SystemMetricCollector {
    fn new(metric_definitions: &mut MetricDefinitions) -> EventResult<Self> where Self: Sized;
    fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()>;
}

pub struct SystemMetricsCollector {
    collectors: Vec<Box<dyn SystemMetricCollector>>
}

impl SystemMetricsCollector {
    pub fn new(metric_definitions: &mut MetricDefinitions) -> EventResult<SystemMetricsCollector> {
        Ok(
            SystemMetricsCollector {
                collectors: vec![
                    Box::new(CpuUsageCollector::new(metric_definitions)?),
                    Box::new(MemoryUsageCollector::new(metric_definitions)?),
                    Box::new(DiskUsageCollector::new(metric_definitions)?),
                    Box::new(DiskIOStatsCollector::new(metric_definitions)?),
                    Box::new(NetworkStatsCollector::new(metric_definitions)?)
                ]
            }
        )
    }

    pub fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()> {
        for collector in &mut self.collectors {
            collector.collect(time, metrics)?;
        }

        Ok(())
    }
}

pub struct CpuUsageCollector {
    prev_values: FnvHashMap<String, (MetricId, i64, i64)>
}

impl SystemMetricCollector for CpuUsageCollector {
    fn new(metric_definitions: &mut MetricDefinitions) -> EventResult<CpuUsageCollector> {
        let mut prev_values = FnvHashMap::default();
        let content = CpuUsageCollector::get_cpu_content()?;
        for (core_name, (total, idle)) in CpuUsageCollector::get_cpu_values(&content) {
            let metric_id = metric_definitions.define(MetricName::sub("system.cpu_usage", &core_name));
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
            if let Some((metric_id, prev_total, prev_idle)) = self.prev_values.get_mut(&core_name) {
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
    fn get_cpu_values<'a>(content: &'a str) -> impl Iterator<Item=(String, (i64, i64))> + 'a {
        content
            .lines()
            .map(|line| {
                let parts = line.split(" ").collect::<Vec<_>>();

                if parts[0].starts_with("cpu") {
                    let core_name = parts[0];
                    let int_parts = parts.iter().skip(1).map(|x| i64::from_str(x)).flatten().collect::<Vec<_>>();
                    let total = int_parts.iter().sum::<i64>();
                    let idle = int_parts[3];

                    let core_name = if core_name == "cpu" {
                        "all".to_owned()
                    } else {
                        core_name.replace("cpu", "core")
                    };

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

pub struct MemoryUsageCollector {
    total_memory_metric: MetricId,
    used_memory_metric: MetricId,
    used_memory_ratio_metric: MetricId,
    used_memory_rate_metric: MetricId,
    available_memory_metric: MetricId,
    available_memory_metric_ratio: MetricId,
    prev_memory_usage: Option<(Instant, MemoryUsage)>,
}

impl SystemMetricCollector for MemoryUsageCollector {
    fn new(metric_definitions: &mut MetricDefinitions) -> EventResult<MemoryUsageCollector> {
        Ok(
            MemoryUsageCollector {
                total_memory_metric: metric_definitions.define(MetricName::all("system.total_memory_bytes")),
                used_memory_metric: metric_definitions.define(MetricName::all("system.used_memory_bytes")),
                used_memory_ratio_metric: metric_definitions.define(MetricName::all("system.used_memory_ratio")),
                used_memory_rate_metric: metric_definitions.define(MetricName::all("system.used_memory_bytes.rate")),
                available_memory_metric: metric_definitions.define(MetricName::all("system.available_memory_bytes")),
                available_memory_metric_ratio: metric_definitions.define(MetricName::all("system.available_memory_ratio")),
                prev_memory_usage: Some((Instant::now(), MemoryUsageCollector::get_memory_usage()?))
            }
        )
    }

    fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()> {
        let measurement_time = Instant::now();
        let memory_usage = MemoryUsageCollector::get_memory_usage()?;

        metrics.insert(time, self.total_memory_metric, memory_usage.total_memory);
        metrics.insert(time, self.used_memory_metric, memory_usage.used_memory);
        metrics.insert(time, self.used_memory_ratio_metric, memory_usage.used_memory / memory_usage.total_memory);
        metrics.insert(time, self.available_memory_metric, memory_usage.available_memory);
        metrics.insert(time, self.available_memory_metric_ratio, memory_usage.available_memory / memory_usage.total_memory);

        if let Some((prev_measurement_time, prev_memory_usage)) = self.prev_memory_usage.as_ref() {
            let elapsed = (measurement_time - *prev_measurement_time).as_secs_f64();
            metrics.insert(time, self.used_memory_rate_metric, (memory_usage.used_memory - prev_memory_usage.used_memory) / elapsed);
        }

        self.prev_memory_usage = Some((measurement_time, memory_usage));

        Ok(())
    }
}

impl MemoryUsageCollector {
    fn get_memory_usage() -> EventResult<MemoryUsage> {
        let mut memory_usage = MemoryUsage {
            total_memory: 0.0,
            used_memory: 0.0,
            available_memory: 0.0
        };

        for line in std::fs::read_to_string("/proc/meminfo").map_err(|err| EventError::FailedToCollectSystemMetric(err))?.lines() {
            let parts = line.split(":").collect::<Vec<_>>();
            let name = parts[0];
            let value = f64::from_str(parts[1].trim().split(" ").next().unwrap()).unwrap() / 1024.0;
            match name {
                "MemTotal" => {
                    memory_usage.total_memory = value;
                }
                "MemAvailable" => {
                    memory_usage.available_memory = value;
                    memory_usage.used_memory = memory_usage.total_memory - memory_usage.available_memory;
                }
                _ => {}
            }
        }

        Ok(memory_usage)
    }
}

struct MemoryUsage {
    total_memory: f64,
    used_memory: f64,
    available_memory: f64,
}

pub struct DiskIOStatsCollector {
    prev_values: FnvHashMap<String, (DiskStatsMetrics, DiskStats)>,
    prev_measurement_time: Instant
}

impl SystemMetricCollector for DiskIOStatsCollector {
    fn new(metric_definitions: &mut MetricDefinitions) -> EventResult<DiskIOStatsCollector> {
        let mut prev_values = FnvHashMap::default();

        let measurement_time = Instant::now();
        let content = DiskIOStatsCollector::get_disk_content()?;
        for (disk, disk_stats) in DiskIOStatsCollector::get_disk_stats_values(&content) {
            let disk_stats_metrics = DiskStatsMetrics {
                read_operations_rate_metric: metric_definitions.define(MetricName::sub("system.disk.read_operations.rate", disk)),
                read_bytes_rate_metric: metric_definitions.define(MetricName::sub("system.disk.read_bytes.rate", disk)),
                write_operations_rate_metric: metric_definitions.define(MetricName::sub("system.disk.write_operations.rate", disk)),
                write_bytes_rate_metric: metric_definitions.define(MetricName::sub("system.disk.write_bytes.rate", disk))
            };
            
            prev_values.insert(disk.to_owned(), (disk_stats_metrics, disk_stats));
        }

        Ok(
            DiskIOStatsCollector {
                prev_measurement_time: measurement_time,
                prev_values
            }
        )
    }

    fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()> {
        let scale = 1.0 / (1024.0 * 1024.0);

        let measurement_time = Instant::now();
        let elapsed_time = (measurement_time - self.prev_measurement_time).as_secs_f64();
        let content = DiskIOStatsCollector::get_disk_content()?;
        for (disk, disk_stats) in DiskIOStatsCollector::get_disk_stats_values(&content) {
            if let Some((disk_stats_metrics, prev_disk_stats)) = self.prev_values.get_mut(disk) {
                let diff_disk_stats = disk_stats.diff(prev_disk_stats);
                metrics.insert(time, disk_stats_metrics.read_operations_rate_metric, diff_disk_stats.read_ios as f64 / elapsed_time);
                metrics.insert(time, disk_stats_metrics.write_operations_rate_metric, diff_disk_stats.write_ios as f64 / elapsed_time);
                metrics.insert(time, disk_stats_metrics.read_bytes_rate_metric, scale * (diff_disk_stats.read_bytes as f64 / elapsed_time));
                metrics.insert(time, disk_stats_metrics.write_bytes_rate_metric, scale * (diff_disk_stats.write_bytes as f64 / elapsed_time));

                // To handle when sampling too fast
                if elapsed_time > 1.0 {
                    *prev_disk_stats = disk_stats;
                    self.prev_measurement_time = measurement_time;
                }
            }
        }

        Ok(())
    }

}

impl DiskIOStatsCollector {
    fn get_disk_stats_values<'a>(content: &'a str) -> impl Iterator<Item=(&'a str, DiskStats)> + 'a {
        content
            .lines()
            .map(|line| {
                let parts = line.split(" ").filter(|p| !p.is_empty()).skip(2).collect::<Vec<_>>();
                let block_size = 512;

                if parts[0].starts_with("loop") || parts[0].starts_with("dm") {
                    return None;
                }

                Some(
                    (
                        parts[0],
                        DiskStats {
                            read_ios: i64::from_str(parts[1]).unwrap(),
                            read_bytes: i64::from_str(parts[3]).unwrap() * block_size,
                            write_ios: i64::from_str(parts[5]).unwrap(),
                            write_bytes: i64::from_str(parts[7]).unwrap() * block_size
                        }
                    )
                )
            })
            .flatten()
    }

    fn get_disk_content() -> EventResult<String> {
        std::fs::read_to_string("/proc/diskstats")
            .map_err(|err| EventError::FailedToCollectSystemMetric(err))
    }
}

struct DiskStatsMetrics {
    read_operations_rate_metric: MetricId,
    read_bytes_rate_metric: MetricId,
    write_operations_rate_metric: MetricId,
    write_bytes_rate_metric: MetricId,
}

struct DiskStats {
    read_ios: i64,
    read_bytes: i64,
    write_ios: i64,
    write_bytes: i64
}

impl DiskStats {
    pub fn diff(&self, other: &DiskStats) -> DiskStats {
        DiskStats {
            read_ios: self.read_ios - other.read_ios,
            read_bytes: self.read_bytes - other.read_bytes,
            write_ios: self.write_ios - other.write_ios,
            write_bytes: self.write_bytes - other.write_bytes
        }
    }
}

pub struct DiskUsageCollector {
    disk_metrics: FnvHashMap<String, DiskUsageEntry>
}

impl SystemMetricCollector for DiskUsageCollector {
    fn new(metric_definitions: &mut MetricDefinitions) -> EventResult<DiskUsageCollector> {
        let mut disk_metrics = FnvHashMap::default();
        let parents = get_block_device_parents().map_err(|err| EventError::FailedToCollectSystemMetric(err))?;

        let content = std::fs::read_to_string("/proc/self/mountinfo").map_err(|err| EventError::FailedToCollectSystemMetric(err))?;
        for line in content.lines() {
            let parts = line.split(" ").collect::<Vec<_>>();
            let mount = parts[4];
            let filesystem_type = parts[8];
            let device_id = parts[2];

            match filesystem_type {
                "ext4" => {}
                _ => { continue; }
            }

            let block_device = Path::new("/sys/dev/block").join(device_id);
            let mut block_device = block_device.canonicalize().map_err(|err| EventError::FailedToCollectSystemMetric(err))?;

            // LVM device, use parent to find disk identifier
            if block_device.to_str().unwrap().contains("virtual") {
                if let Some(parent) = parents.get(&block_device) {
                    block_device = parent.clone();
                }
            }

            let disk = block_device.iter().last().unwrap().to_str().unwrap();

            disk_metrics.insert(
                disk.to_owned(),
                DiskUsageEntry {
                    mount: CString::new(mount).unwrap(),
                    total_disk_metric: metric_definitions.define(MetricName::sub("system.disk.total_bytes", disk)),
                    used_disk_metric: metric_definitions.define(MetricName::sub("system.disk.used_bytes", disk)),
                    used_disk_ratio_metric: metric_definitions.define(MetricName::sub("system.disk.used_ratio", disk)),
                    free_disk_metric: metric_definitions.define(MetricName::sub("system.disk.free_bytes", disk))
                }
            );
        }

        Ok(
            DiskUsageCollector {
                disk_metrics
            }
        )
    }

    fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()> {
        let scale = 1.0 / (1024.0 * 1024.0 * 1024.0);

        for entry in self.disk_metrics.values() {
            unsafe {
                let mut statfs_result = MaybeUninit::<libc::statfs64>::zeroed();
                if libc::statfs64(entry.mount.as_ptr(), statfs_result.as_mut_ptr()) != 0 {
                    return Err(EventError::FailedToCollectSystemMetric(std::io::Error::new(ErrorKind::Other, "statfs64 failed")));
                }
                let statfs_result = statfs_result.assume_init();

                let block_size = statfs_result.f_frsize as u64;
                let total_disk = block_size * statfs_result.f_blocks;
                let free_disk = block_size * statfs_result.f_bavail;
                let used_disk = total_disk - free_disk;

                metrics.insert(time, entry.total_disk_metric, scale * total_disk as f64);
                metrics.insert(time, entry.used_disk_metric, scale * used_disk as f64);
                metrics.insert(time, entry.used_disk_ratio_metric, used_disk as f64 / total_disk as f64);
                metrics.insert(time, entry.free_disk_metric, scale * free_disk as f64);
            }
        }

        Ok(())
    }
}

fn get_block_device_parents() -> std::io::Result<FnvHashMap<PathBuf, PathBuf>> {
    let mut parents = FnvHashMap::default();
    for parent in std::fs::read_dir("/sys/dev/block")? {
        let parent = parent?;
        let parent = parent.path().canonicalize()?;

        for child in std::fs::read_dir(parent.join("holders"))? {
            let child = child?;
            let child = child.path().canonicalize()?;
            parents.insert(child, parent.clone());
        }
    }

    Ok(parents)
}

pub struct DiskUsageEntry {
    mount: CString,
    total_disk_metric: MetricId,
    used_disk_metric: MetricId,
    used_disk_ratio_metric: MetricId,
    free_disk_metric: MetricId
}

pub struct NetworkStatsCollector {
    prev_values: FnvHashMap<String, (NetworkStatsMetrics, NetworkStats)>,
    prev_measurement_time: Instant
}

impl SystemMetricCollector for NetworkStatsCollector {
    fn new(metric_definitions: &mut MetricDefinitions) -> EventResult<NetworkStatsCollector> {
        let mut prev_values = FnvHashMap::default();

        let measurement_time = Instant::now();
        let content = NetworkStatsCollector::get_network_stats_content()?;
        for (interface, network_stats) in NetworkStatsCollector::get_network_stats_values(&content) {
            let network_stats_metrics = NetworkStatsMetrics {
                received_packets_metric: metric_definitions.define(MetricName::sub("system.net.received_packets.rate", interface)),
                received_bytes_metric: metric_definitions.define(MetricName::sub("system.net.received_bytes.rate", interface)),
                sent_packets_metric: metric_definitions.define(MetricName::sub("system.net.sent_packets.rate", interface)),
                sent_bytes_metric: metric_definitions.define(MetricName::sub("system.net.sent_bytes.rate", interface))
            };

            prev_values.insert(interface.to_owned(), (network_stats_metrics, network_stats));
        }

        Ok(
            NetworkStatsCollector {
                prev_measurement_time: measurement_time,
                prev_values
            }
        )
    }

    fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()> {
        let scale = 1.0 / (1024.0 * 1024.0);

        let measurement_time = Instant::now();
        let elapsed_time = (measurement_time - self.prev_measurement_time).as_secs_f64();
        let content = NetworkStatsCollector::get_network_stats_content()?;
        for (disk, disk_stats) in NetworkStatsCollector::get_network_stats_values(&content) {
            if let Some((disk_stats_metrics, prev_network_stats)) = self.prev_values.get_mut(disk) {
                let diff_network_stats = disk_stats.diff(prev_network_stats);
                metrics.insert(time, disk_stats_metrics.received_packets_metric, diff_network_stats.received_packets as f64 / elapsed_time);
                metrics.insert(time, disk_stats_metrics.sent_packets_metric, diff_network_stats.sent_packets as f64 / elapsed_time);
                metrics.insert(time, disk_stats_metrics.received_bytes_metric, scale * (diff_network_stats.received_bytes as f64 / elapsed_time));
                metrics.insert(time, disk_stats_metrics.sent_bytes_metric, scale * (diff_network_stats.sent_bytes as f64 / elapsed_time));

                // To handle when sampling too fast
                if elapsed_time > 0.5 {
                    *prev_network_stats = disk_stats;
                    self.prev_measurement_time = measurement_time;
                }
            }
        }

        Ok(())
    }

}

impl NetworkStatsCollector {
    fn get_network_stats_values<'a>(content: &'a str) -> impl Iterator<Item=(&'a str, NetworkStats)> + 'a {
        content
            .lines()
            .skip(2)
            .map(|line| {
                let parts = line.split(" ").filter(|p| !p.is_empty()).collect::<Vec<_>>();

                Some(
                    (
                        parts[0].split(":").next().unwrap(),
                        NetworkStats {
                            received_packets: i64::from_str(parts[2]).unwrap(),
                            received_bytes: i64::from_str(parts[1]).unwrap(),
                            sent_packets: i64::from_str(parts[10]).unwrap(),
                            sent_bytes: i64::from_str(parts[9]).unwrap()
                        }
                    )
                )
            })
            .flatten()
    }

    fn get_network_stats_content() -> EventResult<String> {
        std::fs::read_to_string("/proc/net/dev")
            .map_err(|err| EventError::FailedToCollectSystemMetric(err))
    }
}

struct NetworkStatsMetrics {
    received_packets_metric: MetricId,
    received_bytes_metric: MetricId,
    sent_packets_metric: MetricId,
    sent_bytes_metric: MetricId,
}

#[derive(Debug)]
struct NetworkStats {
    received_packets: i64,
    received_bytes: i64,
    sent_packets: i64,
    sent_bytes: i64
}

impl NetworkStats {
    pub fn diff(&self, other: &NetworkStats) -> NetworkStats {
        NetworkStats {
            received_packets: self.received_packets - other.received_packets,
            received_bytes: self.received_bytes - other.received_bytes,
            sent_packets: self.sent_packets - other.sent_packets,
            sent_bytes: self.sent_bytes - other.sent_bytes
        }
    }
}

#[test]
fn test_system_metrics_collector1() {
    use crate::model::TimeInterval;

    let mut metric_definitions = MetricDefinitions::new();
    let mut system_metrics_collector = SystemMetricsCollector::new(&mut metric_definitions).unwrap();

    std::thread::sleep(std::time::Duration::from_secs_f64(0.2));

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    system_metrics_collector.collect(TimePoint::now(), &mut metrics).unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_specific_name(metric).unwrap(), value)
    }
}

#[test]
fn test_cpu_collector1() {
    use crate::model::TimeInterval;

    let mut metric_definitions = MetricDefinitions::new();
    let mut cpu_usage_collector = CpuUsageCollector::new(&mut metric_definitions).unwrap();

    std::thread::sleep(std::time::Duration::from_secs_f64(0.1));

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    cpu_usage_collector.collect(TimePoint::now(), &mut metrics).unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_specific_name(metric).unwrap(), value)
    }

    assert_ne!(Some(&0.0), metrics.get(&metric_definitions.get_specific_id(&MetricName::sub("system.cpu_usage", "all")).unwrap()));
}

#[test]
fn test_memory_collector1() {
    use crate::model::TimeInterval;

    let mut metric_definitions = MetricDefinitions::new();
    let mut memory_usage_collector = MemoryUsageCollector::new(&mut metric_definitions).unwrap();

    std::thread::sleep(std::time::Duration::from_secs_f64(0.2));

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    memory_usage_collector.collect(TimePoint::now(), &mut metrics).unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_specific_name(metric).unwrap(), value)
    }

    assert_ne!(Some(&0.0), metrics.get(&metric_definitions.get_specific_id(&MetricName::all("system.used_memory_ratio")).unwrap()));
}

#[test]
fn test_disk_io_stats_collector1() {
    use crate::model::TimeInterval;

    let mut metric_definitions = MetricDefinitions::new();
    let mut disk_io_stats_collector = DiskIOStatsCollector::new(&mut metric_definitions).unwrap();

    std::thread::sleep(std::time::Duration::from_secs_f64(1.1));

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    disk_io_stats_collector.collect(TimePoint::now(), &mut metrics).unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_specific_name(metric).unwrap(), value)
    }
}

#[test]
fn test_disk_usage_collector1() {
    use crate::model::TimeInterval;

    let mut metric_definitions = MetricDefinitions::new();
    let mut disk_usage_collector = DiskUsageCollector::new(&mut metric_definitions).unwrap();

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    disk_usage_collector.collect(TimePoint::now(), &mut metrics).unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_specific_name(metric).unwrap(), value)
    }
}

#[test]
fn test_network_stats_collector1() {
    use crate::model::TimeInterval;

    let mut metric_definitions = MetricDefinitions::new();
    let mut network_stats_collector = NetworkStatsCollector::new(&mut metric_definitions).unwrap();

    std::thread::sleep(std::time::Duration::from_secs_f64(0.5));

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    network_stats_collector.collect(TimePoint::now(), &mut metrics).unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_specific_name(metric).unwrap(), value)
    }
}