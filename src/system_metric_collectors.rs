use std::ffi::CString;
use std::io::ErrorKind;
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;
use fnv::FnvHashMap;

use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{EventError, EventResult, MetricId, TimeInterval, TimePoint};

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
                    Box::new(DiskIOStatsCollector::new(metric_definitions)?)
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
            let metric_id = metric_definitions.define(&format!("cpu_usage:{}", core_name));
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
                read_operations_metric: metric_definitions.define(&format!("disk_read_operations:{}", disk)),
                read_bytes_metric: metric_definitions.define(&format!("disk_read_bytes:{}", disk)),
                write_operations_metric: metric_definitions.define(&format!("disk_write_operations:{}", disk)),
                write_bytes_metric: metric_definitions.define(&format!("disk_write_bytes:{}", disk))
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
                metrics.insert(time, disk_stats_metrics.read_operations_metric, diff_disk_stats.read_ios as f64 / elapsed_time);
                metrics.insert(time, disk_stats_metrics.write_operations_metric, diff_disk_stats.write_ios as f64 / elapsed_time);
                metrics.insert(time, disk_stats_metrics.read_bytes_metric, scale * (diff_disk_stats.read_bytes as f64 / elapsed_time));
                metrics.insert(time, disk_stats_metrics.write_bytes_metric, scale * (diff_disk_stats.write_bytes as f64 / elapsed_time));

                *prev_disk_stats = disk_stats;
                self.prev_measurement_time = measurement_time;
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
    read_operations_metric: MetricId,
    read_bytes_metric: MetricId,
    write_operations_metric: MetricId,
    write_bytes_metric: MetricId,
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
                    total_disk_metric: metric_definitions.define(&format!("total_disk:{}", disk)),
                    used_disk_metric: metric_definitions.define(&format!("used_disk:{}", disk)),
                    used_disk_ratio_metric: metric_definitions.define(&format!("used_disk_ratio:{}", disk)),
                    free_disk_metric: metric_definitions.define(&format!("free_disk:{}", disk))
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

#[test]
fn test_system_metrics_collector1() {
    let mut metric_definitions = MetricDefinitions::new();
    let mut system_metrics_collector = SystemMetricsCollector::new(&mut metric_definitions).unwrap();

    std::thread::sleep(std::time::Duration::from_secs_f64(0.2));

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    system_metrics_collector.collect(TimePoint::now(), &mut metrics).unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_name(metric).unwrap(), value)
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

    assert_ne!(Some(&0.0), metrics.get(&metric_definitions.get_id("cpu_usage:all").unwrap()));
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

#[test]
fn test_disk_io_stats_collector1() {
    let mut metric_definitions = MetricDefinitions::new();
    let mut disk_io_stats_collector = DiskIOStatsCollector::new(&mut metric_definitions).unwrap();

    std::thread::sleep(std::time::Duration::from_secs_f64(0.2));

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    disk_io_stats_collector.collect(TimePoint::now(), &mut metrics).unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_name(metric).unwrap(), value)
    }
}

#[test]
fn test_disk_usage_collector1() {
    let mut metric_definitions = MetricDefinitions::new();
    let mut disk_usage_collector = DiskUsageCollector::new(&mut metric_definitions).unwrap();

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    disk_usage_collector.collect(TimePoint::now(), &mut metrics).unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_name(metric).unwrap(), value)
    }
}