use std::time::Instant;

use fnv::FnvHashMap;

use log::error;

use tokio_postgres::NoTls;

use crate::config::PostgresMetricsConfig;
use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::{EventError, EventResult, MetricId, MetricName, TimePoint};

pub struct PostgresStatsCollector {
    client: tokio_postgres::Client,
    databases: FnvHashMap<String, DatabaseEntry>,
    last_measurement: TimePoint
}

impl PostgresStatsCollector {
    pub async fn new(config: &PostgresMetricsConfig, metric_definitions: &mut MetricDefinitions) -> EventResult<PostgresStatsCollector> {
        let (client, connection) = tokio_postgres::connect(&config.connection_string(), NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Postgres connection error: {}", e);
            }
        });

        let mut collector = PostgresStatsCollector {
            client,
            databases: FnvHashMap::default(),
            last_measurement: Instant::now()
        };

        let databases = config.databases.iter().map(|db| db).collect::<Vec<_>>();
        for (database_name, stats) in collector.get_stats(&databases).await? {
            collector.databases.insert(
                database_name.clone(),
                DatabaseEntry {
                    database_size_metric: metric_definitions.define(MetricName::sub("postgres.database.database_size", &database_name)),
                    database_size_rate_metric: metric_definitions.define(MetricName::sub("postgres.database.database_size.rate", &database_name)),

                    rows_returned_count_metric: metric_definitions.define(MetricName::sub("postgres.database.rows_returned.count", &database_name)),
                    rows_returned_rate_metric: metric_definitions.define(MetricName::sub("postgres.database.rows_returned.rate", &database_name)),

                    rows_fetched_count_metric: metric_definitions.define(MetricName::sub("postgres.database.rows_fetched.count", &database_name)),
                    rows_fetched_rate_metric: metric_definitions.define(MetricName::sub("postgres.database.rows_fetched.rate", &database_name)),

                    rows_inserted_count_metric: metric_definitions.define(MetricName::sub("postgres.database.rows_inserted.count", &database_name)),
                    rows_inserted_rate_metric: metric_definitions.define(MetricName::sub("postgres.database.rows_inserted.rate", &database_name)),

                    rows_updated_count_metric: metric_definitions.define(MetricName::sub("postgres.database.rows_updated.count", &database_name)),
                    rows_updated_rate_metric: metric_definitions.define(MetricName::sub("postgres.database.rows_updated.rate", &database_name)),

                    rows_deleted_count_metric: metric_definitions.define(MetricName::sub("postgres.database.rows_deleted.count", &database_name)),
                    rows_deleted_rate_metric: metric_definitions.define(MetricName::sub("postgres.database.rows_deleted.rate", &database_name)),

                    prev_stats: stats
                }
            );
        }

        Ok(collector)
    }

    pub async fn collect(&mut self, time: TimePoint, metrics: &mut MetricValues) -> EventResult<()> {
        let time_now = Instant::now();
        let elapsed = (time_now - self.last_measurement).as_secs_f64();
        let database_size_scale = 1.0 / (1024.0 * 1024.0 * 1024.0);

        let databases = self.databases.keys().collect::<Vec<_>>();
        for (database_name, stats) in self.get_stats(&databases).await? {
            if let Some(entry) = self.databases.get_mut(&database_name) {
                let diff_stats = stats.diff(&entry.prev_stats);

                metrics.insert(time, entry.database_size_metric, database_size_scale * stats.database_size as f64);
                metrics.insert(time, entry.database_size_rate_metric, database_size_scale * elapsed * diff_stats.database_size as f64);

                metrics.insert(time, entry.rows_returned_count_metric, stats.tup_returned as f64);
                metrics.insert(time, entry.rows_returned_rate_metric, elapsed * diff_stats.tup_returned as f64);

                metrics.insert(time, entry.rows_fetched_count_metric, stats.tup_fetched as f64);
                metrics.insert(time, entry.rows_fetched_rate_metric, elapsed * diff_stats.tup_fetched as f64);

                metrics.insert(time, entry.rows_inserted_count_metric, stats.tup_inserted as f64);
                metrics.insert(time, entry.rows_inserted_rate_metric, elapsed * diff_stats.tup_inserted as f64);

                metrics.insert(time, entry.rows_updated_count_metric, stats.tup_updated as f64);
                metrics.insert(time, entry.rows_updated_rate_metric, elapsed * diff_stats.tup_updated as f64);

                metrics.insert(time, entry.rows_deleted_count_metric, stats.tup_deleted as f64);
                metrics.insert(time, entry.rows_deleted_rate_metric, elapsed *  diff_stats.tup_deleted as f64);

                entry.prev_stats = stats;
            }
        }

        self.last_measurement = time_now;

        Ok(())
    }
    
    async fn get_stats(&self, databases: &Vec<&String>) -> EventResult<Vec<(String, DatabaseStats)>> {
        let mut entries = Vec::new();
        
        let rows = self.client.query(
            "SELECT \
            datname, (SELECT pg_database_size FROM pg_database_size(datname)) AS database_size, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted \
            FROM pg_stat_database \
            WHERE datname = any($1);",
            &[databases]
        ).await.map_err(|err| EventError::FailedToCollectPostgresMQMetric(err))?;

        for row in rows {
            if let Some(name) = row.get::<_, Option<&str>>(0) {
                entries.push(
                    (
                        name.to_owned(),
                        DatabaseStats {
                            database_size: row.get::<_, i64>(1),
                            tup_returned: row.get::<_, i64>(2),
                            tup_fetched: row.get::<_, i64>(3),
                            tup_inserted: row.get::<_, i64>(4),
                            tup_updated: row.get::<_, i64>(5),
                            tup_deleted: row.get::<_, i64>(6)
                        }
                    )
                );
            }
        }
        
        Ok(entries)
    }
}

struct DatabaseEntry {
    database_size_metric: MetricId,
    database_size_rate_metric: MetricId,

    rows_returned_count_metric: MetricId,
    rows_returned_rate_metric: MetricId,

    rows_fetched_count_metric: MetricId,
    rows_fetched_rate_metric: MetricId,

    rows_inserted_count_metric: MetricId,
    rows_inserted_rate_metric: MetricId,

    rows_updated_count_metric: MetricId,
    rows_updated_rate_metric: MetricId,

    rows_deleted_count_metric: MetricId,
    rows_deleted_rate_metric: MetricId,

    prev_stats: DatabaseStats
}

struct DatabaseStats {
    database_size: i64,
    tup_returned: i64,
    tup_fetched: i64,
    tup_inserted: i64,
    tup_updated: i64,
    tup_deleted: i64
}

impl DatabaseStats {
    pub fn diff(&self, other: &DatabaseStats) -> DatabaseStats {
        DatabaseStats {
            database_size: self.database_size - other.database_size,
            tup_returned: self.tup_returned - other.tup_returned,
            tup_fetched: self.tup_fetched - other.tup_fetched,
            tup_inserted: self.tup_inserted - other.tup_inserted,
            tup_updated: self.tup_updated - other.tup_updated,
            tup_deleted: self.tup_deleted - other.tup_deleted
        }
    }
}

#[tokio::test(flavor="multi_thread", worker_threads=1)]
async fn test_collect1() {
    use crate::model::TimeInterval;

    let mut config = PostgresMetricsConfig::default();
    config.hostname = "172.17.0.2".to_owned();
    config.password = "password123".to_owned();
    config.databases = vec!["sakila".to_owned()];

    let mut metric_definitions = MetricDefinitions::new();
    let mut postgres_stats_collector = PostgresStatsCollector::new(
        &config,
        &mut metric_definitions
    ).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs_f64(0.5)).await;

    let mut metrics = MetricValues::new(TimeInterval::Minutes(1.0));
    postgres_stats_collector.collect(TimePoint::now(), &mut metrics).await.unwrap();

    for (metric, value) in metrics.iter() {
        println!("{}: {}", metric_definitions.get_specific_name(metric).unwrap(), value)
    }
}