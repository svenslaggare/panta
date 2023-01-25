use std::path::{Path, PathBuf};
use serde::Deserialize;

use crate::model::{EventError, EventResult};

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub log_level: log::LevelFilter,
    pub custom_metrics: CustomMetricsConfig,
    pub docker: Option<DockerMetricsConfig>,
    pub rabbitmq: Option<RabbitMQMetricsConfig>,
    pub postgres: Option<PostgresMetricsConfig>,
    pub rediscover_rate: f64
}

impl Config {
    pub fn load_from_file(path: &Path) -> EventResult<Config> {
        let content = std::fs::read_to_string(path).map_err(|err| EventError::FailedToLoad(err.to_string()))?;
        serde_yaml::from_str(&content).map_err(|err| EventError::FailedToLoad(err.to_string()))
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            log_level: log::LevelFilter::Info,
            custom_metrics: CustomMetricsConfig::default(),
            docker: Some(DockerMetricsConfig::default()),
            rabbitmq: Some(RabbitMQMetricsConfig::default()),
            postgres: None,
            rediscover_rate: 0.1
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct CustomMetricsConfig {
    pub socket_path: PathBuf
}

impl Default for CustomMetricsConfig {
    fn default() -> Self {
        CustomMetricsConfig {
            socket_path: Path::new("panta.sock").to_owned()
        }
    }
}


#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct DockerMetricsConfig {

}

impl Default for DockerMetricsConfig {
    fn default() -> Self {
        DockerMetricsConfig {

        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct RabbitMQMetricsConfig {
    pub base_url: String,
    pub username: String,
    pub password: String
}

impl Default for RabbitMQMetricsConfig {
    fn default() -> Self {
        RabbitMQMetricsConfig {
            base_url: "http://localhost:15672".to_string(),
            username: "guest".to_string(),
            password: "guest".to_string()
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct PostgresMetricsConfig {
    pub hostname: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub databases: Vec<String>
}

impl PostgresMetricsConfig {
    pub fn connection_string(&self) -> String {
        format!("host={} port={} user={} password={}", self.hostname, self.port, self.username, self.password)
    }
}

impl Default for PostgresMetricsConfig {
    fn default() -> Self {
        PostgresMetricsConfig {
            hostname: "localhost".to_string(),
            port: 5432,
            username: "postgres".to_string(),
            password: "".to_string(),
            databases: Vec::new()
        }
    }
}