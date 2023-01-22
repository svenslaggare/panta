use std::path::{Path, PathBuf};
use serde::Deserialize;

use crate::model::{EventError, EventResult};

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub log_level: log::LevelFilter,
    pub custom_metrics: CustomMetricsConfig,
    pub rabbitmq_metrics: RabbitMQMetricsConfig,
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
            rabbitmq_metrics: RabbitMQMetricsConfig::default(),
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