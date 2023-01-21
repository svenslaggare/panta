use std::path::{Path, PathBuf};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub custom_metrics: CustomMetricsConfig,
    pub rabbitmq_metrics: RabbitMQMetricsConfig
}

impl Default for Config {
    fn default() -> Self {
        Config {
            custom_metrics: CustomMetricsConfig {
                socket_path: Path::new("panta.sock").to_owned()
            },
            rabbitmq_metrics: RabbitMQMetricsConfig::default()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CustomMetricsConfig {
    pub socket_path: PathBuf
}

#[derive(Debug, Deserialize)]
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