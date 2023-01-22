use std::error::Error;
use std::num::ParseFloatError;
use std::str::FromStr;
use log::{debug, error};

use tokio::net::UnixDatagram;
use tokio::sync::mpsc::UnboundedSender;

use crate::config::CustomMetricsConfig;

pub struct CustomMetricsCollector {
    uds_receiver: UnixDatagram,
    result_sender: UnboundedSender<CustomMetric>
}

impl CustomMetricsCollector {
    pub fn new(config: &CustomMetricsConfig,
               result_sender: UnboundedSender<CustomMetric>) -> Result<CustomMetricsCollector, Box<dyn Error>> {
        #[allow(unused)] {
            std::fs::remove_file(&config.socket_path);
        }

        let uds_receiver = UnixDatagram::bind(&config.socket_path)?;

        Ok(
            CustomMetricsCollector {
                uds_receiver,
                result_sender
            }
        )
    }

    pub async fn collect(&self) -> Result<(), Box<dyn Error>> {
        let mut buffer = vec![0u8; 4096];
        loop {
            let amount = self.uds_receiver.recv(&mut buffer).await?;
            let data = &buffer[..amount];
            if let Ok(str_data) = std::str::from_utf8(data) {
                match CustomMetric::from_str(str_data) {
                    Ok(custom_metric) => {
                        debug!("Received custom metric: {:?}", custom_metric);
                        if self.result_sender.send(custom_metric).is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        error!("Failed to parse custom metric: {:?}", err);
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub enum CustomMetric {
    Gauge { name: String, value: f64, sub: Option<String> }
}

#[derive(Debug, PartialEq)]
pub enum CustomMetricParseError {
    InvalidType(String),
    ParseFloatError(ParseFloatError)
}

impl FromStr for CustomMetric {
    type Err = CustomMetricParseError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut name = String::new();
        let mut value = String::new();
        let mut metric_type = String::new();
        let mut sub = String::new();
        let mut has_sub = false;

        enum State {
            Name,
            Value,
            Type,
            Sub
        }

        let mut state = State::Name;
        for current in text.chars() {
            match state {
                State::Name => {
                    if current == ':' {
                        state = State::Value;
                    } else {
                        name.push(current);
                    }
                }
                State::Value => {
                    if current == '|' {
                        state = State::Type;
                    } else {
                        value.push(current);
                    }
                }
                State::Type => {
                    if current == '#' {
                        state = State::Sub;
                        has_sub = true;
                    } else {
                        metric_type.push(current);
                    }
                }
                State::Sub => {
                    sub.push(current);
                }
            }
        }

        match metric_type.as_str() {
            "g" => {
                Ok(
                    CustomMetric::Gauge {
                        name,
                        value: f64::from_str(&value).map_err(|err| CustomMetricParseError::ParseFloatError(err))?,
                        sub: if has_sub {Some(sub)} else {None}
                    }
                )
            }
            _ => {
                Err(CustomMetricParseError::InvalidType(metric_type))
            }
        }
    }
}

#[test]
fn test_parse1() {
    assert_eq!(
        Ok(CustomMetric::Gauge { name: "custom.test".to_string(), value: 44.0, sub: None }),
        CustomMetric::from_str("custom.test:44.0|g")
    );
}

#[test]
fn test_parse2() {
    assert_eq!(
        Ok(CustomMetric::Gauge { name: "custom.test".to_string(), value: 44.0, sub: Some("sub".to_owned()) }),
        CustomMetric::from_str("custom.test:44.0|g#sub")
    );
}