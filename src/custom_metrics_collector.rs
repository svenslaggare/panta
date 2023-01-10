use std::error::Error;
use std::num::ParseFloatError;
use std::path::Path;
use std::str::FromStr;
use log::{debug, error};

use tokio::net::UnixDatagram;
use tokio::sync::mpsc::UnboundedSender;

pub struct CustomMetricsCollector {
    uds_receiver: UnixDatagram,
    result_sender: UnboundedSender<CustomMetric>
}

impl CustomMetricsCollector {
    pub fn new(result_sender: UnboundedSender<CustomMetric>) -> Result<CustomMetricsCollector, Box<dyn Error>> {
        let socket_name = Path::new("panta.sock");

        #[allow(unused)] {
            std::fs::remove_file(socket_name);
        }

        let uds_receiver = UnixDatagram::bind(socket_name)?;

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
                        debug!("{:?}", custom_metric);
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

#[derive(Debug)]
pub enum CustomMetric {
    Gauge { name: String, value: f64 }
}

#[derive(Debug)]
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

        enum State {
            Name,
            Value,
            Type
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
                    metric_type.push(current);
                }
            }
        }

        match metric_type.as_str() {
            "g" => {
                Ok(
                    CustomMetric::Gauge {
                        name,
                        value: f64::from_str(&value).map_err(|err| CustomMetricParseError::ParseFloatError(err))?
                    }
                )
            }
            _ => {
                Err(CustomMetricParseError::InvalidType(metric_type))
            }
        }
    }
}