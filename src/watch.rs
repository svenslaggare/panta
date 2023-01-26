use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicBool;

use fnv::{FnvHashMap, FnvHashSet};

use structopt::StructOpt;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedSender};
use tokio::time::Instant;

use crate::metrics::{MetricDefinitions, MetricValues};
use crate::model::MetricName;


pub struct SendWatches {
    has_watchers: Arc<AtomicBool>,
    watch_sender: UnboundedSender<FnvHashMap<MetricName, f64>>,
    last_watch: Instant
}

impl SendWatches {
    pub async fn new(watch_socket_path: &Path) -> Result<SendWatches, Box<dyn Error>> {
        #[allow(unused)] {
            std::fs::remove_file(watch_socket_path);
        }

        let watch_listener = UnixListener::bind(watch_socket_path).unwrap();

        let (watch_sender, mut watch_receiver) = mpsc::unbounded_channel::<FnvHashMap<MetricName, f64>>();
        let has_watchers = Arc::new(AtomicBool::new(false));
        let has_watchers_clone = has_watchers.clone();

        tokio::spawn(async move {
            let mut watchers = Vec::new();
            loop {
                tokio::select! {
                    accept_result = watch_listener.accept() => {
                        if let Ok((stream, _)) = accept_result {
                            watchers.push(stream);
                            has_watchers_clone.store(true, Ordering::SeqCst);
                        }
                    }
                    Some(serialized_values) = watch_receiver.recv() => {
                        let serialized_bytes = bincode::serialize(&serialized_values).unwrap();

                        let mut removed = FnvHashSet::default();
                        for (index, stream) in watchers.iter_mut().enumerate() {
                            if send_watch_bytes(stream, &serialized_bytes).await.is_err() {
                                removed.insert(index);
                            }
                        }

                        let mut index = 0;
                        watchers.retain(|_| {
                            let keep = !removed.contains(&index);
                            index += 1;
                            keep
                        });

                        has_watchers_clone.store(!watchers.is_empty(), Ordering::SeqCst);
                    }
                }
            }
        });

        Ok(
            SendWatches {
                has_watchers,
                watch_sender,
                last_watch: Instant::now()
            }
        )
    }

    pub async fn try_send(&mut self, metric_definitions: &MetricDefinitions, values: &MetricValues) {
        if self.has_watchers.load(Ordering::SeqCst) && (Instant::now() - self.last_watch).as_secs_f64() > 1.0 {
            let serialized_values = values.serialize(&metric_definitions);
            self.watch_sender.send(serialized_values).unwrap();
            self.last_watch = Instant::now();
        }
    }
}

async fn send_watch_bytes(stream: &mut UnixStream, serialized_bytes: &Vec<u8>) -> Result<(), Box<dyn Error>> {
    stream.write_u32(serialized_bytes.len() as u32).await?;
    stream.write_all(&serialized_bytes).await?;
    Ok(())
}

#[derive(Debug, StructOpt)]
pub struct WatchCommand {
    /// The watch socket path
    #[structopt(default_value="watch_panta.sock")]
    pub watch_socket_path: String,
    /// Only show metrics that matches this regex
    #[structopt(long)]
    pub filter: Option<String>
}

pub async fn consume_watch(config: WatchCommand) -> Result<(), Box<dyn Error>> {
    let filter = if let Some(filter) = config.filter {
        Some(regex::Regex::new(&filter)?)
    } else {
        None
    };

    let mut stream = UnixStream::connect(config.watch_socket_path).await?;
    let mut bytes = Vec::new();
    loop {
        let to_read = stream.read_u32().await? as usize;
        bytes.resize(to_read, 0);
        stream.read_exact(&mut bytes[..to_read]).await?;

        let values: FnvHashMap<MetricName, f64> = bincode::deserialize(&bytes[..to_read])?;

        let mut values_group = BTreeMap::default();
        for (name, value) in values {
            if let Some(filter) = filter.as_ref() {
                if !filter.is_match(&name.to_string()) {
                    continue;
                }
            }

            if name.is_specific() {
                let name_all = name.as_all();
                values_group.entry(name_all)
                    .or_insert_with(|| Vec::new())
                    .push((name.sub.unwrap(), value));
            } else {
                values_group.insert(name, vec![(String::new(), value)]);
            }
        }

        for (metric, sub_metrics) in values_group {
            if sub_metrics.len() == 1 && sub_metrics[0].0.is_empty() {
                println!("{}: {}", metric, sub_metrics[0].1);
            } else {
                println!("{}", metric);
                for (sub, value) in sub_metrics {
                    println!("\t{}: {}", sub, value);
                }
            }
        }

        println!("========================================================================");
    }
}