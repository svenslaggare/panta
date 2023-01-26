use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use fnv::FnvHashMap;

use log::info;

use serde::{Serialize, Deserialize};

use crate::event::EventId;
use crate::model::{EventError, EventResult, Value};

#[derive(Debug, Deserialize)]
pub struct EventOutputDefinition {
    #[serde(rename="type")]
    output_type: EventOutputType,
    path: Option<PathBuf>
}

impl EventOutputDefinition {
    pub fn create(&self) -> Result<BoxEventOutputHandler, String> {
        match self.output_type {
            EventOutputType::Console => {
                Ok(Box::new(ConsoleEventOutputHandler::new()))
            }
            EventOutputType::TextFile => {
                let path = self.path.as_ref().ok_or_else(|| "Expected 'path' attribute.")?;
                Ok(Box::new(TextFileEventOutputHandler::new(&path).map_err(|err| format!("{:?}", err))?))
            }
            EventOutputType::JsonFile => {
                let path = self.path.as_ref().ok_or_else(|| "Expected 'path' attribute.")?;
                Ok(Box::new(JsonFileEventOutputHandler::new(&path).map_err(|err| format!("{:?}", err))?))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub enum EventOutputType {
    #[serde(rename="console")]
    Console,
    #[serde(rename="text_file")]
    TextFile,
    #[serde(rename="json_file")]
    JsonFile
}

pub trait EventOutputHandler {
    fn handle_output(
        &mut self,
        event_id: &EventId,
        name: &str,
        outputs: &Vec<(String, Value)>
    ) -> EventResult<()>;
}

pub type BoxEventOutputHandler = Box<dyn EventOutputHandler>;

pub struct EventOutputHandlers {
    handlers: Vec<BoxEventOutputHandler>
}

impl EventOutputHandlers {
    pub fn new() -> EventOutputHandlers {
        EventOutputHandlers {
            handlers: Vec::new()
        }
    }

    pub fn add_handler(&mut self, handler: BoxEventOutputHandler) {
        self.handlers.push(handler);
    }

    pub fn handle_output(&mut self,
                         event_id: &EventId,
                         name: &str,
                         outputs: &Vec<(String, Value)>) -> EventResult<()> {
        for handler in &mut self.handlers {
            handler.handle_output(event_id, name, outputs)?;
        }

        Ok(())
    }
}

pub struct ConsoleEventOutputHandler {

}

impl ConsoleEventOutputHandler {
    pub fn new() -> ConsoleEventOutputHandler {
        ConsoleEventOutputHandler {

        }
    }
}

impl EventOutputHandler for ConsoleEventOutputHandler {
    fn handle_output(&mut self, event_id: &EventId, name: &str, outputs: &Vec<(String, Value)>) -> EventResult<()> {
        info!("Event generated for {} (id: {}), {}", name, event_id, join_event_output(outputs));
        Ok(())
    }
}

pub fn join_event_output(outputs: &Vec<(String, Value)>) -> String {
    let mut output_string = String::new();
    let mut is_first = true;
    for (name, value) in outputs {
        if !is_first {
            output_string += ", ";
        } else {
            is_first = false;
        }

        output_string += &name;
        output_string += "=";
        output_string += &value.to_string();
    }

    output_string
}

pub struct TextFileEventOutputHandler {
    file: File
}

impl TextFileEventOutputHandler {
    pub fn new(path: &Path) -> EventResult<TextFileEventOutputHandler> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|err| EventError::FailedToCreateFile(err))?;

        Ok(
            TextFileEventOutputHandler {
                file
            }
        )
    }
}

impl EventOutputHandler for TextFileEventOutputHandler {
    fn handle_output(&mut self, event_id: &EventId, name: &str, outputs: &Vec<(String, Value)>) -> EventResult<()> {
        let line = format!(
            "{} Event generated for {} (id: {}), {}\n",
            chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S.%f]"),
            name,
            event_id,
            join_event_output(outputs)
        );

        let mut line = line.into_bytes();
        self.file.write_all(&mut line).map_err(|err| EventError::FailedToWriteFile(err))?;
        self.file.flush().map_err(|err| EventError::FailedToWriteFile(err))?;
        Ok(())
    }
}

pub struct JsonFileEventOutputHandler {
    file: File
}

impl JsonFileEventOutputHandler {
    pub fn new(path: &Path) -> EventResult<JsonFileEventOutputHandler> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|err| EventError::FailedToCreateFile(err))?;

        Ok(
            JsonFileEventOutputHandler {
                file
            }
        )
    }
}

impl EventOutputHandler for JsonFileEventOutputHandler {
    fn handle_output(&mut self, event_id: &EventId, name: &str, outputs: &Vec<(String, Value)>) -> EventResult<()> {
        let entry = JsonFileEntry {
            time: chrono::Local::now().timestamp_micros() as f64 / 1.0E6,
            name: name.to_owned(),
            id: event_id.0,
            values: FnvHashMap::from_iter(outputs.iter().map(|(name, value)| (name.to_owned(), value.convert_float())))
        };

        let mut entry_json = serde_json::to_string(&entry).unwrap();
        entry_json.push('\n');
        let mut entry_json = entry_json.into_bytes();

        self.file.write_all(&mut entry_json).map_err(|err| EventError::FailedToWriteFile(err))?;
        self.file.flush().map_err(|err| EventError::FailedToWriteFile(err))?;
        Ok(())
    }
}

#[derive(Serialize)]
struct JsonFileEntry {
    time: f64,
    name: String,
    id: u64,
    values: FnvHashMap<String, f64>
}