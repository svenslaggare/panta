use log::info;

use crate::event::EventId;
use crate::model::{EventResult, Value};

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
        let output_string = join_event_output(outputs);
        info!("Event generated for {} (id: {}), {}", name, event_id, output_string);
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