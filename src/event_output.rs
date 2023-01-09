use log::info;

use crate::event::EventId;
use crate::model::{EventResult, Value};

pub trait EventOutputHandler {
    fn handle_output(&mut self,
                     event_id: &EventId,
                     values: &Vec<(String, Value)>) -> EventResult<()>;
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
                         values: &Vec<(String, Value)>) -> EventResult<()> {
        for handler in &mut self.handlers {
            handler.handle_output(event_id, values)?;
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
    fn handle_output(&mut self, event_id: &EventId, values: &Vec<(String, Value)>) -> EventResult<()> {
        let mut output_string = String::new();
        let mut is_first = true;
        for (name, value) in values {
            if !is_first {
                output_string += ", ";
            } else {
                is_first = false;
            }

            output_string += &name;
            output_string += "=";
            output_string += &value.to_string();
        }

        info!("Event generated for #{}, {}", event_id, output_string);
        Ok(())
    }
}