//! Custom event type for Ruffle on Android

use ruffle_core::events::KeyDescriptor;
use ruffle_core::external::Value as ExternalValue;
use std::sync::mpsc::Sender;

use crate::PlayerRunnable;

/// User-defined events.
pub enum RuffleEvent {
    /// Indicates that a task is ready to be polled.
    TaskPoll(PlayerRunnable),
    VirtualKeyEvent {
        down: bool,
        key_descriptor: KeyDescriptor,
    },
    RunContextMenuCallback(usize),
    ClearContextMenu,
    RequestContextMenu,
    /// Skua bridge: call an ExternalInterface callback registered by the movie.
    /// The JSON response line is sent through `reply`.
    ExternalCall {
        id: u64,
        name: String,
        args: Vec<ExternalValue>,
        reply: Sender<String>,
    },
}
