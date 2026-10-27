//! Skua bridge (PoC).
//!
//! Exposes the movie's ExternalInterface over a plain TCP socket so an external
//! controller (VibeSkua on a PC, via `RemoteFlashUtil`) can drive skua.swf.
//!
//! Wire protocol: one JSON object per line (UTF-8, '\n' terminated).
//!
//!   controller -> phone : {"id": 7, "fn": "getGameObject", "args": ["world.myAvatar"]}
//!   phone -> controller : {"id": 7, "r": "<return value>"}
//!   phone -> controller : {"ev": "pext", "args": [["{...json packet...}"]]}
//!   phone -> controller : {"ev": "__hello", "args": [1]}   (sent on connect)
//!
//! Calls from the controller are queued onto the Ruffle event loop (the Player is
//! not thread-safe) and answered asynchronously, so several calls can be in flight.
//! Only one controller is "live" at a time; a new connection replaces the old one.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::iter::FromIterator;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Mutex, OnceLock};
use std::thread;

use ruffle_core::context::UpdateContext;
use ruffle_core::external::{ExternalInterfaceProvider, Value};
use serde_json::{Map, Number, Value as Json};

use crate::custom_event::RuffleEvent;
use crate::EventSender;

pub const PORT: u16 = 5150;

/// Events that are replayed to every newly connected controller, so that a
/// controller started after the movie can still drive the login flow.
const STICKY_EVENTS: &[&str] = &["requestLoadGame"];

struct Shared {
    /// Writer channel of the live controller connection, tagged with its id.
    out: Mutex<Option<(u64, mpsc::Sender<String>)>>,
    sticky: Mutex<BTreeMap<String, String>>,
}

fn shared() -> &'static Shared {
    static SHARED: OnceLock<Shared> = OnceLock::new();
    SHARED.get_or_init(|| Shared {
        out: Mutex::new(None),
        sticky: Mutex::new(BTreeMap::new()),
    })
}

static NEXT_CONN: AtomicU64 = AtomicU64::new(1);

/// The ExternalInterface provider installed on the Player.
/// Every `ExternalInterface.call(...)` from ActionScript ends up here.
pub struct SkuaExternalInterface;

impl ExternalInterfaceProvider for SkuaExternalInterface {
    fn call_method(&self, _context: &mut UpdateContext<'_>, name: &str, args: &[Value]) -> Value {
        emit(name, args);
        // The desktop ActiveX host never returns anything for these either.
        Value::Undefined
    }

    fn on_callback_available(&self, _name: &str) {}

    fn get_id(&self) -> Option<String> {
        None
    }
}

/// Forward an ActionScript -> host call to the controller (non-blocking).
fn emit(name: &str, args: &[Value]) {
    let line = Json::Object(Map::from_iter([
        ("ev".to_string(), Json::String(name.to_string())),
        (
            "args".to_string(),
            Json::Array(args.iter().map(to_json).collect()),
        ),
    ]))
    .to_string();

    if STICKY_EVENTS.contains(&name) {
        shared()
            .sticky
            .lock()
            .unwrap()
            .insert(name.to_string(), line.clone());
    }

    if let Some((_, tx)) = shared().out.lock().unwrap().as_ref() {
        let _ = tx.send(line);
    }
}

/// Build the response line for a finished call. Called from the event loop.
pub fn response(id: u64, value: &Value) -> String {
    Json::Object(Map::from_iter([
        ("id".to_string(), Json::Number(id.into())),
        ("r".to_string(), to_json(value)),
    ]))
    .to_string()
}

/// Start the TCP listener. Safe to call more than once; only the first call binds.
pub fn start(events: EventSender) {
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.set(()).is_err() {
        return;
    }

    thread::Builder::new()
        .name("skua-bridge".into())
        .spawn(move || {
            let listener = match TcpListener::bind(("0.0.0.0", PORT)) {
                Ok(l) => l,
                Err(e) => {
                    log::error!("[skua-bridge] cannot bind port {PORT}: {e}");
                    return;
                }
            };
            log::info!("[skua-bridge] listening on 0.0.0.0:{PORT}");
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let events = events.clone();
                        let _ = thread::Builder::new()
                            .name("skua-bridge-conn".into())
                            .spawn(move || handle_connection(stream, events));
                    }
                    Err(e) => log::warn!("[skua-bridge] accept failed: {e}"),
                }
            }
        })
        .expect("spawn skua-bridge thread");
}

fn handle_connection(stream: TcpStream, events: EventSender) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "?".into());
    log::info!("[skua-bridge] controller connected: {peer}");
    let _ = stream.set_nodelay(true);

    let conn_id = NEXT_CONN.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = mpsc::channel::<String>();

    // Writer thread: everything going to the controller passes through `tx`.
    let mut write_half = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            log::error!("[skua-bridge] try_clone failed: {e}");
            return;
        }
    };
    let writer = thread::Builder::new()
        .name("skua-bridge-write".into())
        .spawn(move || {
            for mut line in rx {
                line.push('\n');
                if write_half.write_all(line.as_bytes()).is_err() {
                    break;
                }
            }
            let _ = write_half.shutdown(std::net::Shutdown::Both);
        });

    let _ = tx.send(r#"{"ev":"__hello","args":[1]}"#.to_string());
    for line in shared().sticky.lock().unwrap().values() {
        let _ = tx.send(line.clone());
    }
    // Become the live controller (drops the previous one's sender).
    *shared().out.lock().unwrap() = Some((conn_id, tx.clone()));

    let reader = BufReader::new(stream);
    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        let msg: Json = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(e) => {
                log::warn!("[skua-bridge] bad json: {e}");
                continue;
            }
        };
        let id = msg.get("id").and_then(Json::as_u64).unwrap_or(0);
        let name = msg
            .get("fn")
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_string();
        let args: Vec<Value> = msg
            .get("args")
            .and_then(Json::as_array)
            .map(|a| a.iter().map(from_json).collect())
            .unwrap_or_default();

        if name == "__ping" {
            let _ = tx.send(response(id, &Value::String("pong".into())));
            continue;
        }

        events.send(RuffleEvent::ExternalCall {
            id,
            name,
            args,
            reply: tx.clone(),
        });
    }

    log::info!("[skua-bridge] controller disconnected: {peer}");
    {
        let mut out = shared().out.lock().unwrap();
        if matches!(out.as_ref(), Some((cid, _)) if *cid == conn_id) {
            *out = None;
        }
    }
    drop(tx);
    if let Ok(w) = writer {
        let _ = w.join();
    }
}

fn to_json(value: &Value) -> Json {
    match value {
        Value::Undefined | Value::Null => Json::Null,
        Value::Bool(b) => Json::Bool(*b),
        Value::Number(n) => {
            if n.is_finite() && n.fract() == 0.0 && n.abs() < 9.0e15 {
                Json::Number((*n as i64).into())
            } else {
                Number::from_f64(*n)
                    .map(Json::Number)
                    .unwrap_or_else(|| Json::String(n.to_string()))
            }
        }
        Value::String(s) => Json::String(s.clone()),
        Value::Object(map) => {
            Json::Object(map.iter().map(|(k, v)| (k.clone(), to_json(v))).collect())
        }
        Value::List(list) => Json::Array(list.iter().map(to_json).collect()),
    }
}

fn from_json(json: &Json) -> Value {
    match json {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Bool(*b),
        Json::Number(n) => Value::Number(n.as_f64().unwrap_or(0.0)),
        Json::String(s) => Value::String(s.clone()),
        Json::Array(a) => Value::List(a.iter().map(from_json).collect()),
        Json::Object(o) => {
            Value::Object(o.iter().map(|(k, v)| (k.clone(), from_json(v))).collect())
        }
    }
}
