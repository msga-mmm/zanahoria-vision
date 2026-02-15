use std::{
    collections::{HashMap, HashSet},
    fs,
    io::ErrorKind,
    os::unix::fs::FileTypeExt,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use clap::Parser;
use nvim_rs::{Handler, Neovim, Value, compat::tokio::Compat, create::tokio as nvim_create};
use parity_tokio_ipc::Connection as IpcConnection;
use serde::{Deserialize, Serialize};
use swayipc::{Connection, Event, EventType, Fallible, WindowChange};
use tokio::{io::WriteHalf, sync::mpsc, time::MissedTickBehavior};

const NVIM_NOTIFY_EVENT: &str = "external_file_changed";
const EVENT_SCHEMA_VERSION: u32 = 1;
const DEFAULT_OUTPUT_JSON_PATH: &str = "zanahoria_vision_events.json";
const EVENT_CHANNEL_CAPACITY: usize = 1024;
const NVIM_FILE_AUTOCMD_LUA: &str = r#"
local group = vim.api.nvim_create_augroup("zanahoria_vision_file_watch", { clear = true })
vim.api.nvim_create_autocmd("BufEnter", {
  group = group,
  callback = function()
    local file = vim.api.nvim_buf_get_name(0)
    if file ~= "" then
      vim.rpcnotify(0, "external_file_changed", file)
    end
  end
})
"#;

enum CommandMode {
    Collect { output_path: PathBuf },
    Metrics { input_path: PathBuf },
}

#[derive(Debug, Parser)]
#[command(name = "zanahoria-vision")]
#[command(about = "Collect sway/neovim activity events and compute usage metrics")]
struct CliArgs {
    #[arg(long, help = "Print app usage metrics and exit")]
    metrics: bool,
    #[arg(
        long,
        value_name = "file",
        help = "JSON file path. In collect mode this is output, in metrics mode this is input"
    )]
    path: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CollectedEvent {
    #[serde(default = "default_schema_version")]
    schema_version: u32,
    timestamp_ms: u128,
    source: String,
    app_name: Option<String>,
    window_title: Option<String>,
    browser_url: Option<String>,
    nvim_socket: Option<String>,
    file_path: Option<String>,
}

fn default_schema_version() -> u32 {
    EVENT_SCHEMA_VERSION
}

#[derive(Clone)]
struct NvimEventHandler {
    socket: String,
    event_tx: mpsc::Sender<CollectedEvent>,
}

#[async_trait]
impl Handler for NvimEventHandler {
    type Writer = Compat<WriteHalf<IpcConnection>>;

    async fn handle_notify(&self, name: String, args: Vec<Value>, _neovim: Neovim<Self::Writer>) {
        if name != NVIM_NOTIFY_EVENT {
            return;
        }

        if let Some(file) = args.first().and_then(Value::as_str) {
            println!("Neovim file ({}): {}", self.socket, file);
            let _ = self
                .event_tx
                .send(CollectedEvent {
                    schema_version: EVENT_SCHEMA_VERSION,
                    timestamp_ms: now_timestamp_ms(),
                    source: "nvim_buf_enter".to_string(),
                    app_name: Some("Neovim".to_string()),
                    window_title: None,
                    browser_url: None,
                    nvim_socket: Some(self.socket.clone()),
                    file_path: Some(file.to_string()),
                })
                .await;
        }
    }
}

fn run_sway_listener(event_tx: mpsc::Sender<CollectedEvent>) -> Fallible<()> {
    let connection = Connection::new()?;
    let event_stream = connection.subscribe([EventType::Window])?;

    for event in event_stream {
        if let Event::Window(e) = event? {
            if e.change == WindowChange::Focus {
                println!("Focused window: {:?}", e.container.name);
                let app_name = e.container.app_id.clone().or_else(|| {
                    e.container
                        .window_properties
                        .as_ref()
                        .and_then(|props| props.class.clone())
                });
                let window_title = e.container.name.clone();
                let browser_url = window_title.as_deref().and_then(extract_first_url);

                if event_tx
                    .blocking_send(CollectedEvent {
                        schema_version: EVENT_SCHEMA_VERSION,
                        timestamp_ms: now_timestamp_ms(),
                        source: "sway_window_focus".to_string(),
                        app_name,
                        window_title,
                        browser_url,
                        nvim_socket: None,
                        file_path: None,
                    })
                    .is_err()
                {
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}

fn discover_nvim_sockets() -> Vec<PathBuf> {
    let mut dirs = Vec::new();

    if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
        dirs.push(PathBuf::from(runtime_dir));
    }

    if let Ok(uid) = std::env::var("UID") {
        dirs.push(PathBuf::from(format!("/run/user/{uid}")));
    }

    dirs.push(PathBuf::from("/tmp"));

    let mut sockets = Vec::new();
    let mut seen_dirs = HashSet::new();

    for dir in dirs {
        if !seen_dirs.insert(dir.clone()) {
            continue;
        }

        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };

        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };

            if !name.starts_with("nvim.") || !name.ends_with(".0") {
                continue;
            }

            let Ok(file_type) = entry.file_type() else {
                continue;
            };

            if file_type.is_socket() {
                sockets.push(entry.path());
            }
        }
    }

    sockets.sort();
    sockets.dedup();
    sockets
}

async fn monitor_single_nvim_socket(
    path: PathBuf,
    event_tx: mpsc::Sender<CollectedEvent>,
) -> Result<(), String> {
    let socket = path.display().to_string();
    let handler = NvimEventHandler {
        socket: socket.clone(),
        event_tx,
    };

    let (nvim, io_handler) = nvim_create::new_path(&path, handler).await.map_err(|err| {
        if err.kind() == ErrorKind::ConnectionRefused || err.kind() == ErrorKind::NotFound {
            // Stale/vanished socket entries are expected when Neovim exits.
            String::new()
        } else {
            format!("Could not connect to Neovim socket {socket}: {err}")
        }
    })?;

    nvim.subscribe(NVIM_NOTIFY_EVENT)
        .await
        .map_err(|err| format!("Could not subscribe to {NVIM_NOTIFY_EVENT} on {socket}: {err}"))?;

    nvim.exec_lua(NVIM_FILE_AUTOCMD_LUA, vec![])
        .await
        .map_err(|err| format!("Could not install BufEnter autocmd on {socket}: {err}"))?;

    match io_handler.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(format!("Neovim IO loop error on {socket}: {err}")),
        Err(err) => Err(format!("Failed joining Neovim IO loop for {socket}: {err}")),
    }
}

async fn run_nvim_monitor(event_tx: mpsc::Sender<CollectedEvent>) {
    let mut active_sockets = HashSet::<PathBuf>::new();
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<PathBuf>();

    let mut interval = tokio::time::interval(Duration::from_secs(2));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                for socket in discover_nvim_sockets() {
                    if active_sockets.contains(&socket) {
                        continue;
                    }

                    active_sockets.insert(socket.clone());
                    let done_tx = done_tx.clone();
                    let event_tx = event_tx.clone();
                    tokio::spawn(async move {
                        if let Err(err) = monitor_single_nvim_socket(socket.clone(), event_tx).await {
                            if !err.is_empty() {
                                eprintln!("{err}");
                            }
                        }
                        let _ = done_tx.send(socket);
                    });
                }
            }
            Some(done_socket) = done_rx.recv() => {
                active_sockets.remove(&done_socket);
            }
        }
    }
}

fn now_timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn extract_first_url(text: &str) -> Option<String> {
    for token in text.split_whitespace() {
        if token.starts_with("https://") || token.starts_with("http://") {
            let cleaned = token.trim_matches(|c: char| r#"'"()[]{}<>,;"#.contains(c));
            return Some(cleaned.to_string());
        }
    }
    None
}

fn output_json_path() -> PathBuf {
    std::env::var("ZANAHORIA_VISION_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_OUTPUT_JSON_PATH))
}

fn parse_command_mode() -> CommandMode {
    let args = CliArgs::parse();
    let path = args.path.unwrap_or_else(output_json_path);
    if args.metrics {
        CommandMode::Metrics { input_path: path }
    } else {
        CommandMode::Collect { output_path: path }
    }
}

fn format_duration_ms(ms: u128) -> String {
    let total_seconds = ms / 1000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    format!("{hours}h {minutes}m {seconds}s")
}

fn is_neovim_terminal_focus(app_name: &str, window_title: Option<&str>) -> bool {
    if app_name != "foot" {
        return false;
    }

    let Some(window_title) = window_title else {
        return false;
    };

    window_title
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .any(|token| token.eq_ignore_ascii_case("nvim") || token.eq_ignore_ascii_case("neovim"))
}

fn foot_activity_label(window_title: Option<&str>) -> String {
    let Some(window_title) = window_title else {
        return "Unknown".to_string();
    };

    let last_token = window_title
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .next_back();

    match last_token {
        Some(token) if is_neovim_terminal_focus("foot", Some(token)) => "Neovim".to_string(),
        Some(token) => token.to_ascii_lowercase(),
        None => "Unknown".to_string(),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct AppUsage {
    total_ms: u128,
    children: HashMap<String, u128>,
}

fn compute_usage(events: Vec<CollectedEvent>) -> Vec<(String, AppUsage)> {
    let mut focus_events = Vec::<(u128, String, Option<String>)>::new();
    for event in events {
        if event.source == "sway_window_focus" {
            if let Some(app_name) = event.app_name {
                focus_events.push((event.timestamp_ms, app_name, event.window_title));
            }
        }
    }

    if focus_events.len() < 2 {
        return Vec::new();
    }

    focus_events.sort_by_key(|(timestamp_ms, _, _)| *timestamp_ms);

    let mut totals = HashMap::<String, AppUsage>::new();
    for i in 0..(focus_events.len() - 1) {
        let (start_ts, app_name, window_title) =
            (&focus_events[i].0, &focus_events[i].1, &focus_events[i].2);
        let end_ts = focus_events[i + 1].0;
        let delta = end_ts.saturating_sub(*start_ts);

        let app_usage = totals.entry(app_name.clone()).or_default();
        app_usage.total_ms += delta;

        if app_name == "foot" {
            let activity = foot_activity_label(window_title.as_deref());
            *app_usage.children.entry(activity).or_insert(0) += delta;
        }
    }

    let mut by_usage = totals.into_iter().collect::<Vec<_>>();
    by_usage.sort_by(|(app_a, usage_a), (app_b, usage_b)| {
        usage_b
            .total_ms
            .cmp(&usage_a.total_ms)
            .then_with(|| app_a.cmp(app_b))
    });

    by_usage
}

fn print_metrics(path: &std::path::Path) -> Result<(), String> {
    let events = load_events_for_metrics(path)?;
    let by_usage = compute_usage(events);
    if by_usage.is_empty() {
        println!("Not enough sway focus events to compute app usage yet.");
        return Ok(());
    }

    let mut app_width = "App".len();
    for (app_name, usage) in &by_usage {
        app_width = app_width.max(app_name.len());
        for child_name in usage.children.keys() {
            app_width = app_width.max(format!("  |- {child_name}").len());
        }
    }

    println!("{:<app_width$}  Time", "App", app_width = app_width);
    println!(
        "{:<app_width$}  ----",
        "-".repeat(app_width),
        app_width = app_width
    );
    for (app_name, usage) in by_usage {
        println!(
            "{:<app_width$}  {}",
            app_name,
            format_duration_ms(usage.total_ms),
            app_width = app_width
        );

        if usage.children.is_empty() {
            continue;
        }

        let mut child_rows = usage.children.into_iter().collect::<Vec<_>>();
        child_rows.sort_by(|(name_a, ms_a), (name_b, ms_b)| {
            ms_b.cmp(ms_a).then_with(|| name_a.cmp(name_b))
        });
        let row_count = child_rows.len();
        for (idx, (child_name, child_ms)) in child_rows.into_iter().enumerate() {
            let branch = if idx + 1 == row_count {
                "  `- "
            } else {
                "  |- "
            };

            println!(
                "{:<app_width$}  {}",
                format!("{branch}{child_name}"),
                format_duration_ms(child_ms),
                app_width = app_width
            );
        }
    }

    Ok(())
}

fn ensure_parent_dir(path: &std::path::Path) -> Result<(), String> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "Could not create output directory {}: {err}",
                parent.display()
            )
        })?;
    }

    Ok(())
}

fn parse_jsonl_events(
    content: &str,
    path: &std::path::Path,
) -> Result<Vec<CollectedEvent>, String> {
    let mut parsed = Vec::new();
    for (idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let event = serde_json::from_str::<CollectedEvent>(line).map_err(|err| {
            format!(
                "Invalid JSONL event at line {} in {}: {err}",
                idx + 1,
                path.display()
            )
        })?;
        parsed.push(event);
    }
    Ok(parsed)
}

fn load_events_for_metrics(path: &std::path::Path) -> Result<Vec<CollectedEvent>, String> {
    let content = fs::read_to_string(path)
        .map_err(|err| format!("Could not read metrics file {}: {err}", path.display()))?;

    if content.trim().is_empty() {
        return Ok(Vec::new());
    }

    parse_jsonl_events(&content, path)
}

fn append_event_jsonl(path: &std::path::Path, event: &CollectedEvent) -> Result<(), String> {
    use std::io::Write;

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| format!("Could not open events output {}: {err}", path.display()))?;

    serde_json::to_writer(&mut file, event)
        .map_err(|err| format!("Could not serialize event for {}: {err}", path.display()))?;
    file.write_all(b"\n")
        .map_err(|err| format!("Could not append newline to {}: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn event(
        timestamp_ms: u128,
        source: &str,
        app_name: Option<&str>,
        window_title: Option<&str>,
    ) -> CollectedEvent {
        CollectedEvent {
            schema_version: EVENT_SCHEMA_VERSION,
            timestamp_ms,
            source: source.to_string(),
            app_name: app_name.map(str::to_string),
            window_title: window_title.map(str::to_string),
            browser_url: None,
            nvim_socket: None,
            file_path: None,
        }
    }

    fn temp_file_path(suffix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "zanahoria-vision-test-{}-{}-{suffix}",
            std::process::id(),
            now_timestamp_ms()
        ))
    }

    #[test]
    fn foot_label_detects_neovim_and_unknown() {
        assert_eq!(foot_activity_label(None), "Unknown");
        assert_eq!(foot_activity_label(Some("workspace nvim")), "Neovim");
        assert_eq!(foot_activity_label(Some("workspace NeOvIm")), "Neovim");
        assert_eq!(foot_activity_label(Some("workspace zsh")), "zsh");
    }

    #[test]
    fn compute_usage_aggregates_and_sorts() {
        let events = vec![
            event(5000, "sway_window_focus", Some("foot"), Some("proj zsh")),
            event(1000, "sway_window_focus", Some("foot"), Some("proj nvim")),
            event(
                3000,
                "sway_window_focus",
                Some("firefox"),
                Some("example.com"),
            ),
            event(9000, "sway_window_focus", Some("code"), Some("src/main.rs")),
            event(7000, "nvim_buf_enter", Some("Neovim"), Some("src/main.rs")),
        ];

        let by_usage = compute_usage(events);
        assert_eq!(by_usage.len(), 2);

        assert_eq!(by_usage[0].0, "foot");
        assert_eq!(by_usage[0].1.total_ms, 6000);
        assert_eq!(by_usage[0].1.children.get("Neovim"), Some(&2000));
        assert_eq!(by_usage[0].1.children.get("zsh"), Some(&4000));

        assert_eq!(by_usage[1].0, "firefox");
        assert_eq!(by_usage[1].1.total_ms, 2000);
        assert!(by_usage[1].1.children.is_empty());
    }

    #[test]
    fn load_events_rejects_legacy_and_supports_jsonl() {
        let legacy_path = temp_file_path("legacy.json");
        let jsonl_path = temp_file_path("events.jsonl");

        let legacy_content = serde_json::to_string_pretty(&serde_json::json!({
            "schema_version": 1,
            "updated_at_ms": 123,
            "events": [
                {
                    "schema_version": 1,
                    "timestamp_ms": 1000,
                    "source": "sway_window_focus",
                    "app_name": "foot",
                    "window_title": "nvim",
                    "browser_url": null,
                    "nvim_socket": null,
                    "file_path": null
                },
                {
                    "schema_version": 1,
                    "timestamp_ms": 3000,
                    "source": "sway_window_focus",
                    "app_name": "firefox",
                    "window_title": "https://example.com",
                    "browser_url": "https://example.com",
                    "nvim_socket": null,
                    "file_path": null
                }
            ]
        }))
        .expect("serialize legacy json");
        fs::write(&legacy_path, legacy_content).expect("write legacy test file");

        let jsonl_events = vec![
            serde_json::json!({
                "schema_version": 1,
                "timestamp_ms": 1000,
                "source": "sway_window_focus",
                "app_name": "foot",
                "window_title": "nvim",
                "browser_url": null,
                "nvim_socket": null,
                "file_path": null
            }),
            serde_json::json!({
                "schema_version": 1,
                "timestamp_ms": 3000,
                "source": "sway_window_focus",
                "app_name": "firefox",
                "window_title": "https://example.com",
                "browser_url": "https://example.com",
                "nvim_socket": null,
                "file_path": null
            }),
        ];
        let mut jsonl_content = jsonl_events
            .into_iter()
            .map(|event| serde_json::to_string(&event).expect("serialize jsonl event"))
            .collect::<Vec<_>>()
            .join("\n");
        jsonl_content.push('\n');
        fs::write(&jsonl_path, jsonl_content).expect("write jsonl test file");

        let legacy_error = load_events_for_metrics(&legacy_path).expect_err("reject legacy");
        let jsonl_events = load_events_for_metrics(&jsonl_path).expect("load jsonl");

        assert!(
            legacy_error.contains("Invalid JSONL event at line 1"),
            "unexpected error: {legacy_error}"
        );
        assert_eq!(jsonl_events.len(), 2);
        assert_eq!(jsonl_events[1].app_name.as_deref(), Some("firefox"));

        let _ = fs::remove_file(legacy_path);
        let _ = fs::remove_file(jsonl_path);
    }
}

async fn run_json_writer(
    mut event_rx: mpsc::Receiver<CollectedEvent>,
    output_path: PathBuf,
) -> Result<(), String> {
    ensure_parent_dir(&output_path)?;

    while let Some(event) = event_rx.recv().await {
        append_event_jsonl(&output_path, &event)?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), String> {
    match parse_command_mode() {
        CommandMode::Metrics { input_path } => {
            print_metrics(&input_path)?;
            return Ok(());
        }
        CommandMode::Collect { output_path } => {
            let (event_tx, event_rx) = mpsc::channel::<CollectedEvent>(EVENT_CHANNEL_CAPACITY);
            println!("Writing collected events to {}", output_path.display());

            let json_writer_task = tokio::spawn(run_json_writer(event_rx, output_path));

            let sway_tx = event_tx.clone();
            let sway_task = tokio::task::spawn_blocking(move || run_sway_listener(sway_tx));
            let nvim_task = tokio::spawn(run_nvim_monitor(event_tx));

            tokio::select! {
                sway_result = sway_task => {
                    match sway_result {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(err)) => Err(format!("Sway listener exited with error: {err}")),
                        Err(err) => Err(format!("Sway listener task join error: {err}")),
                    }
                }
                nvim_result = nvim_task => {
                    match nvim_result {
                        Ok(()) => Err("Neovim monitor exited unexpectedly".to_string()),
                        Err(err) => Err(format!("Neovim monitor task join error: {err}")),
                    }
                }
                writer_result = json_writer_task => {
                    match writer_result {
                        Ok(Ok(())) => Err("JSON writer exited unexpectedly".to_string()),
                        Ok(Err(err)) => Err(err),
                        Err(err) => Err(format!("JSON writer task join error: {err}")),
                    }
                }
            }
        }
    }
}
