use std::io;

use bytes::Bytes;
use crossterm::{
    event::{Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::{SinkExt, StreamExt};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
    Frame, Terminal,
};
use rustkvstore::protocol::{
    decode_response, encode_command, Command, KvCodec, Response,
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tui_input::backend::crossterm::EventHandler;
use tui_input::Input;

#[derive(Clone)]
enum HistoryEntry {
    Command(String),
    Response(String, ResponseKind),
}

#[derive(Clone)]
enum ResponseKind {
    Ok,
    Pong,
    Value,
    Nil,
    Error,
    Info,
}

struct App {
    input: Input,
    history: Vec<HistoryEntry>,
    scroll_offset: usize,
    connection: Option<Framed<TcpStream, KvCodec>>,
    addr: String,
    should_quit: bool,
}

impl App {
    fn new(addr: String) -> Self {
        Self {
            input: Input::default(),
            history: Vec::new(),
            scroll_offset: 0,
            connection: None,
            addr,
            should_quit: false,
        }
    }

    fn is_connected(&self) -> bool {
        self.connection.is_some()
    }

    fn push_command(&mut self, text: &str) {
        self.history
            .push(HistoryEntry::Command(text.to_string()));
        self.scroll_to_bottom();
    }

    fn push_response(&mut self, text: String, kind: ResponseKind) {
        self.history.push(HistoryEntry::Response(text, kind));
        self.scroll_to_bottom();
    }

    fn scroll_to_bottom(&mut self) {
        // Will be clamped during render
        self.scroll_offset = usize::MAX;
    }

    fn history_height(&self) -> usize {
        self.history.len()
    }

    async fn try_connect(&mut self) {
        match TcpStream::connect(&self.addr).await {
            Ok(stream) => {
                self.connection = Some(Framed::new(stream, KvCodec));
                self.push_response(
                    format!("Connected to {}", self.addr),
                    ResponseKind::Info,
                );
            }
            Err(e) => {
                self.connection = None;
                self.push_response(
                    format!("Connection failed: {e}"),
                    ResponseKind::Error,
                );
            }
        }
    }

    async fn send_command(&mut self, cmd: Command) {
        let encoded = match encode_command(&cmd) {
            Ok(b) => b,
            Err(e) => {
                self.push_response(format!("Encode error: {e}"), ResponseKind::Error);
                return;
            }
        };

        if let Some(ref mut conn) = self.connection {
            if let Err(e) = conn.send(encoded).await {
                self.push_response(format!("Send error: {e}"), ResponseKind::Error);
                self.connection = None;
            }
        } else {
            self.push_response(
                "Not connected. Use 'connect' to reconnect.".into(),
                ResponseKind::Error,
            );
        }
    }

    fn handle_response_frame(&mut self, frame: Bytes) {
        match decode_response(&frame) {
            Ok(resp) => match resp {
                Response::Ok => {
                    self.push_response("OK".into(), ResponseKind::Ok);
                }
                Response::Pong => {
                    self.push_response("PONG".into(), ResponseKind::Pong);
                }
                Response::Value(Some(v)) => {
                    let text = match String::from_utf8(v) {
                        Ok(s) => format!("\"{s}\""),
                        Err(e) => format!("<binary {} bytes>", e.into_bytes().len()),
                    };
                    self.push_response(text, ResponseKind::Value);
                }
                Response::Value(None) => {
                    self.push_response("(nil)".into(), ResponseKind::Nil);
                }
                Response::Error(e) => {
                    self.push_response(format!("ERR {e}"), ResponseKind::Error);
                }
            },
            Err(e) => {
                self.push_response(
                    format!("Decode error: {e}"),
                    ResponseKind::Error,
                );
            }
        }
    }

    async fn process_input(&mut self) {
        let raw = self.input.value().trim().to_string();
        if raw.is_empty() {
            return;
        }
        self.input.reset();
        self.push_command(&raw);

        let parts: Vec<&str> = raw.splitn(3, ' ').collect();
        let cmd_name = parts[0].to_lowercase();

        match cmd_name.as_str() {
            "quit" | "exit" => {
                self.should_quit = true;
            }
            "help" => {
                self.push_response(
                    "Commands: get <key>, set <key> <value>, del <key>, ping, connect [addr], help, quit".into(),
                    ResponseKind::Info,
                );
            }
            "connect" => {
                if parts.len() > 1 {
                    self.addr = parts[1].to_string();
                }
                self.connection = None;
                self.try_connect().await;
            }
            "ping" => {
                self.send_command(Command::Ping).await;
            }
            "get" => {
                if parts.len() < 2 {
                    self.push_response(
                        "Usage: get <key>".into(),
                        ResponseKind::Error,
                    );
                } else {
                    self.send_command(Command::Get {
                        key: parts[1].to_string(),
                    })
                    .await;
                }
            }
            "set" => {
                if parts.len() < 3 {
                    self.push_response(
                        "Usage: set <key> <value>".into(),
                        ResponseKind::Error,
                    );
                } else {
                    self.send_command(Command::Set {
                        key: parts[1].to_string(),
                        value: parts[2].as_bytes().to_vec(),
                    })
                    .await;
                }
            }
            "del" | "delete" => {
                if parts.len() < 2 {
                    self.push_response(
                        "Usage: del <key>".into(),
                        ResponseKind::Error,
                    );
                } else {
                    self.send_command(Command::Delete {
                        key: parts[1].to_string(),
                    })
                    .await;
                }
            }
            _ => {
                self.push_response(
                    format!("Unknown command: '{}'. Type 'help' for usage.", cmd_name),
                    ResponseKind::Error,
                );
            }
        }
    }
}

fn ui(f: &mut Frame, app: &mut App) {
    let chunks = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(3),
    ])
    .split(f.area());

    // -- Top bar --
    let status = if app.is_connected() {
        Span::styled(
            format!(" Connected: {} ", app.addr),
            Style::default().fg(Color::Green),
        )
    } else {
        Span::styled(
            " Disconnected ",
            Style::default().fg(Color::Red),
        )
    };
    let title_bar = Line::from(vec![
        Span::styled(
            " rustkvstore ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("─".repeat(
            chunks[0]
                .width
                .saturating_sub(14 + status.width() as u16) as usize,
        )),
        status,
    ]);
    f.render_widget(Paragraph::new(title_bar), chunks[0]);

    // -- History --
    let history_area = chunks[1];
    let visible_height = history_area.height.saturating_sub(2) as usize; // account for borders
    let total = app.history_height();

    // Clamp scroll offset
    if total > visible_height {
        app.scroll_offset = app.scroll_offset.min(total - visible_height);
    } else {
        app.scroll_offset = 0;
    }

    let lines: Vec<Line> = app
        .history
        .iter()
        .skip(app.scroll_offset)
        .take(visible_height)
        .map(|entry| match entry {
            HistoryEntry::Command(text) => Line::from(vec![
                Span::styled("> ", Style::default().fg(Color::Cyan)),
                Span::styled(text.clone(), Style::default().fg(Color::Cyan)),
            ]),
            HistoryEntry::Response(text, kind) => {
                let style = match kind {
                    ResponseKind::Ok => Style::default().fg(Color::Green),
                    ResponseKind::Pong => Style::default().fg(Color::Yellow),
                    ResponseKind::Value => Style::default().fg(Color::White),
                    ResponseKind::Nil => Style::default().fg(Color::DarkGray),
                    ResponseKind::Error => Style::default().fg(Color::Red),
                    ResponseKind::Info => Style::default().fg(Color::Blue),
                };
                Line::from(Span::styled(text.clone(), style))
            }
        })
        .collect();

    let history_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
    let history_widget = Paragraph::new(lines).block(history_block);
    f.render_widget(history_widget, history_area);

    // Scrollbar
    if total > visible_height {
        let mut scrollbar_state = ScrollbarState::new(total.saturating_sub(visible_height))
            .position(app.scroll_offset);
        f.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None),
            history_area,
            &mut scrollbar_state,
        );
    }

    // -- Input --
    let input_area = chunks[2];
    let input_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let input_width = input_area.width.saturating_sub(4) as usize; // borders + "> "
    let scroll = app.input.visual_scroll(input_width);
    let input_line = Line::from(vec![
        Span::styled("> ", Style::default().fg(Color::Green).bold()),
        Span::raw(&app.input.value()[scroll..]),
    ]);
    let input_widget = Paragraph::new(input_line).block(input_block);
    f.render_widget(input_widget, input_area);

    // Place cursor
    let cursor_pos = app.input.visual_cursor().saturating_sub(scroll);
    f.set_cursor_position((
        input_area.x + 3 + cursor_pos as u16, // border + "> "
        input_area.y + 1,                      // border
    ));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6379".to_string());

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(addr);
    app.try_connect().await;

    let mut event_reader = crossterm::event::EventStream::new();
    let mut tick_interval = tokio::time::interval(tokio::time::Duration::from_millis(250));

    // Main loop
    loop {
        terminal.draw(|f| ui(f, &mut app))?;

        if app.should_quit {
            break;
        }

        tokio::select! {
            event = event_reader.next() => {
                match event {
                    Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => {
                        match key.code {
                            KeyCode::Enter => {
                                app.process_input().await;
                            }
                            KeyCode::Esc => {
                                app.should_quit = true;
                            }
                            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                                app.should_quit = true;
                            }
                            KeyCode::Up => {
                                app.scroll_offset = app.scroll_offset.saturating_sub(1);
                            }
                            KeyCode::Down => {
                                app.scroll_offset = app.scroll_offset.saturating_add(1);
                            }
                            KeyCode::PageUp => {
                                app.scroll_offset = app.scroll_offset.saturating_sub(10);
                            }
                            KeyCode::PageDown => {
                                app.scroll_offset = app.scroll_offset.saturating_add(10);
                            }
                            _ => {
                                app.input.handle_event(&Event::Key(key));
                            }
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) => {
                        app.should_quit = true;
                    }
                    None => {
                        app.should_quit = true;
                    }
                }
            }
            frame = async {
                if let Some(ref mut conn) = app.connection {
                    conn.next().await
                } else {
                    // Never resolve when disconnected
                    std::future::pending().await
                }
            } => {
                match frame {
                    Some(Ok(bytes)) => {
                        app.handle_response_frame(bytes);
                    }
                    Some(Err(e)) => {
                        app.push_response(
                            format!("Connection error: {e}"),
                            ResponseKind::Error,
                        );
                        app.connection = None;
                    }
                    None => {
                        app.push_response(
                            "Server closed connection.".into(),
                            ResponseKind::Error,
                        );
                        app.connection = None;
                    }
                }
            }
            _ = tick_interval.tick() => {
                // Just triggers a redraw for cursor blink
            }
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}
