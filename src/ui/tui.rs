use std::collections::VecDeque;
use std::io::{self, Stdout};
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures_util::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Paragraph};
use tokio::sync::broadcast::error::RecvError;
use tokio::time;

use super::{PhaseKind, ProbeKind, UiEvent, UiEventRx};
use crate::sampler::Sample;

/// 240 samples = 24 s of timeline at the engine's 100 ms sampling cadence.
const SAMPLE_BUFFER_CAP: usize = 240;
/// ~30 fps: short enough to feel live, slow enough to be cheap.
const RENDER_INTERVAL: Duration = Duration::from_millis(33);

/// Run the live TUI until the event channel closes, the user quits, or the
/// crossterm input stream ends. The terminal is restored on every exit path.
pub async fn run(mut rx: UiEventRx) -> Result<()> {
    let mut terminal = enter()?;
    let result = run_loop(&mut terminal, &mut rx).await;
    leave(&mut terminal);
    result
}

fn enter() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let terminal = Terminal::new(CrosstermBackend::new(stdout))?;
    Ok(terminal)
}

fn leave(terminal: &mut Terminal<CrosstermBackend<Stdout>>) {
    let _ = disable_raw_mode();
    let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen);
    let _ = terminal.show_cursor();
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    rx: &mut UiEventRx,
) -> Result<()> {
    let mut input = EventStream::new();
    let mut tick = time::interval(RENDER_INTERVAL);
    tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut state = State::default();

    loop {
        tokio::select! {
            biased;
            ev = rx.recv() => match ev {
                Ok(ev) => state.apply(ev),
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(_)) => {}
            },
            key = input.next() => match key {
                Some(Ok(Event::Key(k))) if is_quit(k) => break,
                Some(Ok(_) | Err(_)) => {}
                None => break,
            },
            _ = tick.tick() => {
                terminal.draw(|f| draw(f, &state))?;
            }
        }
    }
    Ok(())
}

fn is_quit(k: KeyEvent) -> bool {
    matches!(k.code, KeyCode::Char('q') | KeyCode::Esc)
        || (matches!(k.code, KeyCode::Char('c')) && k.modifiers.contains(KeyModifiers::CONTROL))
}

#[derive(Default)]
struct State {
    samples: VecDeque<Sample>,
    phase: Option<PhaseKind>,
    last_idle_rtt_ms: Option<f64>,
    last_loaded_rtt_ms: Option<f64>,
    last_error: Option<String>,
}

impl State {
    fn push_sample(&mut self, s: Sample) {
        if self.samples.len() == SAMPLE_BUFFER_CAP {
            self.samples.pop_front();
        }
        self.samples.push_back(s);
    }

    fn apply(&mut self, ev: UiEvent) {
        match ev {
            UiEvent::Throughput(s) => self.push_sample(s),
            UiEvent::PhaseStarted { kind, .. } => {
                self.phase = Some(kind);
                self.samples.clear();
            }
            UiEvent::LatencyProbe { kind, rtt_us } => {
                let ms = rtt_us as f64 / 1_000.0;
                match kind {
                    ProbeKind::Idle => self.last_idle_rtt_ms = Some(ms),
                    ProbeKind::LoadedDownload | ProbeKind::LoadedUpload => {
                        self.last_loaded_rtt_ms = Some(ms);
                    }
                }
            }
            UiEvent::PhaseFinished(_) | UiEvent::SessionFinished(_) => {}
            UiEvent::Error(e) => self.last_error = Some(e),
        }
    }

    fn current_mbps(&self) -> f64 {
        self.samples.back().map(|s| s.mbps).unwrap_or(0.0)
    }

    fn phase_label(&self) -> &'static str {
        match self.phase {
            Some(PhaseKind::Download) => "download",
            Some(PhaseKind::Upload) => "upload",
            None => "idle",
        }
    }
}

fn draw(f: &mut ratatui::Frame, state: &State) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(3), Constraint::Length(3)])
        .split(f.area());

    let header = Paragraph::new(format!(
        " {phase}    {mbps:>7.2} Mbps    idle {idle}    loaded {loaded} ",
        phase = state.phase_label(),
        mbps = state.current_mbps(),
        idle = fmt_ms(state.last_idle_rtt_ms),
        loaded = fmt_ms(state.last_loaded_rtt_ms),
    ))
    .style(Style::default().add_modifier(Modifier::BOLD))
    .block(Block::default().borders(Borders::ALL).title("speedt"));
    f.render_widget(header, chunks[0]);

    let body_text = if let Some(err) = &state.last_error {
        format!("error: {err}")
    } else {
        format!(
            "{} sample(s) buffered ({}s timeline)",
            state.samples.len(),
            state.samples.len() / 10
        )
    };
    let body = Paragraph::new(body_text).block(Block::default().borders(Borders::ALL).title("timeline"));
    f.render_widget(body, chunks[1]);

    let footer = Paragraph::new(" q / Esc / Ctrl-C  quit ")
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(footer, chunks[2]);
}

fn fmt_ms(v: Option<f64>) -> String {
    v.map_or_else(|| "-".to_string(), |ms| format!("{ms:>5.1} ms"))
}
