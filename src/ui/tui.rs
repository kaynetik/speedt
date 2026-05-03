use std::collections::VecDeque;
use std::io::{self, Stdout};
use std::path::PathBuf;
use std::sync::Once;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures_util::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Sparkline, Table};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::watch;
use tokio::time;

use super::{PhaseKind, ProbeKind, UiEvent, UiEventRx};
use crate::metadata::Metadata;
use crate::report::{
    self, LatencyReport, PhaseReport, SessionReport, format_summary_card, summary_card,
};
use crate::sampler::Sample;
use crate::stats::{BufferbloatGrade, LatencySummary};

/// 240 samples = 24 s of timeline at the engine's 100 ms sampling cadence.
const SAMPLE_BUFFER_CAP: usize = 240;
/// ~30 fps: short enough to feel live, slow enough to be cheap.
const RENDER_INTERVAL: Duration = Duration::from_millis(33);
/// How long the "saved to …" footer flash stays visible after pressing `s`.
const SAVE_FLASH: Duration = Duration::from_secs(4);

/// Install a process-wide panic hook that restores the terminal (cooked mode,
/// main screen, visible cursor) before delegating to the previously-installed
/// hook. Idempotent via `Once`, so calling it again — including from tests
/// that re-enter the TUI — does not chain the same restore step repeatedly.
pub fn install_panic_hook() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            restore_terminal();
            prev(info);
        }));
    });
}

/// Best-effort terminal restoration. Used by the panic hook and exposed for
/// the rare case where a caller has clobbered terminal state and wants to
/// recover without panicking. Errors are swallowed: there is nothing useful
/// to do with them at this point and the caller usually cannot recover.
fn restore_terminal() {
    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen);
}

/// Run the live TUI until the event channel closes, the user quits, the
/// crossterm input stream ends, or `SIGINT` arrives. The terminal is restored
/// on every exit path; a panic anywhere underneath is also caught by the
/// hook installed in [`install_panic_hook`].
///
/// The TUI owns the `cancel_tx` side of a watch channel shared with the
/// engine session: pressing `q` flips it to `true`, prompting download /
/// upload / latency tasks to wind down promptly so the partial
/// `SessionReport` can still be printed.
pub async fn run(mut rx: UiEventRx, cancel_tx: watch::Sender<bool>) -> Result<()> {
    install_panic_hook();
    let mut terminal = enter()?;
    let result = run_loop(&mut terminal, &mut rx, &cancel_tx).await;
    leave(&mut terminal);
    // After leaving the alt-screen, mirror the final report (if any) to
    // stdout so the data lands in the user's scrollback, matching the plain
    // renderer.
    if let Ok(Some(final_report)) = &result {
        report::print_human(final_report);
    }
    result.map(|_| ())
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
    cancel_tx: &watch::Sender<bool>,
) -> Result<Option<Box<SessionReport>>> {
    let mut input = EventStream::new();
    let mut tick = time::interval(RENDER_INTERVAL);
    tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut state = State::default();

    loop {
        // Once the session has finished, the engine will close the bus
        // shortly after sending `SessionFinished`. Stop polling the bus then
        // so the close doesn't tear down the results view — the user is
        // reading it.
        if state.final_report.is_some() {
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => break,
                key = input.next() => match key {
                    Some(Ok(Event::Key(k))) => match key_action(k) {
                        Some(KeyAction::Quit) => break,
                        Some(KeyAction::Save) => {
                            let result = save_report(&state);
                            state.last_save = Some(SaveStatus { at: Instant::now(), result });
                        }
                        None => {}
                    },
                    Some(Ok(_) | Err(_)) => {}
                    None => break,
                },
                _ = tick.tick() => {
                    terminal.draw(|f| draw(f, &state))?;
                }
            }
        } else {
            tokio::select! {
                biased;
                // SIGINT mirrors `q`: signal cancel and break out so `leave()`
                // restores the terminal. Process-level exit on SIGINT is also
                // handled in `main` for the case where the TUI never started.
                _ = tokio::signal::ctrl_c() => {
                    let _ = cancel_tx.send(true);
                    break;
                }
                ev = rx.recv() => match ev {
                    Ok(ev) => state.apply(ev),
                    // Bus closed mid-session — engine aborted before sending
                    // `SessionFinished`. Nothing meaningful left to render.
                    Err(RecvError::Closed) => break,
                    Err(RecvError::Lagged(_)) => {}
                },
                key = input.next() => match key {
                    Some(Ok(Event::Key(k))) => match key_action(k) {
                        Some(KeyAction::Quit) => {
                            // Signal cancel and exit the loop. Restoring the
                            // terminal here lets the engine's partial-report
                            // printout land on a sane terminal afterwards.
                            let _ = cancel_tx.send(true);
                            break;
                        }
                        Some(KeyAction::Save) => {
                            let result = save_report(&state);
                            state.last_save = Some(SaveStatus { at: Instant::now(), result });
                        }
                        None => {}
                    },
                    Some(Ok(_) | Err(_)) => {}
                    None => break,
                },
                _ = tick.tick() => {
                    terminal.draw(|f| draw(f, &state))?;
                }
            }
        }
    }
    Ok(state.final_report)
}

#[derive(Debug, Clone, Copy)]
enum KeyAction {
    Quit,
    Save,
}

fn key_action(k: KeyEvent) -> Option<KeyAction> {
    match k.code {
        KeyCode::Char('q') => Some(KeyAction::Quit),
        KeyCode::Char('s') => Some(KeyAction::Save),
        _ => None,
    }
}

#[derive(Default)]
struct State {
    mode: Option<&'static str>,
    metadata: Option<Box<Metadata>>,
    session_started_at: Option<Instant>,
    /// Wall-clock start time, captured from `UiEvent::SessionStarted`. Used
    /// by `s` (save report) so the saved JSON matches the `--json` shape.
    session_started_at_utc: Option<chrono::DateTime<chrono::Utc>>,
    total_planned_secs: f64,

    active: Option<ActivePhase>,
    finished: Vec<PhaseReport>,

    idle_rtts_us: Vec<u64>,
    loaded_dl_rtts_us: Vec<u64>,
    loaded_ul_rtts_us: Vec<u64>,

    last_error: Option<String>,
    last_save: Option<SaveStatus>,

    /// Final report received via `SessionFinished`. Its presence flips the
    /// TUI into "results view" mode and is what we mirror to scrollback on
    /// quit.
    final_report: Option<Box<SessionReport>>,
    /// Frozen elapsed time at the moment `SessionFinished` arrived, so the
    /// header clock stops advancing once the session is done.
    final_elapsed_secs: Option<f64>,
}

struct SaveStatus {
    at: Instant,
    result: std::result::Result<PathBuf, String>,
}

struct ActivePhase {
    kind: PhaseKind,
    label: &'static str,
    started_at: Instant,
    planned_secs: f64,
    samples: VecDeque<Sample>,
}

impl ActivePhase {
    fn push_sample(&mut self, s: Sample) {
        if self.samples.len() == SAMPLE_BUFFER_CAP {
            self.samples.pop_front();
        }
        self.samples.push_back(s);
    }

    fn elapsed_secs(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    fn current_mbps(&self) -> f64 {
        self.samples.back().map(|s| s.mbps).unwrap_or(0.0)
    }
}

impl State {
    fn apply(&mut self, ev: UiEvent) {
        match ev {
            UiEvent::SessionStarted { mode, total_planned_secs, metadata, started_at } => {
                self.mode = Some(mode);
                self.metadata = Some(metadata);
                self.session_started_at = Some(Instant::now());
                self.session_started_at_utc = Some(started_at);
                self.total_planned_secs = total_planned_secs;
            }
            UiEvent::PhaseStarted { kind, label, planned_secs } => {
                self.active = Some(ActivePhase {
                    kind,
                    label,
                    started_at: Instant::now(),
                    planned_secs,
                    samples: VecDeque::new(),
                });
            }
            UiEvent::Throughput(s) => {
                if let Some(ph) = self.active.as_mut() {
                    ph.push_sample(s);
                }
            }
            UiEvent::LatencyProbe { kind, rtt_us } => match kind {
                ProbeKind::Idle => self.idle_rtts_us.push(rtt_us),
                ProbeKind::LoadedDownload => self.loaded_dl_rtts_us.push(rtt_us),
                ProbeKind::LoadedUpload => self.loaded_ul_rtts_us.push(rtt_us),
            },
            UiEvent::PhaseFinished(rep) => {
                self.finished.push(rep);
                self.active = None;
            }
            UiEvent::SessionFinished(rep) => {
                self.active = None;
                self.final_elapsed_secs = Some(self.session_elapsed_secs());
                self.final_report = Some(rep);
            }
            UiEvent::Error(e) => self.last_error = Some(e),
        }
    }

    fn session_elapsed_secs(&self) -> f64 {
        if let Some(secs) = self.final_elapsed_secs {
            return secs;
        }
        self.session_started_at
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0)
    }
}

fn draw(f: &mut ratatui::Frame, state: &State) {
    // Results view adds a one-line summary card under the latency table.
    let summary_height: u16 = if state.final_report.is_some() { 3 } else { 0 };

    let mut constraints: Vec<Constraint> = vec![
        Constraint::Length(4), // Header
        Constraint::Min(0),    // Phase area (elastic)
        Constraint::Length(5), // Latency
    ];
    if summary_height > 0 {
        constraints.push(Constraint::Length(summary_height));
    }
    constraints.push(Constraint::Length(3)); // Footer

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(f.area());

    draw_header(f, chunks[0], state);
    draw_phase_area(f, chunks[1], state);
    draw_latency(f, chunks[2], state);
    if summary_height > 0 {
        draw_summary_card(f, chunks[3], state);
        draw_footer(f, chunks[4], state);
    } else {
        draw_footer(f, chunks[3], state);
    }
}

fn draw_header(f: &mut ratatui::Frame, area: Rect, state: &State) {
    let mode = state.mode.unwrap_or("…");
    let elapsed = state.session_elapsed_secs();
    let total = state.total_planned_secs;
    let title_line = Line::from(vec![
        Span::styled("speedt", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw("  ·  "),
        Span::styled(mode, Style::default().fg(Color::Cyan)),
        Span::raw("  ·  "),
        Span::raw(format!("{} / {}", fmt_clock(elapsed), fmt_clock(total))),
    ]);
    let meta_line = Line::from(metadata_segments(state.metadata.as_deref()));

    let para = Paragraph::new(vec![title_line, meta_line])
        .block(Block::default().borders(Borders::ALL).title(" session "));
    f.render_widget(para, area);
}

fn metadata_segments(md: Option<&Metadata>) -> Vec<Span<'static>> {
    let Some(md) = md else {
        return vec![Span::styled(
            "(metadata pending…)",
            Style::default().fg(Color::DarkGray),
        )];
    };

    let mut parts: Vec<String> = Vec::new();
    if let Some(c) = md.colo.as_ref() {
        parts.push(format!("colo {c}"));
    }
    if let Some(asn) = md.asn {
        let org = md.as_organization.as_deref().unwrap_or("");
        if org.is_empty() {
            parts.push(format!("AS{asn}"));
        } else {
            parts.push(format!("AS{asn} {org}"));
        }
    }
    if let Some(http) = md.http_protocol.as_ref() {
        parts.push(http.clone());
    }
    if let Some(tls) = md.tls.as_ref() {
        parts.push(tls.clone());
    }
    if parts.is_empty() {
        return vec![Span::styled(
            "(metadata unavailable)",
            Style::default().fg(Color::DarkGray),
        )];
    }
    let mut spans: Vec<Span<'static>> = Vec::with_capacity(parts.len() * 2);
    for (i, p) in parts.into_iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled("  ·  ", Style::default().fg(Color::DarkGray)));
        }
        spans.push(Span::raw(p));
    }
    spans
}

fn draw_phase_area(f: &mut ratatui::Frame, area: Rect, state: &State) {
    // Results view: expand each finished phase into a full-detail card,
    // splitting the phase region equally between them.
    if state.final_report.is_some() && !state.finished.is_empty() {
        let n = state.finished.len() as u32;
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                state
                    .finished
                    .iter()
                    .map(|_| Constraint::Ratio(1, n))
                    .collect::<Vec<_>>(),
            )
            .split(area);
        for (i, rep) in state.finished.iter().enumerate() {
            draw_expanded_phase(f, rows[i], rep);
        }
        return;
    }

    // Live view: each finished phase collapses to a 4-line card (2 content +
    // borders); the active phase (if any) takes the remaining space.
    let mut constraints: Vec<Constraint> = state
        .finished
        .iter()
        .map(|_| Constraint::Length(4))
        .collect();
    if state.active.is_some() {
        constraints.push(Constraint::Min(7));
    } else if constraints.is_empty() {
        // Nothing to show yet — give the placeholder the whole region.
        constraints.push(Constraint::Min(0));
    } else {
        // No active phase, but we have finished cards — pad bottom.
        constraints.push(Constraint::Min(0));
    }

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);

    for (i, rep) in state.finished.iter().enumerate() {
        draw_finished_card(f, rows[i], rep);
    }

    let trailing = rows.last().copied().unwrap_or(area);
    if let Some(active) = &state.active {
        let idx = state.finished.len();
        if idx < rows.len() {
            draw_active_phase(f, rows[idx], active);
        }
    } else if state.finished.is_empty() {
        let para = Paragraph::new(if let Some(err) = &state.last_error {
            format!("error: {err}")
        } else {
            "waiting for first phase…".to_string()
        })
        .block(Block::default().borders(Borders::ALL).title(" phase "));
        f.render_widget(para, trailing);
    }
}

fn draw_finished_card(f: &mut ratatui::Frame, area: Rect, rep: &PhaseReport) {
    let title = format!(" {} ✓ ", rep.label);
    let line1 = format!(
        "mean {:.2} Mbps · stable {} · TTS {}",
        rep.mean_mbps,
        fmt_opt_mbps(rep.stable_mbps),
        fmt_opt_secs(rep.time_to_saturation_secs),
    );
    let line2 = format!(
        "p50 {:.2} · p95 {:.2} · {} · {:.1}s",
        rep.timeline_summary.p50,
        rep.timeline_summary.p95,
        fmt_bytes(rep.total_bytes),
        rep.duration_secs,
    );
    let para = Paragraph::new(vec![Line::from(line1), Line::from(line2)])
        .style(Style::default().fg(Color::DarkGray))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(Span::styled(
                    title,
                    Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
                )),
        );
    f.render_widget(para, area);
}

fn draw_active_phase(f: &mut ratatui::Frame, area: Rect, active: &ActivePhase) {
    let title = format!(" {} (live) ", active.label);
    let block = Block::default().borders(Borders::ALL).title(Span::styled(
        title,
        Style::default()
            .fg(match active.kind {
                PhaseKind::Download => Color::Cyan,
                PhaseKind::Upload => Color::Magenta,
            })
            .add_modifier(Modifier::BOLD),
    ));
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.height < 3 {
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Gauge
            Constraint::Min(1),    // Sparkline
            Constraint::Length(1), // Stats line
        ])
        .split(inner);

    let elapsed = active.elapsed_secs();
    let planned = active.planned_secs.max(0.001);
    let ratio = (elapsed / planned).clamp(0.0, 1.0);
    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(Color::Green).bg(Color::Black))
        .ratio(ratio)
        .label(format!(
            "{} / {}  ({:.0}%)",
            fmt_clock(elapsed),
            fmt_clock(planned),
            ratio * 100.0
        ));
    f.render_widget(gauge, chunks[0]);

    let spark_data: Vec<u64> = active
        .samples
        .iter()
        .map(|s| (s.mbps.max(0.0) * 100.0) as u64)
        .collect();
    let sparkline = Sparkline::default()
        .data(&spark_data)
        .style(Style::default().fg(match active.kind {
            PhaseKind::Download => Color::Cyan,
            PhaseKind::Upload => Color::Magenta,
        }));
    f.render_widget(sparkline, chunks[1]);

    let mbps: Vec<f64> = active.samples.iter().map(|s| s.mbps).collect();
    let p50 = percentile(&mbps, 50.0);
    let p95 = percentile(&mbps, 95.0);
    let stable = stable_tail_mean(&mbps);
    let stats = Line::from(vec![
        Span::raw("now "),
        Span::styled(
            format!("{:>7.2} Mbps", active.current_mbps()),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("   p50 "),
        Span::raw(format!("{p50:>6.2}")),
        Span::raw("   p95 "),
        Span::raw(format!("{p95:>6.2}")),
        Span::raw("   stable "),
        Span::raw(fmt_opt_mbps(stable)),
    ]);
    f.render_widget(Paragraph::new(stats), chunks[2]);
}

fn draw_expanded_phase(f: &mut ratatui::Frame, area: Rect, rep: &PhaseReport) {
    let title = format!(" {} \u{2713} ", rep.label);
    let s = &rep.timeline_summary;
    let lines = vec![
        Line::from(format!(
            "mean {:.2} Mbps   stable {}   TTS {}",
            rep.mean_mbps,
            fmt_opt_mbps(rep.stable_mbps),
            fmt_opt_secs(rep.time_to_saturation_secs),
        )),
        Line::from(format!(
            "p50 {:>7.2}   p75 {:>7.2}   p90 {:>7.2}   p95 {:>7.2}   p99 {:>7.2}",
            s.p50, s.p75, s.p90, s.p95, s.p99,
        )),
        Line::from(format!(
            "min {:>7.2}   max {:>7.2}   stdev {:>6.2}   bytes {}",
            s.min,
            s.max,
            s.stdev,
            fmt_bytes(rep.total_bytes),
        )),
        Line::from(format!(
            "duration {:.2} s   requests {}   errors {}",
            rep.duration_secs, rep.requests, rep.errors,
        )),
    ];
    let para = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(Span::styled(
                title,
                Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
            )),
    );
    f.render_widget(para, area);
}

fn draw_latency(f: &mut ratatui::Frame, area: Rect, state: &State) {
    let idle_p50 = u64_p50_ms(&state.idle_rtts_us);
    let loaded_dl_p50 = u64_p50_ms(&state.loaded_dl_rtts_us);
    let loaded_ul_p50 = u64_p50_ms(&state.loaded_ul_rtts_us);

    let bb_dl = bufferbloat(idle_p50, loaded_dl_p50);
    let bb_ul = bufferbloat(idle_p50, loaded_ul_p50);

    let rows = vec![
        Row::new(vec![
            Cell::from("idle"),
            Cell::from(fmt_count(state.idle_rtts_us.len())),
            Cell::from(fmt_opt_ms(idle_p50)),
            Cell::from(fmt_opt_ms(u64_pct_ms(&state.idle_rtts_us, 95.0))),
            Cell::from(""),
        ]),
        Row::new(vec![
            Cell::from("under download"),
            Cell::from(fmt_count(state.loaded_dl_rtts_us.len())),
            Cell::from(fmt_opt_ms(loaded_dl_p50)),
            Cell::from(fmt_opt_ms(u64_pct_ms(&state.loaded_dl_rtts_us, 95.0))),
            grade_cell(bb_dl),
        ]),
        Row::new(vec![
            Cell::from("under upload"),
            Cell::from(fmt_count(state.loaded_ul_rtts_us.len())),
            Cell::from(fmt_opt_ms(loaded_ul_p50)),
            Cell::from(fmt_opt_ms(u64_pct_ms(&state.loaded_ul_rtts_us, 95.0))),
            grade_cell(bb_ul),
        ]),
    ];

    let widths = [
        Constraint::Length(16),
        Constraint::Length(6),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Min(20),
    ];
    let table = Table::new(rows, widths)
        .header(
            Row::new(vec!["", "n", "p50", "p95", "bufferbloat"])
                .style(Style::default().fg(Color::DarkGray)),
        )
        .block(Block::default().borders(Borders::ALL).title(" latency "));
    f.render_widget(table, area);
}

fn grade_cell(bb: Option<BufferbloatGrade>) -> Cell<'static> {
    let Some(bb) = bb else {
        return Cell::from("");
    };
    let mut style = Style::default().fg(grade_color(bb.grade));
    if bb.grade == 'F' {
        style = style.add_modifier(Modifier::BOLD);
    }
    Cell::from(format!("+{:>5.1} ms  grade {}", bb.added_latency_ms, bb.grade)).style(style)
}

fn grade_color(grade: char) -> Color {
    match grade {
        'A' => Color::Green,
        'B' => Color::Yellow,
        // No DarkYellow in ratatui::Color — pick a recognizable dark gold.
        'C' => Color::Rgb(184, 134, 11),
        'D' | 'F' => Color::Red,
        _ => Color::Gray,
    }
}

fn bufferbloat(idle_p50: Option<f64>, loaded_p50: Option<f64>) -> Option<BufferbloatGrade> {
    let (idle, loaded) = (idle_p50?, loaded_p50?);
    if idle <= 0.0 {
        return None;
    }
    Some(BufferbloatGrade::from_added((loaded - idle).max(0.0)))
}

fn draw_summary_card(f: &mut ratatui::Frame, area: Rect, state: &State) {
    let Some(rep) = state.final_report.as_deref() else {
        return;
    };
    let card = summary_card(rep);
    let para = Paragraph::new(format_summary_card(&card))
        .style(Style::default().add_modifier(Modifier::BOLD))
        .block(Block::default().borders(Borders::ALL).title(" summary "));
    f.render_widget(para, area);
}

fn draw_footer(f: &mut ratatui::Frame, area: Rect, state: &State) {
    let in_results = state.final_report.is_some();
    let quit_suffix = if in_results {
        " quit (results will be written to the terminal)"
    } else {
        " quit"
    };
    let mut spans: Vec<Span<'static>> = Vec::new();
    if let Some(err) = &state.last_error {
        spans.push(Span::styled(
            "error: ",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(err.clone()));
        spans.push(Span::raw("    "));
    }
    spans.push(Span::styled("q", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)));
    spans.push(Span::styled(quit_suffix, Style::default().fg(Color::DarkGray)));
    if !in_results {
        spans.push(Span::styled("  ·  ", Style::default().fg(Color::DarkGray)));
        spans.push(Span::styled(
            "s",
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::styled(" save report", Style::default().fg(Color::DarkGray)));
    }
    if let Some(flash) = save_flash_span(state) {
        spans.push(Span::raw("    "));
        spans.push(flash);
    }
    let para = Paragraph::new(Line::from(spans)).block(Block::default().borders(Borders::ALL));
    f.render_widget(para, area);
}

fn save_flash_span(state: &State) -> Option<Span<'static>> {
    let s = state.last_save.as_ref()?;
    if s.at.elapsed() > SAVE_FLASH {
        return None;
    }
    Some(match &s.result {
        Ok(path) => Span::styled(
            format!("saved → {}", path.display()),
            Style::default().fg(Color::Green),
        ),
        Err(msg) => Span::styled(
            format!("save failed: {msg}"),
            Style::default().fg(Color::Red),
        ),
    })
}

// ---- save report ----------------------------------------------------------

/// Serialise a best-effort `SessionReport` from whatever the TUI has
/// observed so far and write it to `./speedt-<UTC-RFC3339>.json`. Partial
/// (in-flight) phases are intentionally absent rather than synthesised, so
/// the file matches the `--json` shape without lying about phase numbers.
fn save_report(state: &State) -> std::result::Result<PathBuf, String> {
    let rep = build_partial_report(state);
    let json = serde_json::to_string_pretty(&rep).map_err(|e| e.to_string())?;
    let stamp = chrono::Utc::now()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        .replace(':', "-");
    let path = PathBuf::from(format!("./speedt-{stamp}.json"));
    std::fs::write(&path, json).map_err(|e| e.to_string())?;
    Ok(path)
}

fn build_partial_report(state: &State) -> SessionReport {
    let download = state
        .finished
        .iter()
        .find(|p| p.label == "download")
        .cloned();
    let upload = state
        .finished
        .iter()
        .find(|p| p.label == "upload")
        .cloned();

    let idle = (!state.idle_rtts_us.is_empty())
        .then(|| LatencySummary::from_micros(&state.idle_rtts_us));
    let loaded_dl = (!state.loaded_dl_rtts_us.is_empty())
        .then(|| LatencySummary::from_micros(&state.loaded_dl_rtts_us));
    let loaded_ul = (!state.loaded_ul_rtts_us.is_empty())
        .then(|| LatencySummary::from_micros(&state.loaded_ul_rtts_us));

    let bb_dl = match (idle.as_ref(), loaded_dl.as_ref()) {
        (Some(i), Some(l)) if i.p50_ms > 0.0 => Some(BufferbloatGrade::from_added(
            (l.p50_ms - i.p50_ms).max(0.0),
        )),
        _ => None,
    };
    let bb_ul = match (idle.as_ref(), loaded_ul.as_ref()) {
        (Some(i), Some(l)) if i.p50_ms > 0.0 => Some(BufferbloatGrade::from_added(
            (l.p50_ms - i.p50_ms).max(0.0),
        )),
        _ => None,
    };

    SessionReport {
        mode: state.mode.unwrap_or("partial"),
        started_at: state
            .session_started_at_utc
            .unwrap_or_else(chrono::Utc::now),
        ended_at: chrono::Utc::now(),
        metadata: state
            .metadata
            .as_deref()
            .cloned()
            .unwrap_or_default(),
        latency: LatencyReport {
            idle,
            loaded_download: loaded_dl,
            loaded_upload: loaded_ul,
            bufferbloat_download: bb_dl,
            bufferbloat_upload: bb_ul,
        },
        download,
        upload,
    }
}

// ---- helpers ---------------------------------------------------------------

fn fmt_clock(secs: f64) -> String {
    let s = secs.max(0.0).round() as u64;
    let m = s / 60;
    let r = s % 60;
    format!("{m}:{r:02}")
}

fn fmt_opt_mbps(v: Option<f64>) -> String {
    v.map_or_else(|| "-".to_string(), |x| format!("{x:.2} Mbps"))
}

fn fmt_opt_secs(v: Option<f64>) -> String {
    v.map_or_else(|| "-".to_string(), |x| format!("{x:.1}s"))
}

fn fmt_opt_ms(v: Option<f64>) -> String {
    v.map_or_else(|| "-".to_string(), |x| format!("{x:.1} ms"))
}

fn fmt_count(n: usize) -> String {
    if n == 0 { "-".to_string() } else { n.to_string() }
}

fn fmt_bytes(b: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{:.1} {}", v, UNITS[i])
}

fn u64_p50_ms(samples: &[u64]) -> Option<f64> {
    u64_pct_ms(samples, 50.0)
}

fn u64_pct_ms(samples: &[u64], pct: f64) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut v: Vec<u64> = samples.to_vec();
    v.sort_unstable();
    let rank = (pct / 100.0) * (v.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    let val = if lo == hi {
        v[lo] as f64
    } else {
        let frac = rank - lo as f64;
        v[lo] as f64 + (v[hi] as f64 - v[lo] as f64) * frac
    };
    Some(val / 1_000.0)
}

fn percentile(samples: &[f64], pct: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut v: Vec<f64> = samples.iter().copied().filter(|x| x.is_finite()).collect();
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let rank = (pct / 100.0) * (v.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        return v[lo];
    }
    let frac = rank - lo as f64;
    v[lo] + (v[hi] - v[lo]) * frac
}

fn stable_tail_mean(samples: &[f64]) -> Option<f64> {
    if samples.len() < 4 {
        return None;
    }
    let half = samples.len() / 2;
    let tail = &samples[half..];
    if tail.is_empty() {
        return None;
    }
    let sum: f64 = tail.iter().filter(|x| x.is_finite()).sum();
    let n = tail.iter().filter(|x| x.is_finite()).count();
    if n == 0 {
        return None;
    }
    Some(sum / n as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // The panic hook is process-global, so two tests installing competing hooks
    // would race. Serialize hook-touching tests behind a mutex.
    static HOOK_LOCK: Mutex<()> = Mutex::new(());
    static PREV_CALLS: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn install_panic_hook_chains_to_previous_and_is_idempotent() {
        let _guard = HOOK_LOCK.lock().unwrap();

        let original = std::panic::take_hook();

        PREV_CALLS.store(0, Ordering::SeqCst);
        std::panic::set_hook(Box::new(|_| {
            PREV_CALLS.fetch_add(1, Ordering::SeqCst);
        }));

        // Once-guarded: second call is a no-op so the chain stays single-step.
        install_panic_hook();
        install_panic_hook();

        let result = std::panic::catch_unwind(|| panic!("hook-test"));
        assert!(result.is_err(), "panic should propagate through catch_unwind");
        assert_eq!(
            PREV_CALLS.load(Ordering::SeqCst),
            1,
            "previous hook must run exactly once per panic"
        );

        std::panic::set_hook(original);
    }

    #[test]
    fn restore_terminal_is_safe_when_no_terminal_was_entered() {
        // Both calls underneath are documented as safe no-ops outside of
        // raw mode / alt-screen, which the panic hook depends on.
        restore_terminal();
    }
}
