use std::collections::VecDeque;
use std::io::{self, Stdout};
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers};
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
use tokio::time;

use super::{PhaseKind, ProbeKind, UiEvent, UiEventRx};
use crate::metadata::Metadata;
use crate::report::PhaseReport;
use crate::sampler::Sample;
use crate::stats::BufferbloatGrade;

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
    mode: Option<&'static str>,
    metadata: Option<Box<Metadata>>,
    session_started_at: Option<Instant>,
    total_planned_secs: f64,

    active: Option<ActivePhase>,
    finished: Vec<PhaseReport>,

    idle_rtts_us: Vec<u64>,
    loaded_dl_rtts_us: Vec<u64>,
    loaded_ul_rtts_us: Vec<u64>,

    last_error: Option<String>,
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
            UiEvent::SessionStarted { mode, total_planned_secs, metadata } => {
                self.mode = Some(mode);
                self.metadata = Some(metadata);
                self.session_started_at = Some(Instant::now());
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
            UiEvent::SessionFinished(_) => {
                self.active = None;
            }
            UiEvent::Error(e) => self.last_error = Some(e),
        }
    }

    fn session_elapsed_secs(&self) -> f64 {
        self.session_started_at
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0)
    }
}

fn draw(f: &mut ratatui::Frame, state: &State) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),  // Header
            Constraint::Min(0),     // Phase area (elastic)
            Constraint::Length(5),  // Latency
            Constraint::Length(3),  // Footer
        ])
        .split(f.area());

    draw_header(f, chunks[0], state);
    draw_phase_area(f, chunks[1], state);
    draw_latency(f, chunks[2], state);
    draw_footer(f, chunks[3], state);
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
    // Each finished phase collapses to a 4-line card (2 content + borders).
    // The active phase (if any) takes the remaining space.
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

fn draw_footer(f: &mut ratatui::Frame, area: Rect, state: &State) {
    let line = if let Some(err) = &state.last_error {
        Line::from(vec![
            Span::styled("error: ", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
            Span::raw(err.clone()),
            Span::raw("    "),
            Span::styled("q / Esc / Ctrl-C", Style::default().fg(Color::DarkGray)),
            Span::raw(" quit "),
        ])
    } else {
        Line::from(vec![
            Span::styled("q / Esc / Ctrl-C", Style::default().fg(Color::DarkGray)),
            Span::raw(" quit"),
        ])
    };
    let para = Paragraph::new(line).block(Block::default().borders(Borders::ALL));
    f.render_widget(para, area);
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
