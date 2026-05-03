use crate::metadata::Metadata;
use crate::report::{PhaseReport, SessionReport};
use crate::sampler::Sample;

pub mod tui;

/// Which throughput phase a [`UiEvent`] refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseKind {
    Download,
    Upload,
}

/// Which kind of latency probe a [`UiEvent::LatencyProbe`] carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeKind {
    Idle,
    LoadedDownload,
    LoadedUpload,
}

/// Streaming event emitted by the speed-test engine for any UI consumer
/// (TUI, JSON writer, structured-log forwarder, …).
#[derive(Debug, Clone)]
pub enum UiEvent {
    SessionStarted {
        mode: &'static str,
        total_planned_secs: f64,
        metadata: Box<Metadata>,
    },
    PhaseStarted {
        kind: PhaseKind,
        label: &'static str,
        planned_secs: f64,
    },
    Throughput(Sample),
    LatencyProbe { kind: ProbeKind, rtt_us: u64 },
    PhaseFinished(PhaseReport),
    SessionFinished(Box<SessionReport>),
    Error(String),
}

/// Multi-producer, multi-consumer event bus for UI feeds.
///
/// `tokio::sync::broadcast` is chosen over `mpsc` because we want every
/// subscriber (live TUI chart, JSON writer, structured-log forwarder) to
/// see the full stream independently, and over `watch` because each event
/// must be delivered, not just the latest snapshot. Overflow is lossy: if
/// a slow subscriber's queue fills, the channel drops the oldest events
/// for that subscriber. That is acceptable here — a missed `Throughput`
/// sample only blurs the live chart for one tick; the authoritative
/// numbers arrive in `PhaseFinished` / `SessionFinished`, which a UI is
/// expected to always render.
pub type UiEventTx = tokio::sync::broadcast::Sender<UiEvent>;

/// Subscriber side of the UI event bus. See [`UiEventTx`] for design notes.
pub type UiEventRx = tokio::sync::broadcast::Receiver<UiEvent>;
