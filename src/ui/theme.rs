use ratatui::style::{Color, Modifier, Style};

/// Honour the `NO_COLOR` convention (https://no-color.org): when the variable
/// is present in the environment (any value, including empty), every named
/// style in this module collapses to `Style::default()` so output stays
/// monochrome.
pub fn no_color_active() -> bool {
    std::env::var("NO_COLOR").is_ok()
}

/// Truecolor (24-bit) is signalled by `COLORTERM=truecolor` or `COLORTERM=24bit`.
/// When absent or set to anything else, callers must avoid `Color::Rgb(...)` and
/// fall back to the nearest named ANSI colour so 8/16-colour terminals don't
/// render the value as an unrelated approximation.
pub fn truecolor_active() -> bool {
    matches!(
        std::env::var("COLORTERM").as_deref(),
        Ok("truecolor" | "24bit")
    )
}

pub fn download_style() -> Style {
    if no_color_active() {
        return Style::default();
    }
    Style::default().fg(Color::Cyan)
}

pub fn upload_style() -> Style {
    if no_color_active() {
        return Style::default();
    }
    Style::default().fg(Color::Magenta)
}

pub fn success_style() -> Style {
    if no_color_active() {
        return Style::default();
    }
    Style::default().fg(Color::Green)
}

/// Reserved for callers that need a non-bold warning accent (e.g. inline
/// hints in non-key contexts). No emit site yet, so silence dead-code.
#[allow(dead_code)]
pub fn warn_style() -> Style {
    if no_color_active() {
        return Style::default();
    }
    Style::default().fg(Color::Yellow)
}

pub fn error_style() -> Style {
    if no_color_active() {
        return Style::default();
    }
    Style::default().fg(Color::Red)
}

pub fn muted_style() -> Style {
    if no_color_active() {
        return Style::default();
    }
    Style::default().fg(Color::DarkGray)
}

pub fn key_hint_style() -> Style {
    if no_color_active() {
        return Style::default();
    }
    Style::default()
        .fg(Color::Yellow)
        .add_modifier(Modifier::BOLD)
}

pub fn grade_style(grade: char) -> Style {
    if no_color_active() {
        return Style::default();
    }
    let color = match grade {
        'A' => Color::Green,
        'B' => Color::Yellow,
        // No DarkYellow in ratatui::Color — use a dark gold on truecolor
        // terminals and fall back to Yellow elsewhere so we don't ship an Rgb
        // value to a terminal that can't render it.
        'C' => {
            if truecolor_active() {
                Color::Rgb(184, 134, 11)
            } else {
                Color::Yellow
            }
        }
        'D' | 'F' => Color::Red,
        _ => Color::Gray,
    };
    Style::default().fg(color)
}
