use std::io::{self, Stdout};
use tui::layout::{Margin, Rect};
use tui::widgets::{BarChart, Borders, Paragraph};
use tui::{Terminal, widgets::Block};
use tui::backend::TermionBackend;
use termion::raw::{IntoRawMode, RawTerminal};

pub struct TerminalUi {
    terminal: Terminal<TermionBackend<RawTerminal<Stdout>>>
}

impl TerminalUi {
    
    pub fn new() -> anyhow::Result<TerminalUi> {
        let stdout = io::stdout().into_raw_mode()?;
        let backend = TermionBackend::new(stdout);
        let terminal = Terminal::new(backend)?;
        
        Ok(TerminalUi{
            terminal: terminal
        })
    }

    pub fn draw(&mut self, data: &LayoutData) {
        self.terminal.draw(|f| {
            let size = f.size();
            let block = Block::default()
                .title("Workman")
                .borders(Borders::ALL);
            f.render_widget(block, size);

            let size = size.inner(&Margin { horizontal: 2, vertical: 2 });
            let w_status_text = Paragraph::new("Status: ".to_owned() + data.log_message.as_str());
            f.render_widget(w_status_text, size);

            let size = Rect::new(size.x, size.y + 5, size.width, size.height - 5);

            // render tasks stats
            {
                let t: Vec<(&str, u64)> = data.tasks_stats.iter().map(|i| (i.0.as_str(), i.1)).collect();

                let w_tasks_status = BarChart::default()
                    .bar_width(10)
                    .bar_gap(1)
                    .data(t.as_slice());

                f.render_widget(w_tasks_status, size);
            }
            
        }).unwrap();
    }

    pub fn clear(&mut self) {
        self.terminal.clear().unwrap();
    }
}

pub struct LayoutData {
    pub log_message: String,
    pub tasks_stats: Vec<(String, u64)>
}

impl Default for LayoutData {
    
    fn default() -> Self {
        Self {
            log_message: Default::default(),
            tasks_stats: Default::default(),
        }
    }
}