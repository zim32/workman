use std::io::{self, Stdout};
use tui::layout::{Margin, Rect};
use tui::style::{Color, Modifier, Style};
use tui::text::{Span, Spans};
use tui::widgets::{Borders, Gauge, Paragraph};
use tui::{Terminal, widgets::Block};
use tui::backend::TermionBackend;
use termion::raw::{IntoRawMode, RawTerminal};

use crate::storage::TaskStatsResult;

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

        
            // render tasks stats
            let size = Rect::new(size.x, size.y + 2, size.width, size.height - 2);
           
            let avg_elapsed_time = if data.processed_tasks_count > 0 {
                data.total_elapsed_time / data.processed_tasks_count as u128
            } else {
                0
            };

            let text = vec![
                Spans::from(vec![
                    Span::raw(format!("total:       {}", data.tasks_stats_struct.total)),
                ]),
                Spans::from(vec![
                    Span::styled(format!("completed:   {}", data.tasks_stats_struct.completed), Style::default().fg(Color::LightGreen))
                ]),
                Spans::from(vec![
                    Span::raw(format!("new:         {}", data.tasks_stats_struct.new)),
                ]),
                Spans::from(vec![
                    Span::raw(format!("processing:  {}", data.tasks_stats_struct.processing)),
                ]),
                Spans::from(vec![
                    Span::raw(format!("scheduled:   {}", data.tasks_stats_struct.scheduled)),
                ]),
                Spans::from(vec![
                    Span::styled(format!("rescheduled: {}", data.tasks_stats_struct.rescheduled), Style::default().fg(Color::Yellow))
                ]),
                Spans::from(vec![
                    Span::styled(format!("error:       {}", data.tasks_stats_struct.error), Style::default().fg(Color::Red))
                ]),
                Spans::from(vec![
                    Span::styled(format!("aborted:     {}", data.tasks_stats_struct.aborted), Style::default().fg(Color::LightMagenta))
                ]),
                Spans::from(vec![
                    Span::raw(""),
                ]),
                Spans::from(vec![
                    Span::raw(format!("Avg task execution time (ms):   {}", avg_elapsed_time)),
                ]),
                Spans::from(vec![
                    Span::raw(format!("Min task execution time (ms):   {}", data.min_elapsed_time.map_or("-".to_owned(), |f| f.to_string()))),
                ]),
                Spans::from(vec![
                    Span::raw(format!("Max task execution time (ms):   {}", data.max_elapsed_time.map_or("-".to_owned(), |f| f.to_string()))),
                ])
            ];

            let w_tasks_status = Paragraph::new(text);
            f.render_widget(w_tasks_status, size);

            // render progress bar
            let size = Rect::new(size.x, size.y + 14, size.width, size.height - 14);
            {
                let size = Rect::new(size.x, size.y, size.width, 1);

                let progress = if data.tasks_stats_struct.total > 0 {
                    let num_of_finished_jobs = data.tasks_stats_struct.completed + data.tasks_stats_struct.error;
                    let tmp = (num_of_finished_jobs as f64 / data.tasks_stats_struct.total as f64) * 100.0;
                    let tmp = tmp as u16;
                    tmp
                } else {
                    0
                };
    
                let w_total_progress = Gauge::default()
                .percent(progress)
                .gauge_style(Style::default().fg(Color::White).bg(Color::Black).add_modifier(Modifier::ITALIC));
                
                f.render_widget(w_total_progress, size);
            }
        }).unwrap();
    }

    pub fn clear(&mut self) {
        self.terminal.clear().unwrap();
    }
}

pub struct LayoutData {
    pub log_message: String,
    pub tasks_stats_struct: TaskStatsResult,
    pub processed_tasks_count: u64,
    pub total_elapsed_time: u128,
    pub min_elapsed_time: Option<u128>,
    pub max_elapsed_time: Option<u128>
}

impl Default for LayoutData {
    
    fn default() -> Self {
        Self {
            log_message: Default::default(),
            tasks_stats_struct: Default::default(),
            processed_tasks_count: Default::default(),
            total_elapsed_time: Default::default(),
            min_elapsed_time: None,
            max_elapsed_time: None,
        }
    }
}