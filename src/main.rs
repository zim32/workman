mod storage;
mod terminal;

use anyhow::Context;
use clap::{App, Arg};
use storage::{TaskStatus, ConnHandle};
use terminal::{LayoutData, TerminalUi};
use std::cmp::{max, min};
use std::io::Read;
use std::process::exit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};
use std::{fs, sync::Arc};
use threadpool::ThreadPool;
use std::{process, thread, u128};
use std::sync::{mpsc}; 

fn main() -> anyhow::Result<()> {
    let matches = App::new("workman")
        .version("0.2.0")
        .author("Author: zim32 [yurij.uvarov@gmail.com]")
        .about("Utility to process commands using pool of workers")
        .arg(Arg::new("tasks").long("tasks").short('t').takes_value(true).required(true).about("Path to tasks list file"))
        .arg(Arg::new("workers").long("workers").short('w').takes_value(true).required(true).default_value("4").about("Number of workers"))
        .arg(Arg::new("db").long("database").short('d').takes_value(true).required(true).default_value("tasks.db").about("Path to database file"))
        .arg(Arg::new("exec").long("exec").short('e').takes_value(true).required(true).about("Command to execute"))
        .arg(Arg::new("tries").long("tries").short('t').takes_value(true).required(false).default_value("0").about("How many times to retry command if it fails"))
        .arg(Arg::new("delay").long("retry-delay").takes_value(true).required(false).default_value("1").about("Number of seconds task will be in rescheduled state before picked up again"))
        .get_matches();

    // read cli arguments
    let db_path = matches.value_of("db").unwrap().to_owned();
    let exec_command = matches.value_of("exec").unwrap().to_owned();
    let tasks_list_file = matches.value_of("tasks").unwrap();
    let num_of_workers: usize = matches.value_of_t("workers").unwrap();
    let retries: u32 = matches.value_of_t("tries").unwrap();
    let retry_delay: u32 = matches.value_of_t("delay").unwrap();

    // setup ui
    let mut ui = terminal::TerminalUi::new()?;
    let mut ld: LayoutData = Default::default();
    ui.clear();
    
    ld.log_message = String::from("Creating database...");
    ui.draw(&ld);

    // setup database
    let connection = storage::create_database(&db_path).context("Can not create database")?;
    
    // import tasks
    let tasks = fs::read_to_string(tasks_list_file).context("Can not read tasks file")?;
    let tasks: Vec<&str> = tasks.split('\n').collect();

    for task in tasks {
        ld.log_message = format!("Importing tasks {}...", task);
        ui.draw(&ld);
        storage::import_task(&connection, task, &exec_command);
    }

    storage::mark_scheduled_tasks_as_new(&connection)?;

    // setup thread pool
    ld.log_message = String::from("Starting thread pool...");
    ui.draw(&ld);

    let pool = ThreadPool::new(num_of_workers);
    let (tx, rx) = mpsc::channel();

    ld.log_message = String::from("Scheduling tasks...");
    ui.draw(&ld);

    // schedule tasks
    schedule_tasks(&connection, retries, &mut ld, &mut ui, &tx, &pool)?;

    let q_was_pressed = Arc::new(AtomicBool::new(false));

    // start thread to handle user input
    {
        let q_was_pressed = Arc::clone(&q_was_pressed);

        thread::spawn(move || {
            // check Q was presset
            let mut buf = vec![0; 1];
            
            if let Ok(_) = std::io::stdin().read(&mut buf) {
                // q pressed
                if buf[0] == 113 {
                    q_was_pressed.store(true, Ordering::SeqCst);
                }
            }
        });
    }

    // start main loop
    {
        let mut processed_tasks_count = 0;
        let mut total_elapsed_time: u128 = 0;
    
        loop {
            if q_was_pressed.load(Ordering::SeqCst) {
                ld.log_message = String::from("Q was pressed. Exiting...");
                ui.draw(&ld);

                storage::mark_pending_tasks_as_aborted(&connection)?;

                exit(3);
            }

            ld.log_message = String::from("Waiting for all jobs to complete... Prease 'q' to quit");
            ld.tasks_stats_struct = storage::get_stats_struct(&connection)?;
            ld.processed_tasks_count = processed_tasks_count;
            ld.total_elapsed_time = total_elapsed_time;
            ui.draw(&ld);
    
            // process chanel messages
            loop {
                match rx.try_recv() {
                    Ok(message) => {
                        match message {
                            ChannelMessage::CommandResult(result) => {
                                processed_tasks_count += 1;
                                total_elapsed_time += result.elapsed_time_ms;
    
                                ld.min_elapsed_time = match ld.min_elapsed_time {
                                    None => Some(result.elapsed_time_ms),
                                    Some(val) => Some(min(val, result.elapsed_time_ms))
                                };
    
                                ld.max_elapsed_time = match ld.max_elapsed_time {
                                    None => Some(result.elapsed_time_ms),
                                    Some(val) => Some(max(val, result.elapsed_time_ms))
                                };
    
                                if !result.exit_status.success() && retries > 0 {
                                    // handle reshedule logic
                                    let reshedule_count = storage::get_task_reshedule_count(&connection, &result.task_id).unwrap();
                                    
                                    if reshedule_count < retries {
                                        storage::reshedule_task(&connection,  &result.task_id, retry_delay)?;
                                    } else {
                                        storage::update_task_from_result(&connection, &result).unwrap();
                                    }
                                } else {
                                    storage::update_task_from_result(&connection, &result).unwrap();
                                }
                            },
                            ChannelMessage::SetTaskStatus{task_id, status} => {
                                storage::set_task_status(&connection, &task_id, status).unwrap();
                            }
                         };   
                    }
        
                    Err(mpsc::TryRecvError::Empty) => break,
        
                    Err(mpsc::TryRecvError::Disconnected) => {
                        println!("Receive channel disconnected");
                        exit(1);
                    }
                }
            }
    
            if storage::get_number_of_incomplete_tasks(&connection)? == 0 {
                ld.tasks_stats_struct = storage::get_stats_struct(&connection)?;
                ui.draw(&ld);
                break;
            }

            schedule_tasks(&connection, retries, &mut ld, &mut ui, &tx, &pool)?;
            thread::sleep(Duration::from_millis(500));
        }
    }

    ld.log_message = String::from("All jobs complete");
    ui.draw(&ld);

    pool.join();

    Ok(())
}

fn schedule_tasks(
    connection: &ConnHandle, 
    retries:u32, 
    ld: &mut LayoutData, 
    ui: &mut TerminalUi, 
    tx: &Sender<ChannelMessage>,
    pool: &ThreadPool
) -> anyhow::Result<()> 
{
    while let Some(task_id) = storage::get_next_task(connection, retries) {
        ld.log_message = format!("Scheduling task {}...", task_id);
        ui.draw(&ld);

        let tx = tx.clone();

        storage::set_task_status(&connection, &task_id, storage::TaskStatus::Scheduled)?;
        let command_to_execute = storage::get_task_command(&connection, &task_id).expect("Can not get task command to execute");

        pool.execute( move || {
            let message = ChannelMessage::SetTaskStatus {task_id: task_id.clone(), status: TaskStatus::Processing};
            tx.send(message).unwrap();

            let exec_result = execute_command(&command_to_execute, &task_id);
            let message = ChannelMessage::CommandResult(exec_result);

            tx.send(message).unwrap();
        });
    }

    Ok(())
}

fn execute_command(command_str: &str, task_id: &str) -> ExecCommandResult {
        let mut command = process::Command::new("sh");
    command.arg("-c");
    command.arg(command_str);
    
    let now = Instant::now();
    let result = command.output().unwrap();
    
    ExecCommandResult {
        task_id: task_id.to_owned(),
        exit_status: result.status,
        command: command_str.to_owned(),
        stdout: String::from_utf8(result.stdout).unwrap(),
        stderr: String::from_utf8(result.stderr).unwrap(),
        elapsed_time_ms: now.elapsed().as_millis()
    }
}

pub struct ExecCommandResult {
    task_id: String,
    exit_status: process::ExitStatus,
    command: String,
    stdout: String,
    stderr: String,
    elapsed_time_ms: u128
}

impl std::fmt::Debug for ExecCommandResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ExecCommandResult {{ task_id: {}, exit_status: {:?}, command: {} }}", self.task_id, self.exit_status, self.command)
    }
}

enum ChannelMessage {
    CommandResult(ExecCommandResult),
    SetTaskStatus { task_id: String, status: TaskStatus }
}