mod storage;
mod terminal;

use anyhow::Context;
use clap::{App, Arg};
use storage::TaskStatus;
use terminal::LayoutData;
use std::io::Read;
use std::process::exit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::{fs, sync::Arc};
use threadpool::ThreadPool;
use std::{process, thread, u128};
use std::sync::{mpsc}; 

fn main() -> anyhow::Result<()> {
    let matches = App::new("workman")
        .version("0.1.0")
        .author("Author: zim32 [yurij.uvarov@gmail.com]")
        .about("Utility to process commands using pool of workers")
        .arg(Arg::new("tasks").long("tasks").short('t').takes_value(true).required(true).about("Path to tasks list file"))
        .arg(Arg::new("workers").long("workers").short('w').takes_value(true).required(true).default_value("4").about("Number of workers"))
        .arg(Arg::new("db").long("database").short('d').takes_value(true).required(true).default_value("tasks.db").about("Path to database file"))
        .arg(Arg::new("exec").long("exec").short('e').takes_value(true).required(true).about("Command to execute"))
        .get_matches();

    // read cli arguments
    let db_path = matches.value_of("db").unwrap().to_owned();
    let exec_command = matches.value_of("exec").unwrap().to_owned();
    let tasks_list_file = matches.value_of("tasks").unwrap();
    let num_of_workers: usize = matches.value_of_t("workers").unwrap();

    // setup ui
    let mut ui = terminal::TerminalUi::new()?;
    let mut ld: LayoutData = Default::default();
    ui.clear();
    
    ld.log_message = String::from("Creating database...");
    ui.draw(&ld);

    // setup database
    let connection = storage::create_database(&db_path).context("Can not create database")?;
    
    // import tasks
    ld.log_message = String::from("Importing task...");
    ui.draw(&ld);

    let tasks = fs::read_to_string(tasks_list_file).context("Can not read tasks file")?;
    let tasks: Vec<&str> = tasks.split('\n').collect();
    storage::import_tasks(&connection, tasks, &exec_command);
    storage::mark_scheduled_tasks_as_new(&connection)?;

    ld.tasks_stats = storage::get_stats(&connection)?;
    ui.draw(&ld);

    // setup thread pool
    ld.log_message = String::from("Starting thread pool...");
    ui.draw(&ld);

    let pool = ThreadPool::new(num_of_workers);
    let (tx, rx) = mpsc::channel();
    let sig_int_received = Arc::new(AtomicBool::new(false));

    // setup SIGINT handler
    {
        // handle SIGINT
        let db_path = db_path.clone();
        let sig_int_received = Arc::clone(&sig_int_received);
        ctrlc::set_handler(move || {
            println!("SIGINT received");
            sig_int_received.store(true, std::sync::atomic::Ordering::SeqCst);
            // mark all penging tasks as aborted
            let connection = storage::create_database(&db_path).expect("Can not create database connection");
            storage::mark_pending_tasks_as_aborted(&connection).expect("Can not mark pending tasks as aborted");
            println!("Exit");
            exit(1);
        })?;
    }

    ld.log_message = String::from("Scheduling tasks...");
    ui.draw(&ld);

    // sdhedule tasks
    while let Some(task_id) = storage::get_next_task(&connection) {
        // println!("Scheduling task {}", task_id);
        ld.log_message = format!("Scheduling task {}...", task_id);
        ui.draw(&ld);

        let tx = tx.clone();

        storage::set_task_status(&connection, &task_id, storage::TaskStatus::Scheduled)?;
        let command_to_execute = storage::get_task_command(&connection, &task_id).expect("Can not get task command to execute");

        pool.execute( move || {
            let message = ChannelMessage::SetTaskStatus {task_id: task_id.clone(), status: TaskStatus::Processing};
            tx.send(message).unwrap();

            // println!("Executing: {}", command_to_execute);
            let exec_result = execute_command(&command_to_execute, &task_id);
            let message = ChannelMessage::CommandResult(exec_result);

            tx.send(message).unwrap();
        });
    }

    let sig_int_received = Arc::clone(&sig_int_received);

    ld.log_message = String::from("Waiting for all jobs to complete...");
    ui.draw(&ld);

    // start thread to handle user input
    thread::spawn(|| {
        // check Q was presset
        let mut buf = vec![0; 1];
        
        if let Ok(_) = std::io::stdin().read(&mut buf) {
            // q pressed
            if buf[0] == 113 {
                exit(2);
            }
        }
    });

    loop {
        ld.tasks_stats = storage::get_stats(&connection)?;
        ui.draw(&ld);

        // process chanel messages
        loop {
            match rx.try_recv() {
                Ok(message) => {
                    match message {
                        ChannelMessage::CommandResult(result) => {
                            // println!("Command result rereived {:?}", result);
                            if sig_int_received.load(Ordering::SeqCst) {
                                storage::set_task_status(&connection, &result.task_id, storage::TaskStatus::Aborted).unwrap();
                            }
    
                            storage::update_task_from_result(&connection, &result).unwrap();
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
            break;
        }

        thread::sleep(Duration::from_secs(1));
    }

    ld.log_message = String::from("All jobs complete");
    ui.draw(&ld);

    pool.join();

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