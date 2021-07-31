mod storage;

use clap::{App, Arg};
use storage::TaskStatus;
use std::process::exit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{fs, sync::Arc};
use threadpool::ThreadPool;
use std::{process, thread};
use std::sync::{mpsc};

fn main() {
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

    // setup database
    let connection = storage::create_database(&db_path).unwrap();
    
    // import tasks
    let tasks = fs::read_to_string(tasks_list_file).expect("Can not read tasks file");
    let tasks: Vec<&str> = tasks.split('\n').collect();
    storage::import_tasks(&connection, tasks, &exec_command);
    storage::mark_scheduled_tasks_as_new(&connection).unwrap();

    // setup thread pool
    let pool = ThreadPool::new(num_of_workers);
    let (tx, rx) = mpsc::channel();
    let mut rx_iter = rx.into_iter();
    let sig_int_received = Arc::new(AtomicBool::new(false));
    
    {
        // handle SIGINT
        let db_path = db_path.clone();
        let sig_int_received = Arc::clone(&sig_int_received);
        ctrlc::set_handler(move || {
            println!("SIGINT received");
            sig_int_received.store(true, std::sync::atomic::Ordering::SeqCst);
            // mark all penging tasks as aborted
            let connection = storage::create_database(&db_path).unwrap();
            storage::mark_pending_tasks_as_aborted(&connection).unwrap();
            println!("Exit");
            exit(1);
        }).unwrap();
    }

    loop {
        let task = storage::get_next_task(&connection);
        
        if task.is_none() {
            println!("All tasks scheduled");
            break;
        }

        let task_id = task.unwrap();

        println!("Scheduling task {}", task_id);

        let tx = tx.clone();

        storage::set_task_status(&connection, &task_id, storage::TaskStatus::Scheduled).unwrap();
        let command_to_execute = storage::get_task_command(&connection, &task_id).unwrap();

        pool.execute( move || {
            let message = ChannelMessage::SetTaskStatus {task_id: task_id.clone(), status: TaskStatus::Processing};
            tx.send(message).unwrap();

            println!("Executing: {}", command_to_execute);
            let exec_result = execute_command(&command_to_execute, &task_id);
            let message = ChannelMessage::CommandResult(exec_result);

            tx.send(message).unwrap();
        });
    }

    let sig_int_received = Arc::clone(&sig_int_received);

    let main_handle = thread::spawn(move || {
        let connection = storage::create_database(&db_path).unwrap();
        
        loop {
            let message = rx_iter.next().unwrap();
            match message {
                ChannelMessage::CommandResult(result) => {
                    println!("Command result rereived {:?}", result);
                    if sig_int_received.load(Ordering::SeqCst) {
                        storage::set_task_status(&connection, &result.task_id, storage::TaskStatus::Aborted).unwrap();
                        return;
                    }

                    storage::update_task_from_result(&connection, &result).unwrap();
                },
                ChannelMessage::SetTaskStatus{task_id, status} => {
                    storage::set_task_status(&connection, &task_id, status).unwrap();
                }
                ChannelMessage::NoMoreTasks => {
                    return;
                }
            }   
        }
    });

    // wait for all jobs to finish
    println!("Waiting for all jobs to complete...");
    pool.join();
    println!("All jobs complete");

    // send message to indicate there is no more jobs to process
    let message = ChannelMessage::NoMoreTasks;
    tx.send(message).unwrap();

    main_handle.join().unwrap();
}

fn execute_command(command_str: &str, task_id: &str) -> ExecCommandResult {
    let mut command = process::Command::new("sh");
    command.arg("-c");
    command.arg(command_str);
    
    let result = command.output().unwrap();
    
    ExecCommandResult {
        task_id: task_id.to_owned(),
        exit_status: result.status,
        command: command_str.to_owned(),
        stdout: String::from_utf8(result.stdout).unwrap(),
        stderr: String::from_utf8(result.stderr).unwrap()
    }
}

pub struct ExecCommandResult {
    task_id: String,
    exit_status: process::ExitStatus,
    command: String,
    stdout: String,
    stderr: String
}

impl std::fmt::Debug for ExecCommandResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ExecCommandResult {{ task_id: {}, exit_status: {:?}, command: {} }}", self.task_id, self.exit_status, self.command)
    }
}

enum ChannelMessage {
    CommandResult(ExecCommandResult),
    SetTaskStatus { task_id: String, status: TaskStatus },
    NoMoreTasks
}