use std::str::FromStr;

use rusqlite::{Connection, OptionalExtension};
use strum_macros::{EnumString, Display as StrumDisplay};

use crate::ExecCommandResult;

pub fn create_database(path: &str) -> anyhow::Result<ConnHandle> {
    let connection = Connection::open(path)?;
    
    // create schema
    connection.execute(
        "CREATE TABLE IF NOT EXISTS tasks (
             task_id VARCHAR(255) primary key,
             status VARCHAR(255) not null,
             command TEXT,
             elapsed_time VARCHAR(255),
             reshedule_count INT not null,
             ignore_till INT null,
             stdout TEXT,
             stderr TEXT
         )",
        [],
    )?;

    let handle = ConnHandle {
        conn: connection
    };

    return Ok(handle);
}

pub fn get_next_task(handle: &ConnHandle, max_tries: u32) -> Option<String> {
    handle.conn.query_row(
        "SELECT task_id FROM tasks WHERE status = ?1 OR (status = ?2 AND reshedule_count <= ?3 AND CAST(strftime('%s', 'now') as INT) > ignore_till ) LIMIT 1",
        [&TaskStatus::New.to_string(), &TaskStatus::Resheduled.to_string(), &max_tries.to_string()],
        |row| row.get(0)
    ).optional().unwrap()
}

pub fn get_number_of_incomplete_tasks(handle: &ConnHandle) -> rusqlite::Result<usize> {
    handle.conn.query_row(
        "SELECT COUNT(task_id) FROM tasks WHERE status != ?1 AND status != ?2",
        [&TaskStatus::Completed.to_string(), &TaskStatus::Error.to_string()],
        |row| row.get(0)
    )
}

pub fn import_task(handle: &ConnHandle, task: &str, command_template: &str) {
    if task.is_empty() {
        return;
    }

    let task_status = get_task_status(handle, task);
    let command_to_execute = command_template.replace("{{task}}", task);
    
    if task_status.is_some() {
        return;
    }

    // insert new task
    // println!("Inserting new task {}", task);
    handle.conn.execute("INSERT INTO tasks (task_id, status, command, reshedule_count) VALUES (?1, ?2, ?3, 0)", [task, &TaskStatus::New.to_string(), &command_to_execute]).unwrap();
}


pub fn get_task_status(handle: &ConnHandle, task_id: &str) -> Option<String> {
    handle.conn.query_row("SELECT status FROM tasks WHERE task_id = ?1", [task_id], |row| row.get(0)).optional().unwrap()
}

pub fn get_task_command(handle: &ConnHandle, task_id: &str) -> Option<String> {
    handle.conn.query_row("SELECT command FROM tasks WHERE task_id = ?1", [task_id], |row| row.get(0)).optional().unwrap()
}

pub fn get_task_reshedule_count(handle: &ConnHandle, task_id: &str) -> Option<u32> {
    handle.conn.query_row("SELECT reshedule_count FROM tasks WHERE task_id = ?1", [task_id], |row| row.get(0)).optional().unwrap()
}

pub fn set_task_status(handle: &ConnHandle, task_id: &str, status: TaskStatus) -> rusqlite::Result<usize> {
    handle.conn.execute("UPDATE tasks SET status = ?1 WHERE task_id = ?2 LIMIT 1", [&status.to_string(), task_id])
}

pub fn reshedule_task(handle: &ConnHandle, task_id: &str, seconds: u32) -> rusqlite::Result<usize> {
    handle.conn.execute(
        "UPDATE tasks SET status = ?1, reshedule_count = reshedule_count + 1, ignore_till = CAST(strftime('%s', 'now') as INT) + ?3  WHERE task_id = ?2 LIMIT 1", 
        [&TaskStatus::Resheduled.to_string(), task_id, &seconds.to_string()]
    )
}

pub fn update_task_from_result(handle: &ConnHandle, result: &ExecCommandResult) -> rusqlite::Result<usize> {
    let status = if result.exit_status.success() { TaskStatus::Completed } else { TaskStatus::Error };

    handle.conn.execute(
        "UPDATE tasks SET status = ?1, command = ?2, stdout = ?3, stderr = ?4, elapsed_time = ?5 WHERE task_id = ?6 LIMIT 1", 
        [&status.to_string(), &result.command, &result.stdout, &result.stderr, &result.elapsed_time_ms.to_string(), &result.task_id])
}

pub fn mark_pending_tasks_as_aborted(handle: &ConnHandle) -> rusqlite::Result<usize> {
    handle.conn.execute("UPDATE tasks SET status = ?1 WHERE status = ?2", [&TaskStatus::Aborted.to_string(), &TaskStatus::Processing.to_string()])
}

pub fn mark_scheduled_tasks_as_new(handle: &ConnHandle) -> rusqlite::Result<usize> {
    handle.conn.execute("UPDATE tasks SET status = ?1 WHERE status = ?2", [&TaskStatus::New.to_string(), &TaskStatus::Scheduled.to_string()])
}

pub fn get_stats_struct(handle: &ConnHandle) -> anyhow::Result<TaskStatsResult> {
    let mut stmt = handle.conn.prepare("SELECT status, COUNT(status) cnt FROM tasks GROUP BY status")?;
    let mut result = TaskStatsResult::default();

    let mut rows = stmt.query([])?;
    
    while let Ok(Some(row)) = rows.next() {
        let status_str: String = row.get(0).unwrap();
        let count: u64 = row.get(1).unwrap();
        let status = TaskStatus::from_str(&status_str)?;

        match status {
            TaskStatus::New         => result.new += count,
            TaskStatus::Aborted     => result.aborted += count,
            TaskStatus::Completed   => result.completed += count,
            TaskStatus::Error       => result.error += count,
            TaskStatus::Processing  => result.processing += count,
            TaskStatus::Scheduled   => result.scheduled += count,
            TaskStatus::Resheduled  => result.rescheduled += count
        }

        result.total += count;
    }

    Ok(result)
}

pub struct ConnHandle {
    conn: Connection
}

#[derive(StrumDisplay, EnumString)]
pub enum TaskStatus {
    #[strum(serialize = "new")]
    New,
    #[strum(serialize = "scheduled")]
    Scheduled,
    #[strum(serialize = "rescheduled")]
    Resheduled,
    #[strum(serialize = "processing")]
    Processing,
    #[strum(serialize = "completed")]
    Completed,
    #[strum(serialize = "error")]
    Error,
    #[strum(serialize = "aborted")]
    Aborted
}

#[derive(Default)]
pub struct TaskStatsResult {
    pub new: u64,
    pub scheduled: u64,
    pub rescheduled: u64,
    pub processing: u64,
    pub completed: u64,
    pub error: u64,
    pub aborted: u64,
    pub total: u64
}