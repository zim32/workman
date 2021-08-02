use rusqlite::{Connection, OptionalExtension};

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

pub fn get_next_task(handle: &ConnHandle) -> Option<String> {
    handle.conn.query_row("SELECT task_id FROM tasks WHERE status = ?1 LIMIT 1", [&TaskStatus::New.to_string()], |row| row.get(0)).optional().unwrap()
}

pub fn get_number_of_incomplete_tasks(handle: &ConnHandle) -> rusqlite::Result<usize> {
    handle.conn.query_row("SELECT COUNT(task_id) FROM tasks WHERE status != ?1", [&TaskStatus::Completed.to_string()], |row| row.get(0))
}

pub fn import_tasks(handle: &ConnHandle, tasks: Vec<&str>, command_template: &str) {
    // println!("Importing {} tasks", tasks.len());
    
    for task in tasks {
        if task.is_empty() {
            continue;
        }

        let task_status = get_task_status(handle, task);
        let command_to_execute = command_template.replace("{{task}}", task);
        
        if task_status.is_some() {
            // println!("Task {} already exists", task);
            continue;
        }

        // insert new task
        // println!("Inserting new task {}", task);
        handle.conn.execute("INSERT INTO tasks (task_id, status, command) VALUES (?1, ?2, ?3)", [task, &TaskStatus::New.to_string(), &command_to_execute]).unwrap();
    }
}


pub fn get_task_status(handle: &ConnHandle, task_id: &str) -> Option<String> {
    handle.conn.query_row("SELECT status FROM tasks WHERE task_id = ?1", [task_id], |row| row.get(0)).optional().unwrap()
}

pub fn get_task_command(handle: &ConnHandle, task_id: &str) -> Option<String> {
    handle.conn.query_row("SELECT command FROM tasks WHERE task_id = ?1", [task_id], |row| row.get(0)).optional().unwrap()
}

pub fn set_task_status(handle: &ConnHandle, task_id: &str, status: TaskStatus) -> rusqlite::Result<usize> {
    handle.conn.execute("UPDATE tasks SET status = ?1 WHERE task_id = ?2 LIMIT 1", [&status.to_string(), task_id])
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

pub fn get_stats(handle: &ConnHandle) -> anyhow::Result<Vec<(String, u64)>> {
    let mut stmt = handle.conn.prepare("SELECT status, COUNT(status) cnt FROM tasks GROUP BY status")?;
    
    let data = stmt.query_map([], |row| {
       let status: String = row.get(0)?;
       let cnt: u64 = row.get(1)?;
       Ok((status, cnt))
    })?;

    let result: Vec<(String, u64)> = data.map(|i| i.unwrap()).collect();

    Ok(result)
}

pub struct ConnHandle {
    conn: Connection
}

#[derive(strum_macros::Display)]
pub enum TaskStatus {
    #[strum(serialize = "new")]
    New,
    #[strum(serialize = "scheduled")]
    Scheduled,
    #[strum(serialize = "processing")]
    Processing,
    #[strum(serialize = "completed")]
    Completed,
    #[strum(serialize = "error")]
    Error,
    #[strum(serialize = "aborted")]
    Aborted
}