# Workman

## Description

Workman is a command line utility to process commands using pool of workers

Workman maintains local SQLite database and:

* Track task statuses
* Handle failures and retries
* Save command stout and stderr
* Show simple terminal user interface
* Show min, max and avg command execution time

To undertand what workman is doing, here is simle usage example:

## Installation

Install cargo if needed

```
curl https://sh.rustup.rs -sSf | sh
```

Install workman

```
cargo install --git https://github.com/zim32/workman.git --locked
```

## Usage

### Create tasks.csv file, which contains tasks (each task in new line)

```
1
2
3
// and so on
```

Task can be any string, not just numbers

### Create simple job script

We will create job.php file with this content:

```
<?php

sleep(rand(1, 5));
echo $argv[1];
exit(rand(0, 1));
```

### Execute workman

```
workman process --tasks ./tasks.csv --workers 4 --database progress.db --tries 3 --retry-delay 10 --exec 'php job.php {{task}}'
```

Workman will import tasks from tasks.csv file into progress.db, create 4 worker threads and begin executing our job

There are some interpolation rules, applied to exec command:

* {{N}} - where N is some number, will be replaced by column with index **N** is csv file (starting from 0)
* {{tasks}} will be replaced by column with index 0 for compatibility reasons

If command exit code is not 0, it will retry command after 10 seconds. After 3 failures job will fail

Here is what you will see

![Workman TUI](docs/1.png)


You can open progress.db file with any SQLite client to show additional information (stdout, stderr etc) and you can even edit it manually

## Commands reference

Currently there are two subcommands in workman: process and stats

### Process

This command start processing tasks and show terminal UI

Usage: 

```
workman process --tasks 'tasks.csv' --workers 8 --database tasks.db --exec 'sleep1; echo {{task}}'
```

### Stats

This command just dumps tasks stats to stdout in JSON format and exits

Usage:

```
workman stats -d tasks.db
```

Output:

```
{"new":0,"scheduled":0,"rescheduled":0,"processing":0,"completed":28,"error":0,"aborted":0,"total":28}
```

You can view all commands and arguments using: *workman -h* or *workman --help*