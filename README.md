# track-compute-jobs

`track-compute-jobs` is a CLI tool to track HPC compute jobs across one or multiple clusters.
It stores job metadata locally in a Polars database (`~/job_db.polars`) and can sync with remote hosts via SSH.

Jobs are tracked by: ID, Name, Script, Directory, Status, Checked flag, Comments, Date, and optional Host.

**Note**: This program neither replaces, nor attempts to replace, sophisticated workflow frameworks,
such as Aiida and co.
Its sole purpose is to keep track of calculations submitted on a specific cluster.

> **⚠️ Disclaimer**: This program has only been tested on Linux systems (both local and remote). It may not work on macOS or Windows.

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
  - [Using `uv`](#using-uv)
  - [Using `pip`](#using-pip)
- [Running](#running)
  - [Usage](#usage)
  - [Examples](#examples)
- [Bash functions to wrap `slurm` commands](#bash-functions-to-wrap-slurm-commands)
- [Checking status of jobs](#checking-status-of-jobs)
- [Remote Operations](#remote-operations)
  - [Configuration](#configuration)
  - [Remote Commands](#remote-commands)
  - [Examples](#examples-1)
- [License](#license)

## Requirements

- Python 3.12 or higher

## Installation

### Using `uv`

`uv` is a versatile Python package and project tracker.
For installation instructions, visit https://github.com/astral-sh/uv.

After setting up `uv`, you can install this tool collection using the following commands:

```bash
git clone https://github.com/rbi-mtm/track-compute-jobs
cd track-compute-jobs
uv sync
```

This installs the tool collection and its dependencies within a virtual Python
environment located at `.venv` inside the `track-compute-jobs` folder.

To enable remote operations (SSH-based sync and remote job submission):

```bash
uv sync --extra remote
```

### Using `pip`

`pip`, Python's package tracker, is typically installed alongside Python itself.
To ensure a clean installation, it is recommended to create a virtual
environment for this tool collection.
Use the following commands to download the code, set up a virtual environment,
and install the collection within that environment:

```bash
git clone https://github.com/rbi-mtm/track-compute-jobs
cd track-compute-jobs
python -m venv .venv
source .venv/bin/activate
pip install .
```

For remote operations:

```bash
pip install "track-compute-jobs[remote]"
```

## Running

During installation, the executable `track_jobs` is placed in the `.venv/bin/`
directory for easy access.

To execute this script, activate the virtual environment by running
`source .venv/bin/activate`.
On Linux systems, you can optionally link the script (found in `.venv/bin/`) to a
directory within your `$PATH` variable for easier access.
However, avoid adding the whole `.venv/bin/` path to the `$PATH`
variable, as it also contains copies of the Python and other executables
which might conflict with globally installed programs.

### Usage

```plain
Usage: track_jobs [OPTIONS] COMMAND [ARGS]...

  CLI entry point

Options:
  -h, --help  Show this message and exit.

Commands:
  add             Add job to database. Requires specification of ID (-I), Name
                  (-N). Optionally, a submission script (-S), a job directory
                  (-D), comments (-C) and a job status (-T) can be specified.
  check-status    Checks the status of unchecked jobs. Requires a file named
                  check_status_command in the $HOME/.config/track_jobs
                  directory. This file needs to contain the command used by
                  the job scheduler to report job id and status. For slurm, a
                  default will be generated if the file is not found.
                  Use --print to show unchecked jobs after checking status.
  compare         Compare jobs by ID (-I) or current directory (--this).
  del             Delete job from database. Requires specification of ID (-I).
                  (-I can be specified multiple times to delete multiple jobs).
  filter          Show filtered database by selecting column (--key) and
                  specifying string or value (--value) to filter for.
  mod             Modify job in database. Select job by ID (-I), specify the
                  field to modify (--key) and its new value (--value).
  print-dir       Print directory of job selected by ID (-I) or last unchecked
                  job (if called without the -I parameter).
  remote          Remote operations via SSH.
  set-fail        Set job status of specific job(s) selected by ID (-I, more
                  than one can be specified) to FAILED and mark as checked.
                  Optionally, a comment can be appended to any existing
                  comments (-C, same comment for all selected jobs). Use
                  --this to select jobs by current directory.
  set-ok          Set job status of specific job(s) selected by ID (-I, more
                  than one can be specified) to OK and mark as checked.
                  Optionally, a comment can be appended to any existing
                  comments (-C, same comment for all selected jobs). Use
                  --this to select jobs by current directory.
  show            Show job selected by ID (-I). -I can be specified multiple
                  times to show multiple jobs.
  show-all        Show all jobs in database.
  show-unchecked  Show all jobs that have not been marked as checked by the
                  user. This is the default mode in case no subcommand is
                  specified. Use --only-IDs to show only job IDs.
  sort            Sort database by selected column (--key) in ascending order
                  and print it on screen. Descending order can be requested
                  using --desc flag. Use -s/--save to write sorted database
                  back to file.
  tail            Show last n jobs in the database (by date; default n=5).
                  Use -n to specify the number of jobs.
  update-id       Replace ID (-I) with new value (--value; must be of type
                  int).
```

### Examples

#### Add job
```bash
track_jobs add -I 123 -N job_name -S submission_script -D job_directory -C "This is a comment"
```

#### Delete job
```bash
track_jobs del -I 123
```

#### Show unchecked jobs (default)
```bash
track_jobs
# or explicitly:
track_jobs show-unchecked
```

#### Show only IDs of unchecked jobs
```bash
track_jobs show-unchecked --only-IDs
```

#### Set job status by current directory
```bash
# Mark the job in current directory as OK
track_jobs set-ok --this
```

#### Others

For the use of other functions, consult the help message of the respective sub-command, e.g., `track_jobs filter -h`.

## Bash functions to wrap `slurm` commands

The following functions assume that `track_jobs` is in a directory contained in the `$PATH` variable.

### Submit job

This function can be used to submit a job and automatically add it to the database.
It takes 3 arguments:
1.) The submission script,
2.) a job name, and
3.) a comment string (optional, enclosed in quotes if more than one word).
The function calls `sbatch` to submit the job and determines the job number assigned by `slurm`, which is used as job ID.

Example:
`run_job Input/jobscript some_job_name "This is a test job"`

```bash
run_job () {
        cp $1 .
        JOBFILE=$(basename $1)
        RES=$(sbatch --parsable -J $2 $JOBFILE)
        IFS=';' read -a RET <<< $RES
        ID="${RET[0]}"

        if [ -n "${3}" ]; then
            echo "Comment: " $3
            track_jobs add -I $ID -N $2 -D $(pwd -P) -S $JOBFILE -T "submitted" -C "$3"
        else
            track_jobs add -I $ID -N $2 -D $(pwd -P) -S $JOBFILE -T "submitted"
        fi
}
```

### Cancel job

Cancels the job by calling `scancel` and sets the job status to "failed".
It takes the job ID and, optionally, a comment as arguments.

Example: `cancel_job 123 "This job is canceled"`

```bash
cancel_job () {
        echo "Job ID: " $1

        scancel $1

        COMMENT=" Killed manually"

        if [ -n "${2}" ]; then
            COMMENT=" $2 - Killed manually"
            echo "Comment: " $COMMENT

        fi
        track_jobs set-fail -I $1 -C "$COMMENT"
}
```

### Go to job directory

This function takes a job id as argument, looks up the corresponding job directory in the database
and changes to that directory.

Example: `goto_job 123`

```bash
goto_job () {
        NEWDIR=$(track_jobs print-dir -I $1)
        cd $NEWDIR
}
```

### Go to job directory of last unchecked job
This function does not take any arguments, but determines the directory based on the `Checked?` field in the database.
It then takes the last job for which this field has the value `False` and goes to the directory of this job.

Example: `goto_last_job`

```bash
goto_last_job () {
        NEWDIR=$(track_jobs print-dir)
        if [ -d "$NEWDIR" ]; then
                cd $NEWDIR
        else
                echo "$NEWDIR"
        fi
}
```

### Go to n-th unchecked job

This function takes a number as argument and goes to the directory of the n-th unchecked job.
The jobs are sorted by date (oldest first).

Example: `goto_nth_unchecked_job 3`

```bash
goto_nth_unchecked_job () {
        IDS=$(track_jobs show-unchecked --only-IDs)
        NLINES=$(wc -l <<< $IDS)
        NJOBS=$(($NLINES - 4))
        if [[ "$1" -le "$NJOBS" ]]; then
                line=$(echo "$IDS" | head -n $((6 + $1)) | tail -n1)
                read -r first second third JOB fourth <<< "$line"
                echo "$second" "$JOB"
                goto_job $JOB
        else
                echo "Only $NJOBS unchecked jobs exist (requested #$1)!"
        fi
}
```

## Checking status of jobs

Jobs that have not been checked by the user (i.e., jobs for which the field "Checked?" is false), can be checked automatically using the following command:

`track_jobs check-status`

This will change the `Status` field of the jobs to:

- `RUNNING`: if the job is running
- `PENDING`: if the job is waiting for execution
- `Finished?`: if the job id cannot be found in the output of the query command

Array jobs are handled by extracting the base job ID (splitting on `_`, `.`, `[`).

For this command to run successfully, the file `$HOME/.config/track_jobs/check_status_command` must exist.
On the first line, this file should contain the command for querying the jobs status provided by the scheduler (e.g., `squeue` for `slurm`).
The following lines each can contain arguments passed to the command from the first line.
The output format has to be `JOB_ID  STATUS`.
For `slurm`, a default configuration will be generated if the file with the command is not found.

For `torque` (i.e., `qstat`), unfortunately, the process is a bit more complicated.
However, the following script, placed in a location where it can be executed (e.g., `$HOME/.local/bin/`) can be used to get the job status in the required format (do not forget to replace `username`):

```bash
#!/usr/bin/bash

qstat -u username -a -n -1 | grep -E '^[0-9]+.*(R|Q|B)' | awk '{print $1, $10}' | sed "s/R/RUNNING/g" | sed "s/Q/PENDING/g" | sed "s/B/RUNNING (ARRAY JOB)/g"

```

## Remote Operations

For tracking jobs across multiple HPC clusters, `track-compute-jobs` supports SSH-based sync and remote job submission. This requires the `fabric` package (see Installation section).

> **⚠️ Warning**: Remote operations have only been tested with SLURM. They may not work with other job queuing systems (e.g., PBS, Torque). Use at your own risk.

### Configuration

Create `~/.config/track_jobs/hosts.json` with host configurations. See `examples/hosts.json` for a complete example:

```json
{
  "cluster": {
    "remote_job_db_path": "/remote/user/job_db.polars",
    "local_base_path": "/home/user/Projects/",
    "remote_base_path": "/remote/user/Projects/",
    "check_jobs_command": "squeue --noheader --format=\"%.18i %.9T\"",
    "remote_submit_cmd": "sbatch --parsable -J $JOBNAME $JOBFILE",
    "remote_submit_array_cmd": "sbatch --parsable -J $JOBNAME --array=$START-$END $JOBFILE",
    "remote_cancel_cmd": "scancel $JOBID"
  }
}
```

Required fields:
- `remote_job_db_path`: Path to the job database on the remote host
- `remote_submit_cmd`: Command to submit jobs (use `$JOBNAME` and `$JOBFILE` placeholders)
- `remote_cancel_cmd`: Command to cancel jobs (use `$JOBID` placeholder)

Optional fields:
- `local_base_path`: Local base path for mapping local dirs to remote dirs
- `remote_base_path`: Remote base path for mapping local dirs to remote dirs
- `check_jobs_command`: Custom command to check job status (otherwise uses default; see section "Checking status of jobs")
- `remote_submit_array_cmd`: Command to submit array jobs (use `$JOBNAME`, `$JOBFILE`, `$START`, and `$END` placeholders)

The host identifier (e.g., `cluster`) must match an entry in your SSH config (`~/.ssh/config`).

### Remote Commands

- `track_jobs remote -H <host> pull` - Sync remote database to local
- `track_jobs remote -H <host> push` - Push local jobs to remote
- `track_jobs remote -H <host> check-status` - Check job status on remote
- `track_jobs remote -H <host> submit -S script -N name -D dir` - Submit job remotely
- `track_jobs remote -H <host> cancel -I 123` - Cancel remote job

### Examples

#### Pull jobs from remote cluster
```bash
track_jobs remote -H cluster pull
```

#### Submit job on remote cluster
```bash
track_jobs remote -H cluster submit -S my_script.sh -N my_job -D /home/user/Projects/project1
```

#### Check status on remote cluster
```bash
track_jobs remote -H cluster check-status --print
```

## License

This project is licensed under the GNU General Public License v3 (GPLv3). See the [LICENSE](LICENSE) file for details.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.