# track-compute-jobs

`track-compute-jobs` is a command line program to keep track of jobs submitted on HPC clusters and supercomputers.
It runs locally on the respective machine and stores the job database in the home directory of the user.
It has to be invoked by the user every time a change to the job database is made.

**Note**: This program neither replaces, nor attempts to replace, sophisticated workflow frameworks,
such as Aiida and co. 
Its sole purpose is to keep track of calculations submitted on a specific cluster.

## Installation

### Using `uv`

`uv` is a versatile Python package and project trackr.
For installation instructions, visit
https://github.com/astral-sh/uv.

After setting up `uv`, you can install this tool collection using the following
commands:

```bash
git clone https://github.com/bernik86/track-compute-jobs
cd track-compute-jobs
uv sync
```

This installs the tool collection and its dependencies within a virtual Python
environment located at  `.venv` inside the `track-compute-jobs` folder.

### Using `pip`

`pip`, Python's package trackr, is typically installed alongside Python itself.
To ensure a clean installation, it is recommended to create a virtual
environment for this tool collection.
Use the following commands to download the code, set up a virtual environment,
and install the collection within that environment:

```bash
git clone https://github.com/bernik86/track-compute-jobs
cd track-compute-jobs
python -m venv .venv
source .venv/bin/activate
pip install .
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
  add              Add job to database. Requires specification of ID (-I),
                   Name (-N). Optionally, a submission script (-S), a job
                   directory (-D), comments (-C) and a job status (-T) can be
                   specified.
  del              Delete job from database. Requires specification of ID
                   (-I).
  filter           Show filtered database by selecting column (--key) and
                   specifying string or value (--value) to filter for.
  mod              Modify job in database. Select job by ID (-I), specify the
                   filed to modify (--key) and its new value (--value).
  print-dir        Print directory of job selected by ID (-I).
  set-fail         Set job status of specific job selected by ID (-I) to
                   FAILED and mark as finished. Optionally, a comment can be
                   appended to any existing comments (-C)
  set-ok           Set job status of specific job selected by ID (-I) to OK
                   and mark as finished. Optionally, a comment can be appended
                   to any existing comments (-C)
  show             Show job selected by ID (-I). -I can be specified multiple
                   times to show multiple jobs.
  show-all         Show all jobs in database.
  show-unfinished  Show all jobs with status 'unfinished'. This is the default
                   mode in case no subcommand is specified.
  sort             Sort database by selected column (--key) in ascending order
                   and print it on screen. Descending order can be requested
                   using --desc flag. Use -s/--save to write sorted database
                   back to file.
  update-id        Replace ID (-I) with new value (--value; must be of type
                   int).
```

### Examples

#### Add job
`track_jobs -I 123 -N job_name -S submission_script -D job_directory -C "This is a comment"`

#### Delete job
`track_jobs -I 123`

#### Others

For the use of other functions, consult the help message of the respective sub-command, e.g., `track_jobs filter -h`.


## Bash functions to wrap `slurm` commands

The following functions assume that `track_jobs` is in a directory contained in the `$PATH` variable.

### Submit job

This function can be used to submit a job and automatically add it to the database.
It takes 3 arguments: 
1.) The submission script, 
2.) a job name, and 
3.) a comment string (optional, encompass in "" if more than one word).
The function calles `sbatch` to submit the job and determines the job number assigned by `slurm`, which is used as job ID.



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
            track_jobs add -I $ID -N $2 -D $(pwd) -S $JOBFILE -T "submitted" -C "$3"
        else
            track_jobs add -I $ID -N $2 -D $(pwd) -S $JOBFILE -T "submitted"
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

## Documentation

This project's documentation was generated using `pdoc3` (see https://pdoc3.github.io/pdoc/).
As a result, it consists of static web pages accessible offline.

To view the documentation, simply open `docs/trackjobs/index.html`
in any web browser.
