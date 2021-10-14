# Covid Data Example
This is a simple example usage of nextdata. Our example user is getting data about covid
cases and deaths from a CDC API and then running a "model" on it (computing a 7-day
exponentially weighted average), and then producing a report via a jupyter notebook.

## Key concepts:

In this example, we have three jobs. The first job downloads data from a CDC API and
writes a pandas dataframe to nextdb: 

```python
# from cdc_covid_data.py:cdc_covid_data
# ... try to download data from the CDC API and populate df ...
if not df.empty:
    ndb_conn.write("cdc_covid_data", df)
```

We will schedule this job to run for each day:

```python
# from nextbeat_main.py:add_daily_jobs
_function(
    function_pointer=covid_data.cdc_covid_data.cdc_covid_data,
    function_args=[date],
    run_on=Periodic(datetime.timedelta(minutes=2)),
    run_state_predicate=AllPredicate(
        [
            PointInTimePredicate.between(tomorrow(8, 30), tomorrow(17, 30)),
            UntilNextdbWritten.any(),
        ]
    ),
),
```

This job definition expresses that we want to run every 2 minutes between 8:30am and
5:30pm on T+1, until we've written something to nextdb. The integration between nextbeat
(the job scheduler) and nextdb (the database) makes it easy to specify these kinds of
database-driven conditions for scheduling jobs. Here, the `UntilNextdbWritten` clause
eliminates a class of issues related to determining whether a job raised an exception vs
the data was not available vs successfully acquired data.

Then, we have a second job that reads `cdc_covid_data`, computes a smoothed version of
the data and then writes to `cdc_covid_data_smoothed`. Note that we're querying the
`cdc_covid_data` table using pandas-like syntax to filter to the subset of data that we
need:

```python
# from cdc_covid_data.py:cdc_covid_data_smoothed
t = ndb_conn.read("cdc_covid_data")
df = (
    t[
        t["submission_date"].between(
            pd.to_datetime(date - datetime.timedelta(days=6)),
            pd.to_datetime(date),
        )
    ]
    .to_pd()
    .sort_values("submission_date")
)
# ... some smoothing computation on df ...
ndb_conn.write("cdc_covid_data_smoothed", df)
```

Scheduling this job is even easier:

```python
# from nextbeat_main.py:add_daily_jobs
_function(
    function_pointer=covid_data.cdc_covid_data.cdc_covid_data_smoothed,
    function_args=[date],
    run_on=NextdbDynamicDependency(scope),
)
```

This tells nextbeat that this job should be run whenever the nextdb tables it reads get
updated. This means we don't need to explicitly connect `cdc_covid_data_smoothed` and
`cdc_covid_data` in the job definitions, which gives us flexibility--we can rename
`cdc_covid_data`, split it into multiple jobs or add a new job that also updates the
`cdc_covid_data` table. No matter what we do, `cdc_covid_data_smoothed` will kick off
whenever there are updates to the `cdc_covid_data` table. In some ways, this makes
`cdc_covid_data_smoothed` almost like a materialized view. Also, if we change
`cdc_covid_data_smoothed` to read from a different table, like `cdc_covid_data_new`, we
don't have to worry about manually updating the job definition/dependencies to tell
nextbeat that we no longer need to kick off `cdc_covid_data_smoothed` when
`cdc_covid_data` runs. This eliminates a class of issues related to making sure that the
job dependencies are kept up to date with the actual data dependencies in the code for
these jobs--nextdata takes care of automatically tracking and triggering based on
dependencies.

The third job is a jupyter notebook that reads both `cdc_covid_data` and
`cdc_covid_data_smoothed` and presents the data in a "report". We can schedule the
notebook to get executed and rendered into html:

```python
# from nextbeat_main.py:add_daily_jobs
_notebook(
    notebook_path="covid_data/cdc_covid_report.ipynb",
    context_variables={"date": date},
    output_name=f"cdc_covid_report_{date:%Y-%m-%d}",
    run_on=NextdbDynamicDependency(scope),
)
```

We use `context_variables`/`nextbeat.context.variables` to set the date variable in the
notebook:

```python
# from cdc_covid_report.ipynb
date = nextbeat.context.variables().get("date", datetime.date(2021, 10, 11))
```

(We also plan on supporting papermill for parameterizing notebooks; this requires adding
support for chaining multiple processes together.)


## How to run
This section is a work in progress.

To run this example from the `examples/covid` directory:

If you don't have poetry installed, [install
it](https://python-poetry.org/docs/#installation) first. Then: 

```shell
# one time setup of virtualenv
poetry install

# run these two commands in their own shell
poetry run nextbeat_
poetry run nextrun_ --nextbeat-address localhost:15321

# now simulate using the UI/command line
poetry run python
>>> import nextbeat_main
>>> nextbeat_main.initial_setup()
```

We still need to run the first "database migration" so to speak, which we'll also
simulate doing manually/via command line:

```python

import nextbeat.server.client
from nextbeat.topic_names import pname

client = nextbeat.server.client.NextBeatClientSync()
client.manual_run(
    pname("define_schemas"),
    wait_for_completion=True
)
```

At this point, the covid data-related jobs have been added to the nextbeat server, the
database has been set up, and everything will run on its own automatically. To speed
things up, we can manually run things that would have happened in the past. Continuing
in the same python shell:

```python
import datetime
from nextbeat.jobs import JobRunOverrides

yesterday = datetime.date.today() - datetime.timedelta(days=1)

client.manual_run(
    pname("instantiate_scopes"),
    JobRunOverrides([yesterday, None]),
    wait_for_completion=True
)
# TODO this shouldn't be necessary
client.manual_run(
    pname("cdc_covid_data_smoothed", date=yesterday),
    wait_for_completion=True
)
client.manual_run(
    pname("cdc_covid_report", date=yesterday),
    wait_for_completion=True
)
```

Now we've instantiated the scope for yesterday. If we wait (up to) 2 minutes, the
`cdc_covid_data` job should automatically kick off (as long as it's between 8:30am and
5:30pm NY time). If the CDC has published data for yesterday, the `cdc_covid_data` job
will write the data to nextdb, which will then kick off the `cdc_covid_data_smoothed`
job. (Except in the rare case where the `cdc_covid_data` job ran before we managed to
manually run `cdc_covid_data_smoothed`, then there won't be anything additional to do,
so `cdc_covid_data_smoothed` won't kick off). Once that happens, we should be able to
see the output of `cdc_covid_data_smoothed` in nextdb:

```python
import covid_data.ndb

conn = covid_data.ndb.ndb_test()
conn.read("cdc_covid_data_smoothed").to_pd()
```

This should also result in the `cdc_covid_report` job getting kicked off automatically.
This will produce a report file in `/test_data/reports`.
