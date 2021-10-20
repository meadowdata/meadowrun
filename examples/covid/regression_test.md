# Regression Test/Model Change Review
Meadowdb has a concept of userspaces which makes it easy to run a regression test or
model change review for any job without any extra effort.

## How to run

This walkthrough assumes that you have at least skimmed the [README.md](README.md) for
the covid example.

First, let's make a local change to the code that we want to test. In [cdc_covid_data.py
on line 140](covid_data/cdc_covid_data.py#L140), let's "accidentally" introduce a change
that doubles the number of new deaths reported that day by inserting:

```python
ewms["new_death"] *= 2
```

After making this change, we can run [regression_test.py](regression_test.py) and it
will print out the difference between the "base" code (`self` in the output below) which
is the latest committed code in the main branch, and the "test" code (`other` in the
output below) which is the locally modified code:

```
                      new_death       
                           self  other
state submission_date                 
AR    2021-10-17            5.0   10.0
FL    2021-10-17            2.0    4.0
GU    2021-10-17            1.0    2.0
HI    2021-10-17            6.0   12.0
MD    2021-10-17            3.0    6.0
MO    2021-10-17            1.0    2.0
NJ    2021-10-17            5.0   10.0
NY    2021-10-17           31.0   62.0
NYC   2021-10-17            8.0   16.0
TX    2021-10-17           88.0  176.0
```

## Key concepts

At a high level, there are two key concepts that enable us to run a regression test
without any extra effort. Here's the code for kicking off the "base" and "test" runs:

```python
# from regression_test.py
client.manual_run(
    pname("cdc_covid_data_smoothed", date=t0),
    JobRunOverrides(meadowdb_userspace=base_userspace),
    wait_for_completion=True,
)

client.manual_run(
    pname("cdc_covid_data_smoothed", date=t0),
    JobRunOverrides(
        meadowdb_userspace=test_userspace,
        deployment=ServerAvailableFolder(
            code_paths=[current_code], interpreter_path=sys.executable
        ),
    ),
    wait_for_completion=True,
)
```

The first concept is the `meadowdb_userspace` override. This redirects all of the job's
output into the specified userspace, which allows us to isolate that run's outputs for
comparison. It also means that we can be confident that neither run will affect any
production data.

The second concept is the `deployment` override. This runs the job with an alternate
version of the code. The `cdc_covid_data_smoothed` job is defined to use the latest
commit on the main branch of the meadowdata git repo, but for the test run, we want to
use the currently on-disk code that we've modified locally with changes. It would be
just as easy to use a different branch of the same repo or even a different clone of the
repo. 
