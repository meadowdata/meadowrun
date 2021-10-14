import datetime

import pandas as pd
import requests

from covid_data.mdb import mdb_test

# https://data.cdc.gov/Case-Surveillance/United-States-COVID-19-Cases-and-Deaths-by-State-o/9mfq-cb36
_URL_FORMAT = (
    "https://data.cdc.gov/resource/9mfq-cb36.json?"
    "$offset={}&$order=:id&submission_date={}"
)


def cdc_covid_data(date: datetime.date) -> None:
    """
    Gets covid data for the specified date from the CDC website and writes it to
    meadowdb.
    """

    # get data

    # if this was real code we would want to page through the results using the offset
    # parameter in case there's more than 1000 records (or whatever the default limit
    # is)
    url = _URL_FORMAT.format(0, date.strftime("%Y-%m-%d"))
    results = requests.get(url)

    df = pd.DataFrame.from_records(results.json())
    # we didn't get any data, don't do anything else
    if df.empty:
        return

    # parse data

    _DATE_COLUMNS = ["submission_date", "created_at"]
    _INT_COLUMNS = [
        "tot_cases",
        "new_case",
        "pnew_case",
        "tot_death",
        "new_death",
        "pnew_death",
    ]
    # these are actually ints but they have missing data
    _FLOAT_COLUMNS = ["conf_cases", "prob_cases", "conf_death", "prob_death"]
    for date_column in _DATE_COLUMNS:
        try:
            df[date_column] = pd.to_datetime(df[date_column])
        except Exception:
            print(f"Error parsing {date_column}")
            raise
    for int_column in _INT_COLUMNS:
        try:
            # nasty hack
            df[int_column] = df[int_column].astype(float).astype(int)
        except Exception:
            print(f"Error parsing {int_column}")
            raise
    for float_column in _FLOAT_COLUMNS:
        try:
            df[float_column] = df[float_column].astype(float)
        except Exception:
            print(f"Error parsing {float_column}")
            raise

    # dedupe and only write new rows

    mdb_conn = mdb_test()
    # TODO consider making this deduping logic "built-in"
    # TODO this should be a "transaction"--we want to guarantee that nothing writes to
    #  cdc_covid_data between this read and the potential write a few lines down
    t = mdb_conn.read("cdc_covid_data")
    # TODO should be able to compare against dates directly
    # noinspection PyTypeChecker
    curr_df = t[
        t["submission_date"] == datetime.datetime.combine(date, datetime.time())
    ].to_pd()

    if len(curr_df):
        new_rows = curr_df.merge(df, how="right", indicator=True)
        new_rows = new_rows[new_rows["_merge"] == "right_only"].drop(
            labels=["_merge"], axis=1
        )
    else:
        new_rows = df

    if not new_rows.empty:
        # TODO consider making the table name default to the current job's name
        mdb_conn.write("cdc_covid_data", df)
        mdb_conn.table_versions_client._save_table_versions()


def cdc_covid_data_smoothed(date: datetime.date) -> None:
    """
    Computes the 7 day exponential weighted average of the covid data from the CDC
    for the specified date.
    """

    # get the raw data

    mdb_conn = mdb_test()
    t = mdb_conn.read("cdc_covid_data")
    # don't do anything if we don't have any input data
    if t.empty:
        return
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

    # don't do anything if we don't have data for the current date

    # TODO if we didn't need to run once to figure out what our meadowdb table
    #  dependencies are, we wouldn't really need this code--given that we only trigger
    #  on data dependencies, we could be confident that the data we're expecting exists.
    #  This is exacerbated by the fact that we need to detect dependencies every day.
    #  It's probably not the worst thing in the world to have this check, though...
    if df["submission_date"].max() != pd.to_datetime(date):
        # data for today isn't available yet, don't do anything
        return

    # compute 7 day exponentially weighted average

    all_ewms = df.groupby("state").ewm(span=7, adjust=False).mean().reset_index()
    # drop everything other than the most recent day (using level_1 assumes original
    # data was sorted by date)
    ewms = (
        all_ewms.sort_values(["state", "level_1"])
        .drop_duplicates(["state"], keep="last")
        .drop(["level_1"], axis=1)
    )
    ewms["submission_date"] = pd.to_datetime(date)

    # write data if it doesn't exist already

    t = mdb_conn.read("cdc_covid_data_smoothed")
    # noinspection PyTypeChecker
    curr_df = t[
        t["submission_date"] == datetime.datetime.combine(date, datetime.time())
    ].to_pd()

    if len(curr_df):
        new_rows = curr_df.merge(ewms, how="right", indicator=True)
        new_rows = new_rows[new_rows["_merge"] == "right_only"].drop(
            labels=["_merge"], axis=1
        )
    else:
        new_rows = ewms

    if not new_rows.empty:
        mdb_conn.write("cdc_covid_data_smoothed", new_rows)
        mdb_conn.table_versions_client._save_table_versions()
