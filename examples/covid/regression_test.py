"""
This script shows how to run a regression test on any meadowflow job, using the
cdc_covid_data_smoothed job as an example.
"""

import datetime
import os.path
import uuid

from meadowgrid.meadowgrid_pb2 import ServerAvailableFolder

import covid_data
from covid_data.mdb import mdb_test
from meadowflow.jobs import JobRunOverrides
import meadowflow.server.client
from meadowflow.topic_names import pname


def main():
    # TODO incorporate this into the core meadowdata code.

    client = meadowflow.server.client.MeadowFlowClientSync()

    # the date we're running the test for
    t0 = datetime.date.today() - datetime.timedelta(days=2)

    # create new unique userspace names for the BASE run and the TEST run
    # TODO automatically clean these up
    userspace = str(uuid.uuid4())
    base_userspace = f"main,BASE_{userspace}"
    test_userspace = f"main,TEST_{userspace}"

    # The cdc_covid_data_smoothed job won't write duplicate data, so the first thing we
    # do is clear the output data in the regression test userspaces.
    # TODO this shouldn't have to be specified manually, we could e.g. pull it from past
    #  runs of the job
    conn = mdb_test()
    conn.delete_all("cdc_covid_data_smoothed", base_userspace)
    conn.delete_all("cdc_covid_data_smoothed", test_userspace)
    conn.table_versions_client._save_table_versions()

    # Next, we run the existing job as-is but with an override that redirects all writes
    # to base_userspace. This means we can be confident that our runs for the regression
    # test won't affect any production data. The job is defined to use the latest commit
    # on the main branch of the current git repo, so it will use the latest committed
    # code.
    client.manual_run(
        pname("cdc_covid_data_smoothed", date=t0),
        JobRunOverrides(meadowdb_userspace=base_userspace),
        wait_for_completion=True,
    )

    # Next, we'll run the job with an override to redirect all writes to test_userspace
    # but also to use different code that has the changes we want to test. In this case,
    # we're specifying the local code as it currently exists on disk, assuming we've
    # made some change that we want to test.
    current_code = os.path.join(covid_data.ROOT_DIR, "examples", "covid")
    client.manual_run(
        pname("cdc_covid_data_smoothed", date=t0),
        JobRunOverrides(
            meadowdb_userspace=test_userspace,
            deployment=ServerAvailableFolder(code_paths=[current_code]),
        ),
        wait_for_completion=True,
    )

    # Finally, we can compare the output data from our test runs
    # TODO ideally we would get a list of tables that had been written to
    conn = mdb_test(reset=True)
    base_data = conn.read("cdc_covid_data_smoothed", base_userspace).to_pd()
    test_data = conn.read("cdc_covid_data_smoothed", test_userspace).to_pd()

    index_columns = ["state", "submission_date"]
    print(
        base_data.set_index(index_columns).compare(test_data.set_index(index_columns))
    )


if __name__ == "__main__":
    main()
