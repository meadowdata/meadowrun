from typing import List
import datetime as dt
from meadowrun.aws_integration.management_lambdas.provisioning import Threshold
from meadowrun.aws_integration.ec2_instance_allocation import AllocEC2Instance
from meadowrun.run_job_core import Resources

# Terminate instances if they haven't run any jobs for this long.
TERMINATE_INSTANCES_IF_IDLE_FOR = dt.timedelta(minutes=5)


# Keep instances around to satisfy these thresholds.
INSTANCE_THRESHOLDS: List[Threshold] = []
