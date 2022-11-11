# a dynamodb table that holds information about EC2 instances we've created and what has
# been allocated to which instances
_EC2_ALLOC_TABLE_NAME = "meadowrun_ec2_alloc_table"

# names of attributes/keys in the EC2 alloc table
_PUBLIC_ADDRESS = "public_address"
_INSTANCE_ID = "instance_id"
_RESOURCES_AVAILABLE = "resources_available"
_NON_CONSUMABLE_RESOURCES = "non_consumable_resources"
_RESOURCES_ALLOCATED = "resources_allocated"
_ALLOCATED_TIME = "allocated_time"
_LAST_UPDATE_TIME = "last_update_time"
_PREVENT_FURTHER_ALLOCATION = "prevent_further_allocation"
_RUNNING_JOBS = "running_jobs"
_JOB_ID = "job_id"

# A tag for EC2 instances that are created using ec2_alloc
_MEADOWRUN_TAG = "meadowrun"
_MEADOWRUN_TAG_VALUE = "TRUE"

_MEADOWRUN_GENERATED_DOCKER_REPO = "meadowrun_generated"

MACHINE_AGENT_QUEUE_PREFIX = "meadowrun-machine-"
