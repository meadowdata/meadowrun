# TODO turn this into a real module that allows the user to specify configurations
import string

JOB_ID_VALID_CHARACTERS = set(string.ascii_letters + string.digits + "-_.")


# A placeholder for the same interpreter that meadowrun.agent is using. Mostly for
# testing, not recommended for normal use.
MEADOWRUN_INTERPRETER = "__MEADOWRUN_INTERPRETER__"


# Paths for meadowrun to bind to inside containers
MEADOWRUN_CODE_MOUNT_LINUX = "/meadowrun/code"
MEADOWRUN_IO_MOUNT_LINUX = "/meadowrun/io"


# names of resources and default values
MEMORY_GB = "memory_gb"
LOGICAL_CPU = "logical_cpu"
# these are totally arbitrary values...
DEFAULT_MEMORY_GB_REQUIRED = 2
DEFAULT_LOGICAL_CPU_REQUIRED = 1


DEFAULT_PRIORITY = 100
DEFAULT_INTERRUPTION_PROBABILITY_THRESHOLD = 15


# names of environment variables to communicate with the child process

# Will be set in child processes launched by an agent, gives the pid of the agent that
# launched the child process. Mostly for testing/debugging.
MEADOWRUN_AGENT_PID = "MEADOWRUN_AGENT_PID"


# specifies how often EC2 prices should get updated
EC2_PRICES_UPDATE_SECS = 60 * 30  # 30 minutes
