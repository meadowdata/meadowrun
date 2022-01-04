# TODO turn this into a real module that allows the user to specify configurations
import string

DEFAULT_COORDINATOR_PORT = 15319
DEFAULT_COORDINATOR_HOST = "localhost"
DEFAULT_COORDINATOR_ADDRESS = f"{DEFAULT_COORDINATOR_HOST}:{DEFAULT_COORDINATOR_PORT}"

JOB_ID_VALID_CHARACTERS = set(string.ascii_letters + string.digits + "-_.")


# A placeholder for the same interpreter that meadowgrid.agent is using. Mostly for
# testing, not recommended for normal use.
MEADOWGRID_INTERPRETER = "__MEADOWGRID_INTERPRETER__"


# Paths for meadowgrid to bind to inside containers
MEADOWGRID_CODE_MOUNT_LINUX = "/meadowgrid/code"
MEADOWGRID_IO_MOUNT_LINUX = "/meadowgrid/io"


# names of resources and default values
MEMORY_GB = "memory_gb"
LOGICAL_CPU = "logical_cpu"
# these are totally arbitrary values...
DEFAULT_MEMORY_GB_REQUIRED = 2
DEFAULT_LOGICAL_CPU_REQUIRED = 1


DEFAULT_PRIORITY = 100


# names of environment variables to communicate with the child process

# Will be set in child processes launched by an agent, gives the pid of the agent that
# launched the child process. Mostly for testing/debugging.
MEADOWGRID_AGENT_PID = "MEADOWGRID_AGENT_PID"
