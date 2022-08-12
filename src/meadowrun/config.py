# TODO turn this into a real module that allows the user to specify configurations
import enum
import string
from typing import Optional

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
GPU = "gpu"
GPU_MEMORY = "gpu_memory"
# in the resources, we set 100 - eviction rate as a non-consumable resource
EVICTION_RATE_INVERSE = "eviction_rate_inverse"
INTEL = "intel"
AMD = "amd"
AVX = "avx"
INTEL_DEEP_LEARNING_BOOST = "intel_deep_learning_boost"


class AvxVersion(enum.IntEnum):
    NONE = 0
    AVX = 1
    AVX2 = 2
    AVX512 = 3


def avx_from_string(s: str) -> Optional[AvxVersion]:
    s = s.upper()
    if s != "NONE":
        try:
            return AvxVersion[s]
        except Exception:
            pass

    return None


# these are totally arbitrary values...
DEFAULT_MEMORY_GB_REQUIRED = 2
DEFAULT_LOGICAL_CPU_REQUIRED = 1


DEFAULT_PRIORITY = 100


# names of environment variables to communicate with the child process

# Will be set in child processes launched by an agent, gives the pid of the agent that
# launched the child process. Mostly for testing/debugging.
MEADOWRUN_AGENT_PID = "MEADOWRUN_AGENT_PID"


# specifies how often EC2 prices should get updated
EC2_PRICES_UPDATE_SECS = 60 * 30  # 30 minutes
