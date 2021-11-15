# TODO turn this into a real module that allows the user to specify configurations
import string

DEFAULT_COORDINATOR_PORT = 15319
DEFAULT_COORDINATOR_HOST = "localhost"
DEFAULT_COORDINATOR_ADDRESS = f"{DEFAULT_COORDINATOR_HOST}:{DEFAULT_COORDINATOR_PORT}"

JOB_ID_VALID_CHARACTERS = set(string.ascii_letters + string.digits + "-_.")


# A placeholder for the same interpreter that meadowgrid.job_worker is using. Mostly for
# testing, not recommended for normal use.
MEADOWGRID_INTERPRETER = "__MEADOWGRID_INTERPRETER__"


# Paths for meadowgrid to bind to inside containers
MEADOWGRID_CODE_MOUNT_LINUX = "/meadowgrid/code"
MEADOWGRID_IO_MOUNT_LINUX = "/meadowgrid/io"
