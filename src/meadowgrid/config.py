# TODO turn this into a real module that allows the user to specify configurations
import string

DEFAULT_COORDINATOR_PORT = 15319
DEFAULT_COORDINATOR_HOST = "localhost"
DEFAULT_COORDINATOR_ADDRESS = f"{DEFAULT_COORDINATOR_HOST}:{DEFAULT_COORDINATOR_PORT}"

JOB_ID_VALID_CHARACTERS = set(string.ascii_letters + string.digits + "-_.")
