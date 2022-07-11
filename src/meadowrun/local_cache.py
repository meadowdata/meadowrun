import datetime
import json
import os
import time
from typing import Any, Optional

from meadowrun._vendor.platformdirs import PlatformDirs


MEADOWRUN_DIRS = PlatformDirs(appname="meadowrun", appauthor=False)


def get_cached_json(name: str, freshness: datetime.timedelta) -> Optional[Any]:
    """Returns the cached JSON file from the standard cache dir,
    if it is not too old.

    Args:
        name (str): The name of the file. (not a path)
        freshness (timedelta): The maximum age of the file.

    Returns:
        Optional[Any]: The JSON data, or None if the file is too old.
    """
    file = os.path.join(MEADOWRUN_DIRS.user_cache_dir, name)
    if (
        not os.path.exists(file)
        or os.path.getmtime(file) + freshness.total_seconds() < time.time()
    ):
        return None

    with open(file, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json_to_cache(name: str, json_data: Any) -> None:
    """Saves the JSON data to the standard cache dir.

    Args:
        name (str): The name of the file. (not a path)
        json_data (Any): The JSON data.
    """
    if not os.path.exists(MEADOWRUN_DIRS.user_cache_dir):
        os.makedirs(MEADOWRUN_DIRS.user_cache_dir, exist_ok=True)
    file = os.path.join(MEADOWRUN_DIRS.user_cache_dir, name)
    with open(file, "w", encoding="utf-8") as f:
        json.dump(json_data, f)


def clear_cache(name: str) -> None:
    try:
        os.remove(os.path.join(MEADOWRUN_DIRS.user_cache_dir, name))
    except OSError:
        pass
