import datetime
import glob
import json
import os
import time
from typing import Any, Dict, Optional, Tuple

if "AWS_LAMBDA_FUNCTION_NAME" in os.environ:
    # platformdirs doesn't work on lambda, and isn't useful -
    # we're better off caching in memory
    use_platformdirs = False
else:
    use_platformdirs = True


if use_platformdirs:
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
        file = MEADOWRUN_DIRS.user_cache_path / name
        if (
            not file.exists()
            or file.stat().st_mtime + freshness.total_seconds() < time.time()
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
        path = MEADOWRUN_DIRS.user_cache_path
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
        file = path / name
        with open(file, "w", encoding="utf-8") as f:
            json.dump(json_data, f)

    def clear_cache(prefix: str) -> None:
        try:
            for file in glob.glob(
                os.path.join(MEADOWRUN_DIRS.user_cache_dir, f"{prefix}*")
            ):
                os.remove(file)
        except OSError:
            pass

else:
    # We are in an AWS lambda, store in memory
    cached_files: Dict[str, Tuple[float, Any]] = {}

    def get_cached_json(name: str, freshness: datetime.timedelta) -> Optional[Any]:
        """Returns the cached JSON file from the in-memory cache,
        if it is not too old.

        Args:
            name (str): The name of the file. (not a path)
            freshness (timedelta): The maximum age of the file.

        Returns:
            Optional[Any]: The JSON data, or None if the file is too old.
        """
        global cached_files
        cached_file = cached_files.get(name)
        if (
            cached_file is None
            or cached_file[0] + freshness.total_seconds() < time.time()
        ):
            return None

        return cached_file[1]

    def save_json_to_cache(name: str, json_data: Any) -> None:
        """Saves the JSON data to the in-memory cache.

        Args:
            name (str): The name of the file. (not a path)
            json_data (Any): The JSON data.
        """
        global cached_files
        cached_files[name] = (time.time(), json_data)

    def clear_cache(prefix: str) -> None:
        global cached_files
        cached_files.clear()
