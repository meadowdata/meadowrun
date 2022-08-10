import os
import uuid
from datetime import timedelta

from meadowrun.local_cache import MEADOWRUN_DIRS, get_cached_json, save_json_to_cache


def test_json_cache() -> None:
    # def get_cached_json(name: str, freshness: timedelta) -> Optional[Any]:
    # def save_json_to_cache(name: str, json_data: Any) -> None:
    try:
        filename = str(uuid.uuid4())
        assert get_cached_json(filename, timedelta(seconds=60)) is None
        json_data = {"foo": "bar"}
        save_json_to_cache(filename, json_data)
        assert get_cached_json(filename, timedelta(seconds=60)) == json_data
        assert get_cached_json(filename, timedelta(seconds=-1)) is None
        # check we can overwrite the file
        save_json_to_cache(filename, json_data)
        assert get_cached_json(filename, timedelta(seconds=60)) == json_data
    finally:
        os.remove(os.path.join(MEADOWRUN_DIRS.user_cache_dir, filename))
