# Cache data on the underlying machine

Your Meadowrun code will usually run in a container, which means that it will not be
able to write files to the underlying file system. Meadowrun provides
`meadowrun.MACHINE_CACHE_FOLDER` which points to `/var/meadowrun/machine_cache` when
running on EC2 or Azure VMs. You can write data to this folder and it will be visible to
any jobs that happen to run on the same node.

The general pattern for using this folder should be something like:

```python
import os.path
import filelock
import meadowrun


def a_slow_computation():
    return "some sample data"


def get_cached_data():
    cached_data_filename = os.path.join(meadowrun.MACHINE_CACHE_FOLDER, "myfile")

    with filelock.FileLock(f"{cached_data_filename}.lock"):
        if not os.path.exists(cached_data_filename):
            data = a_slow_computation()
            with open(cached_data_filename, "w") as cached_data_file:
                cached_data_file.write(data)

            return data
        else:
            with open(cached_data_filename, "r") as cached_data_file:
                return cached_data_file.read()
```

`filelock` is a [library](https://py-filelock.readthedocs.io/en/latest/) which makes
sure only one process at a time is writing to the specified file. You're welcome to use
whatever locking mechanism you like, but you should never assume that your job is the
only process running on the machine.

You should also never assume that any data you wrote will be available for a subsequent
job. Meadowrun does not provide a way to guarantee that two jobs will run on the same
machine.
