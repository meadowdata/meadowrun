"""
This is an S3-based version of __meadowrun_func_worker.py. __meadowrun_func_worker.py
assumes very few dependencies and assumes that the input/output files are available on a
locally available filesystem. This version is equivalent to __meadowrun_func_worker.py,
but assumes that input/output files are available on an S3-compatible object storage
system. Because this file needs to access the S3-compatible storage, this file assumes a
full Meadowrun installation is available.
"""

import argparse
import importlib
import io
import logging
import os
import pickle
import traceback
from typing import Optional

import meadowrun.func_worker_storage_helper
from meadowrun.func_worker_storage_helper import (
    MEADOWRUN_STORAGE_PASSWORD,
    MEADOWRUN_STORAGE_USERNAME,
    get_storage_client_from_args,
)


def main() -> None:
    global STORAGE_CLIENT

    logging.basicConfig(level=logging.INFO)

    # parse arguments

    parser = argparse.ArgumentParser()

    # arguments for determining the function
    parser.add_argument("--module-name")
    parser.add_argument("--function-name")
    parser.add_argument("--has-pickled-function", action="store_true")
    parser.add_argument("--has-pickled-arguments", action="store_true")
    parser.add_argument("--result-highest-pickle-protocol", type=int, required=True)

    # arguments for finding the storage bucket (i.e. S3-compatible store) where we can
    # find inputs and put outputs. These are optional.
    parser.add_argument("--storage-bucket")
    parser.add_argument("--storage-file-prefix", required=True)
    parser.add_argument("--storage-endpoint-url")

    args = parser.parse_args()

    if bool(args.module_name) ^ bool(args.function_name):
        raise ValueError(
            "Cannot specify just one of --module-name and --function-name without the "
            "other"
        )
    if not (bool(args.module_name) ^ args.has_pickled_function):
        raise ValueError(
            "Must specify either --module-name with --function-name OR "
            "--has-pickled-function but not both"
        )

    storage_bucket: Optional[str] = args.storage_bucket
    storage_file_prefix: str = args.storage_file_prefix

    if (
        args.has_pickled_function or args.has_pickled_arguments
    ) and storage_bucket is None:
        raise ValueError(
            "Cannot specify --has-pickled-function or --has-pickled-arguments without "
            "also providing a --storage-bucket"
        )

    # prepare storage client, filenames and pickle protocol for the result
    if storage_bucket is None:
        storage_client = None
    else:
        storage_username = os.environ.get(MEADOWRUN_STORAGE_USERNAME, None)
        storage_password = os.environ.get(MEADOWRUN_STORAGE_PASSWORD, None)
        storage_client = get_storage_client_from_args(
            args.storage_endpoint_url, storage_username, storage_password
        )
    meadowrun.func_worker_storage_helper.STORAGE_CLIENT = storage_client

    suffix = os.environ.get("JOB_COMPLETION_INDEX", "")
    state_filename = f"{storage_file_prefix}.state{suffix}"
    result_filename = f"{storage_file_prefix}.result{suffix}"

    result_pickle_protocol = min(
        args.result_highest_pickle_protocol, pickle.HIGHEST_PROTOCOL
    )

    try:
        # import the module/unpickle the function

        if args.module_name is not None:
            print(f"About to import {args.module_name}.{args.function_name}")
            module = importlib.import_module(args.module_name)
            function = getattr(module, args.function_name)
            print(
                f"Imported {args.function_name} from "
                f"{getattr(module, '__file__', str(module))}"
            )
        else:
            pickled_function_filename = storage_file_prefix + ".function"
            print(
                f"Downloading function from {storage_bucket}/"
                f"{pickled_function_filename}"
            )

            assert storage_client is not None  # just for mypy
            with io.BytesIO() as buffer:
                storage_client.download_fileobj(
                    Bucket=storage_bucket, Key=pickled_function_filename, Fileobj=buffer
                )
                buffer.seek(0)
                function = pickle.load(buffer)

        # read the arguments

        if args.has_pickled_arguments:
            assert storage_client is not None  # just for mypy
            with io.BytesIO() as buffer:
                storage_client.download_fileobj(
                    Bucket=storage_bucket,
                    Key=storage_file_prefix + ".arguments",
                    Fileobj=buffer,
                )
                buffer.seek(0)
                function_args, function_kwargs = pickle.load(buffer)
        else:
            function_args, function_kwargs = (), {}

        # run the function
        result = function(*(function_args or ()), **(function_kwargs or {}))
    except Exception as e:
        # first print the exception for the local log file
        traceback.print_exc()

        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))

        # next, send the exception back if we can

        if storage_client is None:
            print(
                "Warning, failed but not sending back results because --storage-bucket "
                "was not specified"
            )
            raise

        # see MeadowRunClientAsync for why we don't just pickle the exception
        with io.BytesIO() as buffer:
            buffer.write("PYTHON_EXCEPTION".encode("utf-8"))
            buffer.seek(0)
            storage_client.upload_fileobj(
                Fileobj=buffer, Bucket=storage_bucket, Key=state_filename
            )
        with io.BytesIO() as buffer:
            pickle.dump(
                (str(type(e)), str(e), tb), buffer, protocol=result_pickle_protocol
            )
            buffer.seek(0)
            storage_client.upload_fileobj(
                Fileobj=buffer, Bucket=storage_bucket, Key=result_filename
            )
    else:
        if storage_client is None:
            print(
                "Warning, succeeded but not sending back results because "
                "--storage-bucket was not specified"
            )
        else:
            # send back results
            with io.BytesIO() as buffer:
                buffer.write("SUCCEEDED".encode("utf-8"))
                buffer.seek(0)
                storage_client.upload_fileobj(
                    Fileobj=buffer, Bucket=storage_bucket, Key=state_filename
                )
            with io.BytesIO() as buffer:
                pickle.dump(result, buffer, protocol=result_pickle_protocol)
                buffer.seek(0)
                storage_client.upload_fileobj(
                    Fileobj=buffer, Bucket=storage_bucket, Key=result_filename
                )


if __name__ == "__main__":
    main()
