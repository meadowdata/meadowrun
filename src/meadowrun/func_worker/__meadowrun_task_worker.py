"""
Like __meadowrun_func_worker, but connections to given agent.
Listens for arguments, executes the function and returns the result.
"""

import asyncio

import importlib  # available in python 3.1+
import argparse  # available in python 3.2+
import pickle
import struct
import traceback
from typing import Callable


async def send_message(
    writer: asyncio.StreamWriter, state: str, result_bytes: bytes, pickle_protocol: int
) -> None:
    # it's important here that the result is pickled separately. If not, the agent's
    # receive_message will unpickle result. This is not only unnecessary, but also
    # potentially doesn't work because the agent doesn't run in the worker's
    # environment.
    msg_bs = pickle.dumps((state, result_bytes), protocol=pickle_protocol)
    msg_len = struct.pack(">i", len(msg_bs))
    writer.write(msg_len)
    writer.write(msg_bs)
    await writer.drain()


async def receive_bytes(reader: asyncio.StreamReader, bytes_len: int) -> bytearray:
    result_bs = bytearray()
    while len(result_bs) < bytes_len:
        result_bs.extend(await reader.read(bytes_len - len(result_bs)))
    return result_bs


def connect_and_do_tasks(
    host: str, port: int, function: Callable, pickle_protocol: int
) -> None:
    # we avoid using asyncio.run here (or in the caller) so that the user `function` can
    # use asyncio.run
    event_loop = asyncio.new_event_loop()

    reader, writer = event_loop.run_until_complete(asyncio.open_connection(host, port))

    try:
        while True:
            arg_size_bs = event_loop.run_until_complete(reader.read(4))
            if len(arg_size_bs) == 0:
                break
            (arg_size,) = struct.unpack(">i", arg_size_bs)

            try:
                if arg_size > 0:
                    arg_bs = event_loop.run_until_complete(
                        receive_bytes(reader, arg_size)
                    )
                    function_args, function_kwargs = pickle.loads(arg_bs)
                else:
                    function_args, function_kwargs = (), {}
                # run the function
                result = function(*(function_args or ()), **(function_kwargs or {}))
                result_bytes = pickle.dumps(result, protocol=pickle_protocol)
            except Exception as e:
                # first print the exception for the local log file
                traceback.print_exc()

                # next, send the exception back
                tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                event_loop.run_until_complete(
                    send_message(
                        writer,
                        "PYTHON_EXCEPTION",
                        pickle.dumps(
                            (str(type(e)), str(e), tb), protocol=pickle_protocol
                        ),
                        pickle_protocol,
                    )
                )
            else:
                # send back results
                event_loop.run_until_complete(
                    send_message(writer, "SUCCEEDED", result_bytes, pickle_protocol)
                )
    finally:
        # close connection
        writer.close()


def get_function(args: argparse.Namespace) -> Callable:
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
        pickled_function_path = args.io_path + ".function"
        print(f"Unpickling function from {pickled_function_path}")
        with open(pickled_function_path, "rb") as f:
            function = pickle.load(f)
    return function


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--module-name")
    parser.add_argument("--function-name")
    parser.add_argument("--io-path", required=True)
    parser.add_argument("--has-pickled-function", action="store_true")
    parser.add_argument("--result-highest-pickle-protocol", type=int, required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)

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

    # prepare filenames and pickle protocol for the result of the agent
    state_filename = args.io_path + ".state"
    result_filename = args.io_path + ".result"

    result_pickle_protocol = min(
        args.result_highest_pickle_protocol, pickle.HIGHEST_PROTOCOL
    )
    try:
        function = get_function(args)
        connect_and_do_tasks(args.host, args.port, function, result_pickle_protocol)

    except Exception as e:
        # first print the exception for the local log file
        traceback.print_exc()

        # next, send the exception back
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        # see MeadowRunClientAsync for why we don't just pickle the exception
        with open(state_filename, "w", encoding="utf-8") as state_text_writer:
            state_text_writer.write("PYTHON_EXCEPTION")
        with open(result_filename, "wb") as f:
            pickle.dump((str(type(e)), str(e), tb), f, protocol=result_pickle_protocol)
    else:
        # send back results
        with open(state_filename, "w", encoding="utf-8") as state_text_writer:
            state_text_writer.write("SUCCEEDED")
        with open(result_filename, "wb") as f:
            pickle.dump("agent exited normally", f, protocol=result_pickle_protocol)


if __name__ == "__main__":
    main()
