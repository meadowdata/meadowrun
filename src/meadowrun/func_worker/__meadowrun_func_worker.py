"""
This code gets "embedded" into the user's code, so that the meadowrun server can invoke
a function in the user's code base.

This code should therefore have as few dependencies as possible (ideally none).

Lots of parallels to grid_worker.py
"""

import importlib  # available in python 3.1+
import argparse  # available in python 3.2+
import pickle
import traceback


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--module-name")
    parser.add_argument("--function-name")
    parser.add_argument("--io-path", required=True)
    parser.add_argument("--has-pickled-function", action="store_true")
    parser.add_argument("--has-pickled-arguments", action="store_true")
    parser.add_argument("--result-highest-pickle-protocol", type=int, required=True)

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

    # prepare filenames and pickle protocol for the result

    state_filename = args.io_path + ".state"
    result_filename = args.io_path + ".result"

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
            pickled_function_path = args.io_path + ".function"
            print(f"Unpickling function from {pickled_function_path}")
            with open(pickled_function_path, "rb") as f:
                function = pickle.load(f)

        # read the arguments

        if args.has_pickled_arguments:
            with open(args.io_path + ".arguments", "rb") as f:
                # TODO probably should provide nicer error messages on unpickling
                function_args, function_kwargs = pickle.load(f)
        else:
            function_args, function_kwargs = (), {}

        # run the function
        result = function(*(function_args or ()), **(function_kwargs or {}))
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
            pickle.dump(result, f, protocol=result_pickle_protocol)


if __name__ == "__main__":
    main()
