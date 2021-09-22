"""
This code gets "embedded" into the user's code, so that the nextrun server can invoke a
function in the user's code base.

This code should therefore have as few dependencies as possible (ideally none).
"""

import importlib  # available in python 3.1+
import argparse  # available in python 3.2+
import pickle
import traceback


def _get_effects():
    try:
        import nextbeat.effects
    except ModuleNotFoundError:
        return None
    else:
        return nextbeat.effects._effects


def main():
    usage = "module_path function_name argument result_highest_pickle_protocol"
    parser = argparse.ArgumentParser()
    parser.add_argument("module_path")
    parser.add_argument("function_name")
    parser.add_argument("argument")
    parser.add_argument("result_highest_pickle_protocol", type=int)
    args = parser.parse_args()

    # import the module

    print(f"About to import {args.module_path}")
    module = importlib.import_module(args.module_path)
    print(f"Imported {args.module_path} from {module.__file__}")

    # read the arguments

    with open(args.argument, "rb") as f:
        # TODO probably should provide nicer error messages on unpickling
        function_args, function_kwargs = pickle.load(f)

    # prepare filenames and pickle protocol for the result

    state_filename = args.argument[: -len(".argument")] + ".state"
    result_filename = args.argument[: -len(".argument")] + ".result"

    result_pickle_protocol = min(
        args.result_highest_pickle_protocol, pickle.HIGHEST_PROTOCOL
    )

    try:
        # run the function
        result = getattr(module, args.function_name)(*function_args, **function_kwargs)
    except Exception as e:
        # send back exceptions
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        # see NextRunClientAsync for why we don't just pickle the exception
        with open(state_filename, "w", encoding="utf-8") as f:
            f.write("PYTHON_EXCEPTION")
        with open(result_filename, "wb") as f:
            # TODO we should potentially be returning effects on failures as well. And
            #  maybe even on unexpected process quitting?
            pickle.dump((str(type(e)), str(e), tb), f, protocol=result_pickle_protocol)
    else:
        # send back results
        with open(state_filename, "w", encoding="utf-8") as f:
            f.write("SUCCEEDED")
        with open(result_filename, "wb") as f:
            pickle.dump((result, _get_effects()), f, protocol=result_pickle_protocol)


if __name__ == "__main__":
    main()
