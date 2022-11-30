# """ A version of __meadowrun_func_worker that is invoked via AWS Lambda. The main
# difference is that arguments and results are not passed on the command line and
# returned via files, but via JSON in HTTP calls.

# We assume this runs in Lambda's python 3.9 environment.
# """

# import base64
# import importlib
# import os
# import pickle
# import subprocess
# import traceback
# from pathlib import Path
# from typing import Any, Dict
# import venv
# from meadowrun.deployment.pack_envs import pack_venv

# from meadowrun.meadowrun_pb2 import Job
# from meadowrun.storage_grid_job import get_aws_s3_bucket, get_aws_s3_bucket_async


# def _pickle_to_str(data: Any, pickle_protocol: int) -> str:
#     return base64.b64encode(pickle.dumps(data, protocol=pickle_protocol)).decode(
#         "ascii"
#     )


# # https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html
# MAX_LAMBDA_DEPLOY_SIZE = 250 * 1000 * 1000


# # async def run_job_main(region_name: str, job: Job) -> None:
# #     if job.WhichOneof("interpreter_deployment") == "environment_spec":
# #         venv_name = "mdr-venv"
# #         venv_path = Path("/tmp") / venv_name
# #         env_spec = job.environment_spec.spec

# #         if not venv_path.exists():
# #             print(f"Creating environment at {venv_path}")
# #             venv.create(venv_path)

# #             spec_path = venv_path / "requirements.txt"
# #             print(f"Writing spec file {spec_path}")
# #             with open(spec_path, "w+", encoding="utf-8") as f:
# #                 f.write(env_spec)

# #             print("Installing requirements")
# #             pip_res = subprocess.run(
# #                 [
# #                     venv_path / "bin" / "python",
# #                     "-m",
# #                     "pip",
# #                     "install",
# #                     "-r",
# #                     spec_path,
# #                 ]
# #             )
# #             assert pip_res.returncode == 0

# #             # TODO optimization - small environments could be installed as a lambda,
# #             # total_venv_size = sum(
# #             #     f.stat().st_size for f in venv_path.glob("**/*") if f.is_file()
# #             # )
# #             # avoiding starting a new process in the environment on every call
# #             # if total_venv_size < MAX_LAMBDA_DEPLOY_SIZE:
# #             #     # upload as zip to s3
# #             #     pass

# #             # venv-pack it
# #             pack_filename = f"/tmp/{venv_name}.zip"
# #             is_packed = pack_venv(str(venv_path), pack_filename, "pip", False)
# #             if not is_packed:
# #                 raise Exception("Could not pack environment")
# #             s3_bucket = get_aws_s3_bucket(region_name)
# #             await s3_bucket.write_file(
# #                 pack_filename,
# #             )


# def lambda_handler(event: Dict[str, Any], content: Any) -> Dict[str, Any]:
#     if "job" not in event:
#         raise ValueError("Must specify 'job'")

#     # region_name = os.environ["AWS_REGION"]

#     job = Job.FromString(base64.b64decode(event["job"]))

#     result_pickle_protocol = min(
#         job.result_highest_pickle_protocol, pickle.HIGHEST_PROTOCOL
#     )
#     # this is validated in lambda_allocation.py
#     py_function = job.py_function

#     result_body = {}

#     try:
#         # import the module/unpickle the function
#         if py_function.WhichOneof("function_spec") == "qualified_function_name":
#             module_name = py_function.qualified_function_name.module_name
#             function_name = py_function.qualified_function_name.function_name
#             print(f"About to import {module_name}.{function_name}")
#             module = importlib.import_module(module_name)
#             function = getattr(module, function_name)
#             print(
#                 f"Imported {function_name} from "
#                 f"{getattr(module, '__file__', str(module))}"
#             )
#         else:
#             function = pickle.loads(py_function.pickled_function)

#         # read the arguments
#         if py_function.pickled_function_arguments:
#             function_args, function_kwargs = pickle.loads(
#                 job.py_function.pickled_function_arguments
#             )
#         else:
#             function_args, function_kwargs = (), {}

#         # run the function
#         result = function(*(function_args or ()), **(function_kwargs or {}))
#         result_body["result"] = _pickle_to_str(result, result_pickle_protocol)
#     except Exception as e:
#         # first print the exception for the local log file
#         traceback.print_exc()

#         # next, send the exception back
#         tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
#         # see MeadowRunClientAsync for why we don't just pickle the exception
#         result_body["state"] = "PYTHON_EXCEPTION"
#         result_body["result"] = _pickle_to_str(
#             (str(type(e)), str(e), tb), result_pickle_protocol
#         )
#     else:
#         # send back results
#         result_body["state"] = "SUCCEEDED"
#         # result already pickled above in the try block

#     # os.environ["AWS_REGION"]))
#     return {"statusCode": 200, "body": result_body}
