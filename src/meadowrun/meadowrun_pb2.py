# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: meadowrun/meadowrun.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x19meadowrun/meadowrun.proto\x12\tmeadowrun"(\n\nStringPair\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t"+\n\x15ServerAvailableFolder\x12\x12\n\ncode_paths\x18\x01 \x03(\t"G\n\rGitRepoCommit\x12\x10\n\x08repo_url\x18\x01 \x01(\t\x12\x0e\n\x06\x63ommit\x18\x02 \x01(\t\x12\x14\n\x0cpath_in_repo\x18\x03 \x01(\t"G\n\rGitRepoBranch\x12\x10\n\x08repo_url\x18\x01 \x01(\t\x12\x0e\n\x06\x62ranch\x18\x02 \x01(\t\x12\x14\n\x0cpath_in_repo\x18\x03 \x01(\t"6\n\x1aServerAvailableInterpreter\x12\x18\n\x10interpreter_path\x18\x01 \x01(\t"7\n\x11\x43ontainerAtDigest\x12\x12\n\nrepository\x18\x01 \x01(\t\x12\x0e\n\x06\x64igest\x18\x02 \x01(\t"1\n\x0e\x43ontainerAtTag\x12\x12\n\nrepository\x18\x01 \x01(\t\x12\x0b\n\x03tag\x18\x02 \x01(\t".\n\x18ServerAvailableContainer\x12\x12\n\nimage_name\x18\x01 \x01(\t"G\n\x0cPyCommandJob\x12\x14\n\x0c\x63ommand_line\x18\x01 \x03(\t\x12!\n\x19pickled_context_variables\x18\x02 \x01(\x0c"C\n\x15QualifiedFunctionName\x12\x13\n\x0bmodule_name\x18\x01 \x01(\t\x12\x15\n\rfunction_name\x18\x02 \x01(\t"\xa5\x01\n\rPyFunctionJob\x12\x43\n\x17qualified_function_name\x18\x01 \x01(\x0b\x32 .meadowrun.QualifiedFunctionNameH\x00\x12\x1a\n\x10pickled_function\x18\x02 \x01(\x0cH\x00\x12"\n\x1apickled_function_arguments\x18\x03 \x01(\x0c\x42\x0f\n\rfunction_spec"?\n\x08GridTask\x12\x0f\n\x07task_id\x18\x01 \x01(\x05\x12"\n\x1apickled_function_arguments\x18\x02 \x01(\x0c"\'\n\x08Resource\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x02"\xe4\x05\n\x03Job\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x19\n\x11job_friendly_name\x18\x02 \x01(\t\x12\x43\n\x17server_available_folder\x18\x05 \x01(\x0b\x32 .meadowrun.ServerAvailableFolderH\x00\x12\x33\n\x0fgit_repo_commit\x18\x06 \x01(\x0b\x32\x18.meadowrun.GitRepoCommitH\x00\x12\x33\n\x0fgit_repo_branch\x18\x07 \x01(\x0b\x32\x18.meadowrun.GitRepoBranchH\x00\x12M\n\x1cserver_available_interpreter\x18\x08 \x01(\x0b\x32%.meadowrun.ServerAvailableInterpreterH\x01\x12;\n\x13\x63ontainer_at_digest\x18\t \x01(\x0b\x32\x1c.meadowrun.ContainerAtDigestH\x01\x12\x35\n\x10\x63ontainer_at_tag\x18\n \x01(\x0b\x32\x19.meadowrun.ContainerAtTagH\x01\x12I\n\x1aserver_available_container\x18\x0b \x01(\x0b\x32#.meadowrun.ServerAvailableContainerH\x01\x12\x34\n\x15\x65nvironment_variables\x18\x0c \x03(\x0b\x32\x15.meadowrun.StringPair\x12&\n\x1eresult_highest_pickle_protocol\x18\r \x01(\x05\x12-\n\npy_command\x18\x0f \x01(\x0b\x32\x17.meadowrun.PyCommandJobH\x02\x12/\n\x0bpy_function\x18\x10 \x01(\x0b\x32\x18.meadowrun.PyFunctionJobH\x02\x42\x11\n\x0f\x63ode_deploymentB\x18\n\x16interpreter_deploymentB\n\n\x08job_spec"g\n\tJobToRun2\x12\x1b\n\x03job\x18\x01 \x01(\x0b\x32\x0e.meadowrun.Job\x12=\n\x13\x63redentials_sources\x18\x02 \x03(\x0b\x32 .meadowrun.AddCredentialsRequest"\x8a\x03\n\x0cProcessState\x12\x37\n\x05state\x18\x01 \x01(\x0e\x32(.meadowrun.ProcessState.ProcessStateEnum\x12\x0b\n\x03pid\x18\x02 \x01(\x05\x12\x14\n\x0c\x63ontainer_id\x18\x03 \x01(\t\x12\x15\n\rlog_file_name\x18\x04 \x01(\t\x12\x16\n\x0epickled_result\x18\x05 \x01(\x0c\x12\x13\n\x0breturn_code\x18\x06 \x01(\x05"\xd9\x01\n\x10ProcessStateEnum\x12\x0b\n\x07\x44\x45\x46\x41ULT\x10\x00\x12\x11\n\rRUN_REQUESTED\x10\x01\x12\x0b\n\x07RUNNING\x10\x02\x12\r\n\tSUCCEEDED\x10\x03\x12\x16\n\x12RUN_REQUEST_FAILED\x10\x04\x12\x14\n\x10PYTHON_EXCEPTION\x10\x05\x12\x18\n\x14NON_ZERO_RETURN_CODE\x10\x06\x12\x1b\n\x17RESOURCES_NOT_AVAILABLE\x10\x07\x12\x17\n\x13\x45RROR_GETTING_STATE\x10\x08\x12\x0b\n\x07UNKNOWN\x10\t"h\n\x0eJobStateUpdate\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x16\n\x0egrid_worker_id\x18\x02 \x01(\t\x12.\n\rprocess_state\x18\x03 \x01(\x0b\x32\x17.meadowrun.ProcessState"X\n\x15GridTaskStateResponse\x12\x0f\n\x07task_id\x18\x01 \x01(\x05\x12.\n\rprocess_state\x18\x02 \x01(\x0b\x32\x17.meadowrun.ProcessState"O\n\x16GridTaskStatesResponse\x12\x35\n\x0btask_states\x18\x01 \x03(\x0b\x32 .meadowrun.GridTaskStateResponse"\xd4\x01\n\x15\x41\x64\x64\x43redentialsRequest\x12/\n\x07service\x18\x01 \x01(\x0e\x32\x1e.meadowrun.Credentials.Service\x12\x13\n\x0bservice_url\x18\x02 \x01(\t\x12*\n\naws_secret\x18\x03 \x01(\x0b\x32\x14.meadowrun.AwsSecretH\x00\x12?\n\x15server_available_file\x18\x04 \x01(\x0b\x32\x1e.meadowrun.ServerAvailableFileH\x00\x42\x08\n\x06source"\x95\x01\n\x0b\x43redentials\x12\x13\n\x0b\x63redentials\x18\x01 \x01(\x0c"3\n\x07Service\x12\x13\n\x0f\x44\x45\x46\x41ULT_SERVICE\x10\x00\x12\n\n\x06\x44OCKER\x10\x01\x12\x07\n\x03GIT\x10\x02"<\n\x04Type\x12\x10\n\x0c\x44\x45\x46\x41ULT_TYPE\x10\x00\x12\x15\n\x11USERNAME_PASSWORD\x10\x01\x12\x0b\n\x07SSH_KEY\x10\x02"W\n\tAwsSecret\x12\x35\n\x10\x63redentials_type\x18\x01 \x01(\x0e\x32\x1b.meadowrun.Credentials.Type\x12\x13\n\x0bsecret_name\x18\x02 \x01(\t"Z\n\x13ServerAvailableFile\x12\x35\n\x10\x63redentials_type\x18\x01 \x01(\x0e\x32\x1b.meadowrun.Credentials.Type\x12\x0c\n\x04path\x18\x02 \x01(\tb\x06proto3'
)


_STRINGPAIR = DESCRIPTOR.message_types_by_name["StringPair"]
_SERVERAVAILABLEFOLDER = DESCRIPTOR.message_types_by_name["ServerAvailableFolder"]
_GITREPOCOMMIT = DESCRIPTOR.message_types_by_name["GitRepoCommit"]
_GITREPOBRANCH = DESCRIPTOR.message_types_by_name["GitRepoBranch"]
_SERVERAVAILABLEINTERPRETER = DESCRIPTOR.message_types_by_name[
    "ServerAvailableInterpreter"
]
_CONTAINERATDIGEST = DESCRIPTOR.message_types_by_name["ContainerAtDigest"]
_CONTAINERATTAG = DESCRIPTOR.message_types_by_name["ContainerAtTag"]
_SERVERAVAILABLECONTAINER = DESCRIPTOR.message_types_by_name["ServerAvailableContainer"]
_PYCOMMANDJOB = DESCRIPTOR.message_types_by_name["PyCommandJob"]
_QUALIFIEDFUNCTIONNAME = DESCRIPTOR.message_types_by_name["QualifiedFunctionName"]
_PYFUNCTIONJOB = DESCRIPTOR.message_types_by_name["PyFunctionJob"]
_GRIDTASK = DESCRIPTOR.message_types_by_name["GridTask"]
_RESOURCE = DESCRIPTOR.message_types_by_name["Resource"]
_JOB = DESCRIPTOR.message_types_by_name["Job"]
_JOBTORUN2 = DESCRIPTOR.message_types_by_name["JobToRun2"]
_PROCESSSTATE = DESCRIPTOR.message_types_by_name["ProcessState"]
_JOBSTATEUPDATE = DESCRIPTOR.message_types_by_name["JobStateUpdate"]
_GRIDTASKSTATERESPONSE = DESCRIPTOR.message_types_by_name["GridTaskStateResponse"]
_GRIDTASKSTATESRESPONSE = DESCRIPTOR.message_types_by_name["GridTaskStatesResponse"]
_ADDCREDENTIALSREQUEST = DESCRIPTOR.message_types_by_name["AddCredentialsRequest"]
_CREDENTIALS = DESCRIPTOR.message_types_by_name["Credentials"]
_AWSSECRET = DESCRIPTOR.message_types_by_name["AwsSecret"]
_SERVERAVAILABLEFILE = DESCRIPTOR.message_types_by_name["ServerAvailableFile"]
_PROCESSSTATE_PROCESSSTATEENUM = _PROCESSSTATE.enum_types_by_name["ProcessStateEnum"]
_CREDENTIALS_SERVICE = _CREDENTIALS.enum_types_by_name["Service"]
_CREDENTIALS_TYPE = _CREDENTIALS.enum_types_by_name["Type"]
StringPair = _reflection.GeneratedProtocolMessageType(
    "StringPair",
    (_message.Message,),
    {
        "DESCRIPTOR": _STRINGPAIR,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.StringPair)
    },
)
_sym_db.RegisterMessage(StringPair)

ServerAvailableFolder = _reflection.GeneratedProtocolMessageType(
    "ServerAvailableFolder",
    (_message.Message,),
    {
        "DESCRIPTOR": _SERVERAVAILABLEFOLDER,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.ServerAvailableFolder)
    },
)
_sym_db.RegisterMessage(ServerAvailableFolder)

GitRepoCommit = _reflection.GeneratedProtocolMessageType(
    "GitRepoCommit",
    (_message.Message,),
    {
        "DESCRIPTOR": _GITREPOCOMMIT,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.GitRepoCommit)
    },
)
_sym_db.RegisterMessage(GitRepoCommit)

GitRepoBranch = _reflection.GeneratedProtocolMessageType(
    "GitRepoBranch",
    (_message.Message,),
    {
        "DESCRIPTOR": _GITREPOBRANCH,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.GitRepoBranch)
    },
)
_sym_db.RegisterMessage(GitRepoBranch)

ServerAvailableInterpreter = _reflection.GeneratedProtocolMessageType(
    "ServerAvailableInterpreter",
    (_message.Message,),
    {
        "DESCRIPTOR": _SERVERAVAILABLEINTERPRETER,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.ServerAvailableInterpreter)
    },
)
_sym_db.RegisterMessage(ServerAvailableInterpreter)

ContainerAtDigest = _reflection.GeneratedProtocolMessageType(
    "ContainerAtDigest",
    (_message.Message,),
    {
        "DESCRIPTOR": _CONTAINERATDIGEST,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.ContainerAtDigest)
    },
)
_sym_db.RegisterMessage(ContainerAtDigest)

ContainerAtTag = _reflection.GeneratedProtocolMessageType(
    "ContainerAtTag",
    (_message.Message,),
    {
        "DESCRIPTOR": _CONTAINERATTAG,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.ContainerAtTag)
    },
)
_sym_db.RegisterMessage(ContainerAtTag)

ServerAvailableContainer = _reflection.GeneratedProtocolMessageType(
    "ServerAvailableContainer",
    (_message.Message,),
    {
        "DESCRIPTOR": _SERVERAVAILABLECONTAINER,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.ServerAvailableContainer)
    },
)
_sym_db.RegisterMessage(ServerAvailableContainer)

PyCommandJob = _reflection.GeneratedProtocolMessageType(
    "PyCommandJob",
    (_message.Message,),
    {
        "DESCRIPTOR": _PYCOMMANDJOB,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.PyCommandJob)
    },
)
_sym_db.RegisterMessage(PyCommandJob)

QualifiedFunctionName = _reflection.GeneratedProtocolMessageType(
    "QualifiedFunctionName",
    (_message.Message,),
    {
        "DESCRIPTOR": _QUALIFIEDFUNCTIONNAME,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.QualifiedFunctionName)
    },
)
_sym_db.RegisterMessage(QualifiedFunctionName)

PyFunctionJob = _reflection.GeneratedProtocolMessageType(
    "PyFunctionJob",
    (_message.Message,),
    {
        "DESCRIPTOR": _PYFUNCTIONJOB,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.PyFunctionJob)
    },
)
_sym_db.RegisterMessage(PyFunctionJob)

GridTask = _reflection.GeneratedProtocolMessageType(
    "GridTask",
    (_message.Message,),
    {
        "DESCRIPTOR": _GRIDTASK,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.GridTask)
    },
)
_sym_db.RegisterMessage(GridTask)

Resource = _reflection.GeneratedProtocolMessageType(
    "Resource",
    (_message.Message,),
    {
        "DESCRIPTOR": _RESOURCE,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.Resource)
    },
)
_sym_db.RegisterMessage(Resource)

Job = _reflection.GeneratedProtocolMessageType(
    "Job",
    (_message.Message,),
    {
        "DESCRIPTOR": _JOB,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.Job)
    },
)
_sym_db.RegisterMessage(Job)

JobToRun2 = _reflection.GeneratedProtocolMessageType(
    "JobToRun2",
    (_message.Message,),
    {
        "DESCRIPTOR": _JOBTORUN2,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.JobToRun2)
    },
)
_sym_db.RegisterMessage(JobToRun2)

ProcessState = _reflection.GeneratedProtocolMessageType(
    "ProcessState",
    (_message.Message,),
    {
        "DESCRIPTOR": _PROCESSSTATE,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.ProcessState)
    },
)
_sym_db.RegisterMessage(ProcessState)

JobStateUpdate = _reflection.GeneratedProtocolMessageType(
    "JobStateUpdate",
    (_message.Message,),
    {
        "DESCRIPTOR": _JOBSTATEUPDATE,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.JobStateUpdate)
    },
)
_sym_db.RegisterMessage(JobStateUpdate)

GridTaskStateResponse = _reflection.GeneratedProtocolMessageType(
    "GridTaskStateResponse",
    (_message.Message,),
    {
        "DESCRIPTOR": _GRIDTASKSTATERESPONSE,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.GridTaskStateResponse)
    },
)
_sym_db.RegisterMessage(GridTaskStateResponse)

GridTaskStatesResponse = _reflection.GeneratedProtocolMessageType(
    "GridTaskStatesResponse",
    (_message.Message,),
    {
        "DESCRIPTOR": _GRIDTASKSTATESRESPONSE,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.GridTaskStatesResponse)
    },
)
_sym_db.RegisterMessage(GridTaskStatesResponse)

AddCredentialsRequest = _reflection.GeneratedProtocolMessageType(
    "AddCredentialsRequest",
    (_message.Message,),
    {
        "DESCRIPTOR": _ADDCREDENTIALSREQUEST,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.AddCredentialsRequest)
    },
)
_sym_db.RegisterMessage(AddCredentialsRequest)

Credentials = _reflection.GeneratedProtocolMessageType(
    "Credentials",
    (_message.Message,),
    {
        "DESCRIPTOR": _CREDENTIALS,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.Credentials)
    },
)
_sym_db.RegisterMessage(Credentials)

AwsSecret = _reflection.GeneratedProtocolMessageType(
    "AwsSecret",
    (_message.Message,),
    {
        "DESCRIPTOR": _AWSSECRET,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.AwsSecret)
    },
)
_sym_db.RegisterMessage(AwsSecret)

ServerAvailableFile = _reflection.GeneratedProtocolMessageType(
    "ServerAvailableFile",
    (_message.Message,),
    {
        "DESCRIPTOR": _SERVERAVAILABLEFILE,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.ServerAvailableFile)
    },
)
_sym_db.RegisterMessage(ServerAvailableFile)

if _descriptor._USE_C_DESCRIPTORS == False:

    DESCRIPTOR._options = None
    _STRINGPAIR._serialized_start = 40
    _STRINGPAIR._serialized_end = 80
    _SERVERAVAILABLEFOLDER._serialized_start = 82
    _SERVERAVAILABLEFOLDER._serialized_end = 125
    _GITREPOCOMMIT._serialized_start = 127
    _GITREPOCOMMIT._serialized_end = 198
    _GITREPOBRANCH._serialized_start = 200
    _GITREPOBRANCH._serialized_end = 271
    _SERVERAVAILABLEINTERPRETER._serialized_start = 273
    _SERVERAVAILABLEINTERPRETER._serialized_end = 327
    _CONTAINERATDIGEST._serialized_start = 329
    _CONTAINERATDIGEST._serialized_end = 384
    _CONTAINERATTAG._serialized_start = 386
    _CONTAINERATTAG._serialized_end = 435
    _SERVERAVAILABLECONTAINER._serialized_start = 437
    _SERVERAVAILABLECONTAINER._serialized_end = 483
    _PYCOMMANDJOB._serialized_start = 485
    _PYCOMMANDJOB._serialized_end = 556
    _QUALIFIEDFUNCTIONNAME._serialized_start = 558
    _QUALIFIEDFUNCTIONNAME._serialized_end = 625
    _PYFUNCTIONJOB._serialized_start = 628
    _PYFUNCTIONJOB._serialized_end = 793
    _GRIDTASK._serialized_start = 795
    _GRIDTASK._serialized_end = 858
    _RESOURCE._serialized_start = 860
    _RESOURCE._serialized_end = 899
    _JOB._serialized_start = 902
    _JOB._serialized_end = 1642
    _JOBTORUN2._serialized_start = 1644
    _JOBTORUN2._serialized_end = 1747
    _PROCESSSTATE._serialized_start = 1750
    _PROCESSSTATE._serialized_end = 2144
    _PROCESSSTATE_PROCESSSTATEENUM._serialized_start = 1927
    _PROCESSSTATE_PROCESSSTATEENUM._serialized_end = 2144
    _JOBSTATEUPDATE._serialized_start = 2146
    _JOBSTATEUPDATE._serialized_end = 2250
    _GRIDTASKSTATERESPONSE._serialized_start = 2252
    _GRIDTASKSTATERESPONSE._serialized_end = 2340
    _GRIDTASKSTATESRESPONSE._serialized_start = 2342
    _GRIDTASKSTATESRESPONSE._serialized_end = 2421
    _ADDCREDENTIALSREQUEST._serialized_start = 2424
    _ADDCREDENTIALSREQUEST._serialized_end = 2636
    _CREDENTIALS._serialized_start = 2639
    _CREDENTIALS._serialized_end = 2788
    _CREDENTIALS_SERVICE._serialized_start = 2675
    _CREDENTIALS_SERVICE._serialized_end = 2726
    _CREDENTIALS_TYPE._serialized_start = 2728
    _CREDENTIALS_TYPE._serialized_end = 2788
    _AWSSECRET._serialized_start = 2790
    _AWSSECRET._serialized_end = 2877
    _SERVERAVAILABLEFILE._serialized_start = 2879
    _SERVERAVAILABLEFILE._serialized_end = 2969
# @@protoc_insertion_point(module_scope)