# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: meadowrun/meadowrun.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x19meadowrun/meadowrun.proto\x12\tmeadowrun"(\n\nStringPair\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t"+\n\x15ServerAvailableFolder\x12\x12\n\ncode_paths\x18\x01 \x03(\t"@\n\x0b\x43odeZipFile\x12\x0b\n\x03url\x18\x01 \x01(\t\x12\x12\n\ncode_paths\x18\x02 \x03(\t\x12\x10\n\x08\x63wd_path\x18\x03 \x01(\t"I\n\rGitRepoCommit\x12\x10\n\x08repo_url\x18\x01 \x01(\t\x12\x0e\n\x06\x63ommit\x18\x02 \x01(\t\x12\x16\n\x0epath_to_source\x18\x03 \x01(\t"I\n\rGitRepoBranch\x12\x10\n\x08repo_url\x18\x01 \x01(\t\x12\x0e\n\x06\x62ranch\x18\x02 \x01(\t\x12\x16\n\x0epath_to_source\x18\x03 \x01(\t"6\n\x1aServerAvailableInterpreter\x12\x18\n\x10interpreter_path\x18\x01 \x01(\t"7\n\x11\x43ontainerAtDigest\x12\x12\n\nrepository\x18\x01 \x01(\t\x12\x0e\n\x06\x64igest\x18\x02 \x01(\t"1\n\x0e\x43ontainerAtTag\x12\x12\n\nrepository\x18\x01 \x01(\t\x12\x0b\n\x03tag\x18\x02 \x01(\t"\xa7\x02\n\x15\x45nvironmentSpecInCode\x12\x34\n\x10\x65nvironment_type\x18\x01 \x01(\x0e\x32\x1a.meadowrun.EnvironmentType\x12\x14\n\x0cpath_to_spec\x18\x02 \x01(\t\x12\x16\n\x0epython_version\x18\x03 \x01(\t\x12U\n\x13\x61\x64\x64itional_software\x18\x05 \x03(\x0b\x32\x38.meadowrun.EnvironmentSpecInCode.AdditionalSoftwareEntry\x12\x18\n\x10\x65\x64itable_install\x18\x06 \x01(\x08\x1a\x39\n\x17\x41\x64\x64itionalSoftwareEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01"\xa6\x02\n\x0f\x45nvironmentSpec\x12\x34\n\x10\x65nvironment_type\x18\x01 \x01(\x0e\x32\x1a.meadowrun.EnvironmentType\x12\x0c\n\x04spec\x18\x02 \x01(\t\x12\x11\n\tspec_lock\x18\x03 \x01(\t\x12\x16\n\x0epython_version\x18\x04 \x01(\t\x12O\n\x13\x61\x64\x64itional_software\x18\x05 \x03(\x0b\x32\x32.meadowrun.EnvironmentSpec.AdditionalSoftwareEntry\x12\x18\n\x10\x65\x64itable_install\x18\x06 \x01(\x08\x1a\x39\n\x17\x41\x64\x64itionalSoftwareEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01".\n\x18ServerAvailableContainer\x12\x12\n\nimage_name\x18\x01 \x01(\t"G\n\x0cPyCommandJob\x12\x14\n\x0c\x63ommand_line\x18\x01 \x03(\t\x12!\n\x19pickled_context_variables\x18\x02 \x01(\x0c"C\n\x15QualifiedFunctionName\x12\x13\n\x0bmodule_name\x18\x01 \x01(\t\x12\x15\n\rfunction_name\x18\x02 \x01(\t"\xa5\x01\n\rPyFunctionJob\x12\x43\n\x17qualified_function_name\x18\x01 \x01(\x0b\x32 .meadowrun.QualifiedFunctionNameH\x00\x12\x1a\n\x10pickled_function\x18\x02 \x01(\x0cH\x00\x12"\n\x1apickled_function_arguments\x18\x03 \x01(\x0c\x42\x0f\n\rfunction_spec"\xc8\x01\n\nPyAgentJob\x12\x43\n\x17qualified_function_name\x18\x01 \x01(\x0b\x32 .meadowrun.QualifiedFunctionNameH\x00\x12\x1a\n\x10pickled_function\x18\x02 \x01(\x0cH\x00\x12\x1e\n\x16pickled_agent_function\x18\x03 \x01(\x0c\x12(\n pickled_agent_function_arguments\x18\x04 \x01(\x0c\x42\x0f\n\rfunction_spec"P\n\x08GridTask\x12\x0f\n\x07task_id\x18\x01 \x01(\x05\x12\x0f\n\x07\x61ttempt\x18\x03 \x01(\x05\x12"\n\x1apickled_function_arguments\x18\x02 \x01(\x0c"\xf4\x01\n\x0e\x43ontainerImage\x12\x41\n\x19\x63ontainer_image_at_digest\x18\x01 \x01(\x0b\x32\x1c.meadowrun.ContainerAtDigestH\x00\x12;\n\x16\x63ontainer_image_at_tag\x18\x02 \x01(\x0b\x32\x19.meadowrun.ContainerAtTagH\x00\x12O\n server_available_container_image\x18\x03 \x01(\x0b\x32#.meadowrun.ServerAvailableContainerH\x00\x42\x11\n\x0f\x63ontainer_image"\xd8\x08\n\x03Job\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x19\n\x11job_friendly_name\x18\x02 \x01(\t\x12\x43\n\x17server_available_folder\x18\x05 \x01(\x0b\x32 .meadowrun.ServerAvailableFolderH\x00\x12\x33\n\x0fgit_repo_commit\x18\x06 \x01(\x0b\x32\x18.meadowrun.GitRepoCommitH\x00\x12\x33\n\x0fgit_repo_branch\x18\x07 \x01(\x0b\x32\x18.meadowrun.GitRepoBranchH\x00\x12/\n\rcode_zip_file\x18\x13 \x01(\x0b\x32\x16.meadowrun.CodeZipFileH\x00\x12M\n\x1cserver_available_interpreter\x18\x08 \x01(\x0b\x32%.meadowrun.ServerAvailableInterpreterH\x01\x12;\n\x13\x63ontainer_at_digest\x18\t \x01(\x0b\x32\x1c.meadowrun.ContainerAtDigestH\x01\x12\x35\n\x10\x63ontainer_at_tag\x18\n \x01(\x0b\x32\x19.meadowrun.ContainerAtTagH\x01\x12I\n\x1aserver_available_container\x18\x0b \x01(\x0b\x32#.meadowrun.ServerAvailableContainerH\x01\x12\x44\n\x18\x65nvironment_spec_in_code\x18\x0c \x01(\x0b\x32 .meadowrun.EnvironmentSpecInCodeH\x01\x12\x36\n\x10\x65nvironment_spec\x18\x12 \x01(\x0b\x32\x1a.meadowrun.EnvironmentSpecH\x01\x12\x35\n\x12sidecar_containers\x18\x16 \x03(\x0b\x32\x19.meadowrun.ContainerImage\x12\x34\n\x15\x65nvironment_variables\x18\r \x03(\x0b\x32\x15.meadowrun.StringPair\x12&\n\x1eresult_highest_pickle_protocol\x18\x0e \x01(\x05\x12-\n\npy_command\x18\x0f \x01(\x0b\x32\x17.meadowrun.PyCommandJobH\x02\x12/\n\x0bpy_function\x18\x10 \x01(\x0b\x32\x18.meadowrun.PyFunctionJobH\x02\x12)\n\x08py_agent\x18\x17 \x01(\x0b\x32\x15.meadowrun.PyAgentJobH\x02\x12@\n\x13\x63redentials_sources\x18\x11 \x03(\x0b\x32#.meadowrun.CredentialsSourceMessage\x12\r\n\x05ports\x18\x14 \x03(\t\x12\x10\n\x08uses_gpu\x18\x15 \x01(\x08\x42\x11\n\x0f\x63ode_deploymentB\x18\n\x16interpreter_deploymentB\n\n\x08job_spec"\xc2\x03\n\x0cProcessState\x12\x37\n\x05state\x18\x01 \x01(\x0e\x32(.meadowrun.ProcessState.ProcessStateEnum\x12\x0b\n\x03pid\x18\x02 \x01(\x05\x12\x14\n\x0c\x63ontainer_id\x18\x03 \x01(\t\x12\x15\n\rlog_file_name\x18\x04 \x01(\t\x12\x16\n\x0epickled_result\x18\x05 \x01(\x0c\x12\x13\n\x0breturn_code\x18\x06 \x01(\x05\x12\x1a\n\x12max_memory_used_gb\x18\x07 \x01(\x02"\xf5\x01\n\x10ProcessStateEnum\x12\x0b\n\x07\x44\x45\x46\x41ULT\x10\x00\x12\x11\n\rRUN_REQUESTED\x10\x01\x12\x0b\n\x07RUNNING\x10\x02\x12\r\n\tSUCCEEDED\x10\x03\x12\x16\n\x12RUN_REQUEST_FAILED\x10\x04\x12\x14\n\x10PYTHON_EXCEPTION\x10\x05\x12\x18\n\x14NON_ZERO_RETURN_CODE\x10\x06\x12\x1b\n\x17RESOURCES_NOT_AVAILABLE\x10\x07\x12\x17\n\x13\x45RROR_GETTING_STATE\x10\x08\x12\x1a\n\x16UNEXPECTED_WORKER_EXIT\x10\n\x12\x0b\n\x07UNKNOWN\x10\t"P\n\x0eJobStateUpdate\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12.\n\rprocess_state\x18\x02 \x01(\x0b\x32\x17.meadowrun.ProcessState"i\n\x15GridTaskStateResponse\x12\x0f\n\x07task_id\x18\x01 \x01(\x05\x12\x0f\n\x07\x61ttempt\x18\x03 \x01(\x05\x12.\n\rprocess_state\x18\x02 \x01(\x0b\x32\x17.meadowrun.ProcessState"\xd0\x02\n\x18\x43redentialsSourceMessage\x12/\n\x07service\x18\x01 \x01(\x0e\x32\x1e.meadowrun.Credentials.Service\x12\x13\n\x0bservice_url\x18\x02 \x01(\t\x12/\n\naws_secret\x18\x03 \x01(\x0b\x32\x19.meadowrun.AwsSecretProtoH\x00\x12\x33\n\x0c\x61zure_secret\x18\x05 \x01(\x0b\x32\x1b.meadowrun.AzureSecretProtoH\x00\x12?\n\x15server_available_file\x18\x04 \x01(\x0b\x32\x1e.meadowrun.ServerAvailableFileH\x00\x12=\n\x11kubernetes_secret\x18\x06 \x01(\x0b\x32 .meadowrun.KubernetesSecretProtoH\x00\x42\x08\n\x06source"\x95\x01\n\x0b\x43redentials\x12\x13\n\x0b\x63redentials\x18\x01 \x01(\x0c"3\n\x07Service\x12\x13\n\x0f\x44\x45\x46\x41ULT_SERVICE\x10\x00\x12\n\n\x06\x44OCKER\x10\x01\x12\x07\n\x03GIT\x10\x02"<\n\x04Type\x12\x10\n\x0c\x44\x45\x46\x41ULT_TYPE\x10\x00\x12\x15\n\x11USERNAME_PASSWORD\x10\x01\x12\x0b\n\x07SSH_KEY\x10\x02"\\\n\x0e\x41wsSecretProto\x12\x35\n\x10\x63redentials_type\x18\x01 \x01(\x0e\x32\x1b.meadowrun.Credentials.Type\x12\x13\n\x0bsecret_name\x18\x02 \x01(\t"r\n\x10\x41zureSecretProto\x12\x35\n\x10\x63redentials_type\x18\x01 \x01(\x0e\x32\x1b.meadowrun.Credentials.Type\x12\x12\n\nvault_name\x18\x02 \x01(\t\x12\x13\n\x0bsecret_name\x18\x03 \x01(\t"Z\n\x13ServerAvailableFile\x12\x35\n\x10\x63redentials_type\x18\x01 \x01(\x0e\x32\x1b.meadowrun.Credentials.Type\x12\x0c\n\x04path\x18\x02 \x01(\t"c\n\x15KubernetesSecretProto\x12\x35\n\x10\x63redentials_type\x18\x01 \x01(\x0e\x32\x1b.meadowrun.Credentials.Type\x12\x13\n\x0bsecret_name\x18\x02 \x01(\t*>\n\x0f\x45nvironmentType\x12\x0b\n\x07\x44\x45\x46\x41ULT\x10\x00\x12\t\n\x05\x43ONDA\x10\x01\x12\x07\n\x03PIP\x10\x02\x12\n\n\x06POETRY\x10\x03\x62\x06proto3'
)

_ENVIRONMENTTYPE = DESCRIPTOR.enum_types_by_name["EnvironmentType"]
EnvironmentType = enum_type_wrapper.EnumTypeWrapper(_ENVIRONMENTTYPE)
DEFAULT = 0
CONDA = 1
PIP = 2
POETRY = 3


_STRINGPAIR = DESCRIPTOR.message_types_by_name["StringPair"]
_SERVERAVAILABLEFOLDER = DESCRIPTOR.message_types_by_name["ServerAvailableFolder"]
_CODEZIPFILE = DESCRIPTOR.message_types_by_name["CodeZipFile"]
_GITREPOCOMMIT = DESCRIPTOR.message_types_by_name["GitRepoCommit"]
_GITREPOBRANCH = DESCRIPTOR.message_types_by_name["GitRepoBranch"]
_SERVERAVAILABLEINTERPRETER = DESCRIPTOR.message_types_by_name[
    "ServerAvailableInterpreter"
]
_CONTAINERATDIGEST = DESCRIPTOR.message_types_by_name["ContainerAtDigest"]
_CONTAINERATTAG = DESCRIPTOR.message_types_by_name["ContainerAtTag"]
_ENVIRONMENTSPECINCODE = DESCRIPTOR.message_types_by_name["EnvironmentSpecInCode"]
_ENVIRONMENTSPECINCODE_ADDITIONALSOFTWAREENTRY = (
    _ENVIRONMENTSPECINCODE.nested_types_by_name["AdditionalSoftwareEntry"]
)
_ENVIRONMENTSPEC = DESCRIPTOR.message_types_by_name["EnvironmentSpec"]
_ENVIRONMENTSPEC_ADDITIONALSOFTWAREENTRY = _ENVIRONMENTSPEC.nested_types_by_name[
    "AdditionalSoftwareEntry"
]
_SERVERAVAILABLECONTAINER = DESCRIPTOR.message_types_by_name["ServerAvailableContainer"]
_PYCOMMANDJOB = DESCRIPTOR.message_types_by_name["PyCommandJob"]
_QUALIFIEDFUNCTIONNAME = DESCRIPTOR.message_types_by_name["QualifiedFunctionName"]
_PYFUNCTIONJOB = DESCRIPTOR.message_types_by_name["PyFunctionJob"]
_PYAGENTJOB = DESCRIPTOR.message_types_by_name["PyAgentJob"]
_GRIDTASK = DESCRIPTOR.message_types_by_name["GridTask"]
_CONTAINERIMAGE = DESCRIPTOR.message_types_by_name["ContainerImage"]
_JOB = DESCRIPTOR.message_types_by_name["Job"]
_PROCESSSTATE = DESCRIPTOR.message_types_by_name["ProcessState"]
_JOBSTATEUPDATE = DESCRIPTOR.message_types_by_name["JobStateUpdate"]
_GRIDTASKSTATERESPONSE = DESCRIPTOR.message_types_by_name["GridTaskStateResponse"]
_CREDENTIALSSOURCEMESSAGE = DESCRIPTOR.message_types_by_name["CredentialsSourceMessage"]
_CREDENTIALS = DESCRIPTOR.message_types_by_name["Credentials"]
_AWSSECRETPROTO = DESCRIPTOR.message_types_by_name["AwsSecretProto"]
_AZURESECRETPROTO = DESCRIPTOR.message_types_by_name["AzureSecretProto"]
_SERVERAVAILABLEFILE = DESCRIPTOR.message_types_by_name["ServerAvailableFile"]
_KUBERNETESSECRETPROTO = DESCRIPTOR.message_types_by_name["KubernetesSecretProto"]
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

CodeZipFile = _reflection.GeneratedProtocolMessageType(
    "CodeZipFile",
    (_message.Message,),
    {
        "DESCRIPTOR": _CODEZIPFILE,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.CodeZipFile)
    },
)
_sym_db.RegisterMessage(CodeZipFile)

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

EnvironmentSpecInCode = _reflection.GeneratedProtocolMessageType(
    "EnvironmentSpecInCode",
    (_message.Message,),
    {
        "AdditionalSoftwareEntry": _reflection.GeneratedProtocolMessageType(
            "AdditionalSoftwareEntry",
            (_message.Message,),
            {
                "DESCRIPTOR": _ENVIRONMENTSPECINCODE_ADDITIONALSOFTWAREENTRY,
                "__module__": "meadowrun.meadowrun_pb2"
                # @@protoc_insertion_point(class_scope:meadowrun.EnvironmentSpecInCode.AdditionalSoftwareEntry)
            },
        ),
        "DESCRIPTOR": _ENVIRONMENTSPECINCODE,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.EnvironmentSpecInCode)
    },
)
_sym_db.RegisterMessage(EnvironmentSpecInCode)
_sym_db.RegisterMessage(EnvironmentSpecInCode.AdditionalSoftwareEntry)

EnvironmentSpec = _reflection.GeneratedProtocolMessageType(
    "EnvironmentSpec",
    (_message.Message,),
    {
        "AdditionalSoftwareEntry": _reflection.GeneratedProtocolMessageType(
            "AdditionalSoftwareEntry",
            (_message.Message,),
            {
                "DESCRIPTOR": _ENVIRONMENTSPEC_ADDITIONALSOFTWAREENTRY,
                "__module__": "meadowrun.meadowrun_pb2"
                # @@protoc_insertion_point(class_scope:meadowrun.EnvironmentSpec.AdditionalSoftwareEntry)
            },
        ),
        "DESCRIPTOR": _ENVIRONMENTSPEC,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.EnvironmentSpec)
    },
)
_sym_db.RegisterMessage(EnvironmentSpec)
_sym_db.RegisterMessage(EnvironmentSpec.AdditionalSoftwareEntry)

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

PyAgentJob = _reflection.GeneratedProtocolMessageType(
    "PyAgentJob",
    (_message.Message,),
    {
        "DESCRIPTOR": _PYAGENTJOB,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.PyAgentJob)
    },
)
_sym_db.RegisterMessage(PyAgentJob)

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

ContainerImage = _reflection.GeneratedProtocolMessageType(
    "ContainerImage",
    (_message.Message,),
    {
        "DESCRIPTOR": _CONTAINERIMAGE,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.ContainerImage)
    },
)
_sym_db.RegisterMessage(ContainerImage)

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

CredentialsSourceMessage = _reflection.GeneratedProtocolMessageType(
    "CredentialsSourceMessage",
    (_message.Message,),
    {
        "DESCRIPTOR": _CREDENTIALSSOURCEMESSAGE,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.CredentialsSourceMessage)
    },
)
_sym_db.RegisterMessage(CredentialsSourceMessage)

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

AwsSecretProto = _reflection.GeneratedProtocolMessageType(
    "AwsSecretProto",
    (_message.Message,),
    {
        "DESCRIPTOR": _AWSSECRETPROTO,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.AwsSecretProto)
    },
)
_sym_db.RegisterMessage(AwsSecretProto)

AzureSecretProto = _reflection.GeneratedProtocolMessageType(
    "AzureSecretProto",
    (_message.Message,),
    {
        "DESCRIPTOR": _AZURESECRETPROTO,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.AzureSecretProto)
    },
)
_sym_db.RegisterMessage(AzureSecretProto)

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

KubernetesSecretProto = _reflection.GeneratedProtocolMessageType(
    "KubernetesSecretProto",
    (_message.Message,),
    {
        "DESCRIPTOR": _KUBERNETESSECRETPROTO,
        "__module__": "meadowrun.meadowrun_pb2"
        # @@protoc_insertion_point(class_scope:meadowrun.KubernetesSecretProto)
    },
)
_sym_db.RegisterMessage(KubernetesSecretProto)

if _descriptor._USE_C_DESCRIPTORS == False:

    DESCRIPTOR._options = None
    _ENVIRONMENTSPECINCODE_ADDITIONALSOFTWAREENTRY._options = None
    _ENVIRONMENTSPECINCODE_ADDITIONALSOFTWAREENTRY._serialized_options = b"8\001"
    _ENVIRONMENTSPEC_ADDITIONALSOFTWAREENTRY._options = None
    _ENVIRONMENTSPEC_ADDITIONALSOFTWAREENTRY._serialized_options = b"8\001"
    _ENVIRONMENTTYPE._serialized_start = 4643
    _ENVIRONMENTTYPE._serialized_end = 4705
    _STRINGPAIR._serialized_start = 40
    _STRINGPAIR._serialized_end = 80
    _SERVERAVAILABLEFOLDER._serialized_start = 82
    _SERVERAVAILABLEFOLDER._serialized_end = 125
    _CODEZIPFILE._serialized_start = 127
    _CODEZIPFILE._serialized_end = 191
    _GITREPOCOMMIT._serialized_start = 193
    _GITREPOCOMMIT._serialized_end = 266
    _GITREPOBRANCH._serialized_start = 268
    _GITREPOBRANCH._serialized_end = 341
    _SERVERAVAILABLEINTERPRETER._serialized_start = 343
    _SERVERAVAILABLEINTERPRETER._serialized_end = 397
    _CONTAINERATDIGEST._serialized_start = 399
    _CONTAINERATDIGEST._serialized_end = 454
    _CONTAINERATTAG._serialized_start = 456
    _CONTAINERATTAG._serialized_end = 505
    _ENVIRONMENTSPECINCODE._serialized_start = 508
    _ENVIRONMENTSPECINCODE._serialized_end = 803
    _ENVIRONMENTSPECINCODE_ADDITIONALSOFTWAREENTRY._serialized_start = 746
    _ENVIRONMENTSPECINCODE_ADDITIONALSOFTWAREENTRY._serialized_end = 803
    _ENVIRONMENTSPEC._serialized_start = 806
    _ENVIRONMENTSPEC._serialized_end = 1100
    _ENVIRONMENTSPEC_ADDITIONALSOFTWAREENTRY._serialized_start = 746
    _ENVIRONMENTSPEC_ADDITIONALSOFTWAREENTRY._serialized_end = 803
    _SERVERAVAILABLECONTAINER._serialized_start = 1102
    _SERVERAVAILABLECONTAINER._serialized_end = 1148
    _PYCOMMANDJOB._serialized_start = 1150
    _PYCOMMANDJOB._serialized_end = 1221
    _QUALIFIEDFUNCTIONNAME._serialized_start = 1223
    _QUALIFIEDFUNCTIONNAME._serialized_end = 1290
    _PYFUNCTIONJOB._serialized_start = 1293
    _PYFUNCTIONJOB._serialized_end = 1458
    _PYAGENTJOB._serialized_start = 1461
    _PYAGENTJOB._serialized_end = 1661
    _GRIDTASK._serialized_start = 1663
    _GRIDTASK._serialized_end = 1743
    _CONTAINERIMAGE._serialized_start = 1746
    _CONTAINERIMAGE._serialized_end = 1990
    _JOB._serialized_start = 1993
    _JOB._serialized_end = 3105
    _PROCESSSTATE._serialized_start = 3108
    _PROCESSSTATE._serialized_end = 3558
    _PROCESSSTATE_PROCESSSTATEENUM._serialized_start = 3313
    _PROCESSSTATE_PROCESSSTATEENUM._serialized_end = 3558
    _JOBSTATEUPDATE._serialized_start = 3560
    _JOBSTATEUPDATE._serialized_end = 3640
    _GRIDTASKSTATERESPONSE._serialized_start = 3642
    _GRIDTASKSTATERESPONSE._serialized_end = 3747
    _CREDENTIALSSOURCEMESSAGE._serialized_start = 3750
    _CREDENTIALSSOURCEMESSAGE._serialized_end = 4086
    _CREDENTIALS._serialized_start = 4089
    _CREDENTIALS._serialized_end = 4238
    _CREDENTIALS_SERVICE._serialized_start = 4125
    _CREDENTIALS_SERVICE._serialized_end = 4176
    _CREDENTIALS_TYPE._serialized_start = 4178
    _CREDENTIALS_TYPE._serialized_end = 4238
    _AWSSECRETPROTO._serialized_start = 4240
    _AWSSECRETPROTO._serialized_end = 4332
    _AZURESECRETPROTO._serialized_start = 4334
    _AZURESECRETPROTO._serialized_end = 4448
    _SERVERAVAILABLEFILE._serialized_start = 4450
    _SERVERAVAILABLEFILE._serialized_end = 4540
    _KUBERNETESSECRETPROTO._serialized_start = 4542
    _KUBERNETESSECRETPROTO._serialized_end = 4641
# @@protoc_insertion_point(module_scope)
