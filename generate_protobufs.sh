#!/bin/bash
set -euo pipefail

# the fact that the input file is referenced as meadowrun/meadowrun.proto is crucial,
# so that the generated code properly does import meadowrun.meadowrun_pb2 rather than
# just import meadowrun_pb2
poetry run python -m grpc_tools.protoc -Isrc/protobuf_definitions --python_out=src --mypy_out=src meadowrun/meadowrun.proto

# reformat files
poetry run black src/meadowrun/*pb2*
