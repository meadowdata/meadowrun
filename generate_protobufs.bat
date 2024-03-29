REM the fact that the input file is referenced as meadowrun/meadowrun.proto is crucial,
REM so that the generated code properly does import meadowrun.meadowrun_pb2 rather than
REM just import meadowrun_pb2
call poetry run python -m grpc_tools.protoc -Isrc/protobuf_definitions --python_out=src --mypy_out=src meadowrun/meadowrun.proto

REM reformat files
call poetry run black src/meadowrun/*pb2*
