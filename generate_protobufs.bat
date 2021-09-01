REM the fact that the input file is referenced as nextrun/nextrun.proto is crucial, so
REM that the generated code properly does import nextrun.nextrun_pb2 rather than just
REM import nextrun_pb2
call poetry run python -m grpc_tools.protoc -Isrc/protobuf_definitions --python_out=src --grpc_python_out=src nextrun/nextrun.proto

REM reformat files
call poetry run black src/nextrun/*pb2*
