REM the fact that the input file is referenced as meadowgrid/meadowgrid.proto is crucial,
REM so that the generated code properly does import meadowgrid.meadowgrid_pb2 rather than
REM just import meadowgrid_pb2
call poetry run python -m grpc_tools.protoc -Isrc/protobuf_definitions --python_out=src --mypy_out=src --grpc_python_out=src --mypy_grpc_out=src meadowgrid/meadowgrid.proto

call poetry run python -m grpc_tools.protoc -Isrc/protobuf_definitions --python_out=src --mypy_out=src --grpc_python_out=src --mypy_grpc_out=src meadowflow/server/meadowflow.proto

REM reformat files
call poetry run black src/meadowgrid/*pb2*

call poetry run black src/meadowflow/server/*pb2*

