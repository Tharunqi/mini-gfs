#!/bin/bash

set -e

protoc \
  --proto_path=proto \
  --go_out=internal/pb \
  --go_opt=paths=source_relative \
  --go-grpc_out=internal/pb \
  --go-grpc_opt=paths=source_relative \
  proto/common.proto \
  proto/master.proto \
  proto/chunk.proto

echo "Protobuf code generated successfully."
