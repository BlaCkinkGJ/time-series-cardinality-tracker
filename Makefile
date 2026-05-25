.PHONY: build test proto bench lint

export GOTOOLCHAIN=local

build:
	go build ./...

test:
	go test ./... -race -count=1

proto:
	protoc --go_out=gen --go_opt=paths=source_relative \
	  --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
	  --grpc-gateway_out=gen --grpc-gateway_opt=paths=source_relative \
	  -I proto proto/cardinality/v1/cardinality.proto

bench:
	go test ./bench/... -bench=. -benchmem -run=^$

lint:
	golangci-lint run
