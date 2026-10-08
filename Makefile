.PHONY: build test proto bench lint

export GOTOOLCHAIN=local
export CGO_ENABLED=0

build:
	go build ./...

test:
	go test ./... -count=1

# Generator versions gen/ was produced with: protoc-gen-go v1.36.12,
# protoc-gen-go-grpc v1.4.0. Regenerating with a different version rewrites
# the whole tree (style and unsafe usage), so match them or expect churn.
proto:
	protoc --go_out=gen --go_opt=paths=source_relative \
	  --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
	  --grpc-gateway_out=gen --grpc-gateway_opt=paths=source_relative \
	  -I proto -I third_party proto/cardinality/v1/cardinality.proto proto/cardinality/v1/command.proto

bench:
	go test ./bench/... -bench=. -benchmem -run=^$

lint:
	golangci-lint run
