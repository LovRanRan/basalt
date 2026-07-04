# Local and CI invocations stay identical: CI calls these targets.

.PHONY: build vet lint test bench-smoke generate proto-lint proto-drift

build:
	go build ./...

vet:
	go vet ./...

lint:
	golangci-lint run

test:
	go test -race -count=1 ./...

bench-smoke:
	go run ./cmd/basalt-bench -smoke

# generate regenerates the protobuf stubs; the generated code is committed
# so the module builds without protoc installed.
GOBIN := $(shell go env GOPATH)/bin
generate:
	PATH="$(GOBIN):$$PATH" buf generate

proto-lint:
	buf lint

# proto-drift fails when committed stubs do not match the proto sources.
proto-drift: generate
	git diff --exit-code -- api
