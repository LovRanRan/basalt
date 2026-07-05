# Local and CI invocations stay identical: CI calls these targets.

.PHONY: build vet lint test bench-smoke ycsb-smoke generate proto-lint proto-drift cluster-up

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

# ycsb-smoke runs the mixed-workload driver against the in-process engine.
# Point it at a live cluster with e.g.
#   go run ./cmd/basalt-ycsb -cluster 1=127.0.0.1:9101,2=...,3=... -workload a
ycsb-smoke:
	go run ./cmd/basalt-ycsb -smoke -workload a
	go run ./cmd/basalt-ycsb -smoke -workload b
	go run ./cmd/basalt-ycsb -smoke -workload e

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

# cluster-up boots a local 3-node/3-group cluster from examples/cluster.yaml.
cluster-up:
	./scripts/cluster-up.sh
