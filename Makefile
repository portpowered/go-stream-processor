GO ?= go
GO_TEST_TIMEOUT ?= 120s
export GOWORK := off
.DEFAULT_GOAL := check
.PHONY: check build test test-race fmt vet example
check: build test vet example
build:
	$(GO) build ./...
test:
	$(GO) test ./... -timeout $(GO_TEST_TIMEOUT)
test-race:
	$(GO) test -race ./... -timeout $(GO_TEST_TIMEOUT)
fmt:
	$(GO) fmt ./...
vet:
	$(GO) vet ./...
example:
	$(GO) run ./examples/basic
