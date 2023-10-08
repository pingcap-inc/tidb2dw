# Build pd-server, pd-ctl, pd-recover
default: dev

# Development validation.
all: dev
dev: tidy fmt build

.PHONY: default all dev

#### Build ####

BUILD_FLAGS ?=
BUILD_TAGS ?=

DOCKER_IMG ?= pingcap/dwworker:latest

ROOT_PATH := $(shell pwd)
BUILD_OUTPUT := $(ROOT_PATH)/bin/tidb2dw

REPO    := github.com/pingcap-inc/tidb2dw

_COMMIT := $(shell git describe --no-match --always --dirty)
_GITREF := $(shell git rev-parse --abbrev-ref HEAD)
COMMIT  := $(if $(COMMIT),$(COMMIT),$(_COMMIT))
GITREF  := $(if $(GITREF),$(GITREF),$(_GITREF))

LDFLAGS := -w -s
LDFLAGS += -X "$(REPO)/version.GitHash=$(COMMIT)"
LDFLAGS += -X "$(REPO)/version.GitRef=$(GITREF)"
LDFLAGS += $(EXTRA_LDFLAGS)

CGO_ENABLED ?= 0
ifeq ($(shell uname -s),Darwin)
	CGO_ENABLED=1
endif

.PHONY: build
build:
	@echo "Build using CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH)"
	CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_FLAGS) -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -tags "$(BUILD_TAGS)" -o $(BUILD_OUTPUT) main.go

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: tiny
tidy:
	go mod tidy

.PHONY: clean
clean:
	rm -rf $(ROOT_PATH)/bin

.PHONY: docker-build
docker-build: export CGO_ENABLED=0
docker-build: export GOOS=linux
docker-build: export GOARCH=amd64
docker-build: BUILD_OUTPUT=$(ROOT_PATH)/bin/dwworker-linux-amd64
docker-build: build
	docker build -t $(DOCKER_IMG) .

.PHONY: docker-push
docker-push:
	docker push $(DOCKER_IMG)
