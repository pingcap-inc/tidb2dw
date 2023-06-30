# Build pd-server, pd-ctl, pd-recover
default: dev

# Development validation.
all: dev
dev: tidy fmt build

.PHONY: default all dev

#### Build ####

BUILD_FLAGS ?=
BUILD_TAGS ?=

ROOT_PATH := $(shell pwd)
BUILD_BIN_PATH := $(ROOT_PATH)/bin

REPO    := github.com/pingcap-inc/tidb2dw

_COMMIT := $(shell git describe --no-match --always --dirty)
_GITREF := $(shell git rev-parse --abbrev-ref HEAD)
COMMIT  := $(if $(COMMIT),$(COMMIT),$(_COMMIT))
GITREF  := $(if $(GITREF),$(GITREF),$(_GITREF))

LDFLAGS := -w -s
LDFLAGS += -X "$(REPO)/version.GitHash=$(COMMIT)"
LDFLAGS += -X "$(REPO)/version.GitRef=$(GITREF)"
LDFLAGS += $(EXTRA_LDFLAGS)

.PHONY: build fmt tidy

build:
	go build $(BUILD_FLAGS) -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -tags "$(BUILD_TAGS)" -o $(BUILD_BIN_PATH)/tidb2dw main.go

fmt:
	go fmt ./...

tidy:
	go mod tidy

.PHONY: clean

clean:
	rm -rf $(BUILD_BIN_PATH)
