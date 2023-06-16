# Build pd-server, pd-ctl, pd-recover
default: build

# Development validation.
all: dev
dev: build

.PHONY: default all dev

#### Build ####

BUILD_FLAGS ?=
BUILD_TAGS ?=
BUILD_CGO_ENABLED := 0

ROOT_PATH := $(shell pwd)
BUILD_BIN_PATH := $(ROOT_PATH)/bin

build: incremental snapshot

incremental:
	CGO_ENABLED=$(BUILD_CGO_ENABLED) go build $(BUILD_FLAGS) -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -tags "$(BUILD_TAGS)" -o $(BUILD_BIN_PATH)/incremental cmd/incremental/main.go

snapshot:
	CGO_ENABLED=$(BUILD_CGO_ENABLED) go build $(BUILD_FLAGS) -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -tags "$(BUILD_TAGS)" -o $(BUILD_BIN_PATH)/snapshot cmd/snapshot/main.go

.PHONY: build incremental snapshot

#### Clean up ####

clean:
	# Cleaning building files...
	rm -rf $(BUILD_BIN_PATH)

.PHONY: clean