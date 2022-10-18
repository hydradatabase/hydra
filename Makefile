SHELL=/bin/bash -o pipefail

TARGET ?= default

.PHONY: docker_build
# Runs a full multi-platform docker build
docker_build:
	docker buildx bake $(TARGET)

PLATFORM ?= linux/arm64
.PHONY: docker_build_local
# Runs a docker build of the target platform and loads it into the local docker
# environment
docker_build_local:
	docker buildx bake --set *.platform=$(PLATFORM) --load $(TARGET)

GO_TEST_FLAGS ?=
.PHONY: acceptance_test
acceptance_test:
	go test ./... $(GO_TEST_FLAGS) -count=1 -race -v
