SHELL=/bin/bash -o pipefail
DOCKER_CACHE_DIR=tmp/bake_cache

TARGET ?= default

$(DOCKER_CACHE_DIR):
	mkdir -p $(DOCKER_CACHE_DIR)

TEST_CONTAINER_LOG_DIR ?= tmp/testlogs
$(TEST_CONTAINER_LOG_DIR):
	mkdir -p $(TEST_CONTAINER_LOG_DIR)

.PHONY: docker_build
# Runs a full multi-platform docker build
docker_build: $(DOCKER_CACHE_DIR)
	docker buildx bake $(TARGET)

PLATFORM ?= linux/arm64
.PHONY: docker_build_local
# Runs a docker build of the target platform and loads it into the local docker
# environment
docker_build_local: $(DOCKER_CACHE_DIR)
	docker buildx bake --set *.platform=$(PLATFORM) --load $(TARGET)

GO_TEST_FLAGS ?=

.PHONY: acceptance_test
acceptance_test: $(TEST_CONTAINER_LOG_DIR)
	CONTAINER_LOG_DIR=$$(pwd)/$(TEST_CONTAINER_LOG_DIR) go test ./acceptance/... $(GO_TEST_FLAGS) -count=1 -race -v

.PHONY: postgres_acceptance_test
postgres_acceptance_test: $(TEST_CONTAINER_LOG_DIR)
	CONTAINER_LOG_DIR=$$(pwd)/$(TEST_CONTAINER_LOG_DIR) go test ./acceptance/postgres/... -count=1 -v

.PHONY: spilo_acceptance_test
spilo_acceptance_test: $(TEST_CONTAINER_LOG_DIR)
	CONTAINER_LOG_DIR=$$(pwd)/$(TEST_CONTAINER_LOG_DIR) go test ./acceptance/spilo/... -count=1 -v
