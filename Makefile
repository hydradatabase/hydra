SHELL=/bin/bash -o pipefail
DOCKER_CACHE_DIR=tmp/bake_cache

TARGET ?= default
POSTGRES_BASE_VERSION ?= 14

$(DOCKER_CACHE_DIR):
	mkdir -p $(DOCKER_CACHE_DIR)

TEST_CONTAINER_LOG_DIR ?= $$(pwd)/tmp/testlogs
$(TEST_CONTAINER_LOG_DIR):
	mkdir -p $(TEST_CONTAINER_LOG_DIR)

.PHONY: docker_build
# Runs a full multi-platform docker build
docker_build: $(DOCKER_CACHE_DIR)
	POSTGRES_BASE_VERSION=$(POSTGRES_BASE_VERSION) docker buildx bake $(TARGET)

PLATFORM ?= linux/arm64
.PHONY: docker_build_local
# Runs a docker build of the target platform and loads it into the local docker
# environment
docker_build_local: $(DOCKER_CACHE_DIR)
	POSTGRES_BASE_VERESION=$(POSTGRES_BASE_VERSION) docker buildx bake --set *.platform=$(PLATFORM) --load $(TARGET)

.PHONY: docker_build_local_postgres
docker_build_local_postgres: TARGET = postgres
docker_build_local_postgres: docker_build_local

.PHONY: docker_build_local_spilo
docker_build_local_spilo: TARGET = spilo
docker_build_local_spilo: docker_build_local

GO_TEST_FLAGS ?=

.PHONY: acceptance_test
acceptance_test: $(TEST_CONTAINER_LOG_DIR)
	CONTAINER_LOG_DIR=$(TEST_CONTAINER_LOG_DIR) go test ./acceptance/... $(GO_TEST_FLAGS) -count=1 -race -v

POSTGRES_IMAGE ?= ghcr.io/hydrasdb/hydra:latest
POSTGRES_UPGRADE_FROM_IMAGE ?= ghcr.io/hydrasdb/hydra:latest
.PHONY: postgres_acceptance_test
postgres_acceptance_test: $(TEST_CONTAINER_LOG_DIR)
	CONTAINER_LOG_DIR=$(TEST_CONTAINER_LOG_DIR) \
		POSTGRES_IMAGE=$(POSTGRES_IMAGE) \
		POSTGRES_UPGRADE_FROM_IMAGE=$(POSTGRES_UPGRADE_FROM_IMAGE) \
		EXPECTED_POSTGRES_VERSION=$(POSTGRES_BASE_VERSION) \
		go test ./acceptance/postgres/... $(GO_TEST_FLAGS) -count=1 -v

.PHONY: postgres_acceptance_build_test
postgres_acceptance_build_test: docker_build_local_postgres postgres_acceptance_test

SPILO_IMAGE ?= ghcr.io/hydrasdb/spilo:latest
SPILO_UPGRADE_FROM_IMAGE ?= ghcr.io/hydrasdb/hydra:$$(cat HYDRA_PROD_VER)
.PHONY: spilo_acceptance_test
spilo_acceptance_test: $(TEST_CONTAINER_LOG_DIR)
	CONTAINER_LOG_DIR=$(TEST_CONTAINER_LOG_DIR) \
		SPILO_IMAGE=$(SPILO_IMAGE) \
		SPILO_UPGRADE_FROM_IMAGE=$(SPILO_UPGRADE_FROM_IMAGE) \
		go test ./acceptance/spilo/... $(GO_TEST_FLAGS) -count=1 -v

.PHONY: spilo_acceptance_build_test
spilo_acceptance_build_test: docker_build_local_spilo spilo_acceptance_test
