SHELL=/bin/bash -o pipefail
DOCKER_CACHE_DIR=tmp/bake_cache

TARGET ?= default
POSTGRES_BASE_VERSION ?= 14

ECR_REGISTRY ?= 011789831835.dkr.ecr.us-east-1.amazonaws.com

$(DOCKER_CACHE_DIR):
	mkdir -p $(DOCKER_CACHE_DIR)

TEST_CONTAINER_LOG_DIR ?= $(CURDIR)/tmp/testlogs
$(TEST_CONTAINER_LOG_DIR):
	mkdir -p $(TEST_CONTAINER_LOG_DIR)

.PHONY: docker_build
# Runs a full multi-platform docker build
docker_build: $(DOCKER_CACHE_DIR)
	POSTGRES_BASE_VERSION=$(POSTGRES_BASE_VERSION) docker buildx bake --pull $(TARGET)

PLATFORM ?= linux/arm64
.PHONY: docker_build_local
# Runs a docker build for the target platform and loads it into the local docker
# environment
docker_build_local: $(DOCKER_CACHE_DIR)
	POSTGRES_BASE_VERSION=$(POSTGRES_BASE_VERSION) docker buildx bake --set *.platform=$(PLATFORM) --pull --load $(TARGET)

.PHONY: docker_build_local_postgres
docker_build_local_postgres: TARGET = postgres
# Runs a local docker build for the target platform for postgres and loads it
# into the local docker
docker_build_local_postgres: docker_build_local

.PHONY: docker_build_local_spilo
docker_build_local_spilo: TARGET = spilo
# Runs a local docker build for the target platform for spilo and loads it
# into the local docker
docker_build_local_spilo: docker_build_local

.PHONY: docker_check_columnar
docker_check_columnar:
	docker buildx bake --set *.platform=$(PLATFORM) --set columnar.target=checker columnar_13 columnar_14

GO_TEST_FLAGS ?=

.PHONY: acceptance_test
# Runs the acceptance tests
acceptance_test: postgres_acceptance_test spilo_acceptance_test

.PHONY: acceptance_build_test
# Builds local images then runs the acceptance tests
acceptance_build_test: postgres_acceptance_build_test spilo_acceptance_build_test

POSTGRES_IMAGE ?= ghcr.io/hydrasdb/hydra:latest
POSTGRES_UPGRADE_FROM_IMAGE ?= ghcr.io/hydrasdb/hydra:$(POSTGRES_BASE_VERSION)

.PHONY: postgres_acceptance_test
# Runs the postgres acceptance tests
postgres_acceptance_test: $(TEST_CONTAINER_LOG_DIR)
	CONTAINER_LOG_DIR=$(TEST_CONTAINER_LOG_DIR) \
		POSTGRES_IMAGE=$(POSTGRES_IMAGE) \
		POSTGRES_UPGRADE_FROM_IMAGE=$(POSTGRES_UPGRADE_FROM_IMAGE) \
		EXPECTED_POSTGRES_VERSION=$(POSTGRES_BASE_VERSION) \
		go test ./acceptance/postgres/... $(GO_TEST_FLAGS) -count=1 -v

.PHONY: postgres_pull_upgrade_image
postgres_pull_upgrade_image:
	docker pull $(POSTGRES_UPGRADE_FROM_IMAGE)

.PHONY: postgres_acceptance_build_test
# Builds the postgres image then runs the acceptance tests
postgres_acceptance_build_test: docker_build_local_postgres postgres_pull_upgrade_image postgres_acceptance_test

.PHONY: ecr_login
ecr_login:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $(ECR_REGISTRY)

SPILO_REPO ?= $(ECR_REGISTRY)/spilo
SPILO_IMAGE ?= $(SPILO_REPO):latest
SPILO_UPGRADE_FROM_IMAGE ?= $(SPILO_REPO):$$(cat HYDRA_PROD_VER)

.PHONY: spilo_acceptance_test
# Runs the spilo acceptance tests
spilo_acceptance_test: $(TEST_CONTAINER_LOG_DIR)
	CONTAINER_LOG_DIR=$(TEST_CONTAINER_LOG_DIR) \
		SPILO_IMAGE=$(SPILO_IMAGE) \
		SPILO_UPGRADE_FROM_IMAGE=$(SPILO_UPGRADE_FROM_IMAGE) \
		go test ./acceptance/spilo/... $(GO_TEST_FLAGS) -count=1 -v

.PHONY: spilo_pull_upgrade_image
spilo_pull_upgrade_image: ecr_login
	docker pull $(SPILO_UPGRADE_FROM_IMAGE)

.PHONY: spilo_acceptance_build_test
# Builds the spilo image then runs acceptance tests
spilo_acceptance_build_test: docker_build_local_spilo spilo_pull_upgrade_image spilo_acceptance_test

.PHONY: lint_acceptance
# Runs the go linter
lint_acceptance:
	golangci-lint run

.PHONY: lint_fix_acceptance
# Runs the go linter with the auto-fixer
lint_fix_acceptance:
	golangci-lint run --fix
