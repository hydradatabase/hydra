SHELL=/bin/bash -o pipefail

TARGET ?= default
POSTGRES_BASE_VERSION ?= 14

ECR_REGISTRY ?= 011789831835.dkr.ecr.us-east-1.amazonaws.com

GOLANGCI_LINT_VERSION ?= $$(cat .golangci_lint_version)

TEST_ARTIFACT_DIR ?= $(CURDIR)/tmp/test_artifacts
$(TEST_ARTIFACT_DIR):
	mkdir -p $(TEST_ARTIFACT_DIR)

.PHONY: docker_build
# Runs a full multi-platform docker build
docker_build:
	POSTGRES_BASE_VERSION=$(POSTGRES_BASE_VERSION) docker buildx bake --pull $(TARGET)

uname_m := $(shell uname -m)
ifeq ($(uname_m),x86_64)
       PLATFORM ?= linux/amd64
else
       PLATFORM ?= linux/$(uname_m)
endif

.PHONY: docker_build_local
# Runs a docker build for the target platform and loads it into the local docker
# environment
docker_build_local:
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

.PHONY: docker_push_local
docker_push_local:
	POSTGRES_BASE_VERSION=$(POSTGRES_BASE_VERSION) docker buildx bake --set *.platform=$(PLATFORM) --set *.tags=$(IMAGE) --pull --push $(TARGET)

POSTGRES_IMAGE ?= ghcr.io/hydradatabase/hydra:dev
.PHONY: docker_push_postgres
docker_push_postgres: TARGET = postgres
docker_push_postgres: IMAGE = $(POSTGRES_IMAGE)
docker_push_postgres: docker_push_local

SPILO_IMAGE ?= $(ECR_REGISTRY)/spilo:dev
.PHONY: docker_push_spilo
docker_push_spilo: TARGET = spilo
docker_push_spilo: IMAGE = $(SPILO_IMAGE)
docker_push_spilo: docker_push_local

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

POSTGRES_IMAGE ?= ghcr.io/hydradatabase/hydra:latest
POSTGRES_UPGRADE_FROM_IMAGE ?= ghcr.io/hydradatabase/hydra:$(POSTGRES_BASE_VERSION)

.PHONY: postgres_acceptance_test
# Runs the postgres acceptance tests
postgres_acceptance_test: $(TEST_ARTIFACT_DIR)
	export ARTIFACT_DIR=$(TEST_ARTIFACT_DIR) && \
		export POSTGRES_IMAGE=$(POSTGRES_IMAGE) && \
		export POSTGRES_UPGRADE_FROM_IMAGE=$(POSTGRES_UPGRADE_FROM_IMAGE) && \
		export EXPECTED_POSTGRES_VERSION=$(POSTGRES_BASE_VERSION) && \
		cd acceptance && \
		go test ./postgres/... $(GO_TEST_FLAGS) -count=1 -v

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

# Runs the spilo acceptance tests
.PHONY: spilo_acceptance_test
spilo_acceptance_test: $(TEST_ARTIFACT_DIR)
	export ARTIFACT_DIR=$(TEST_ARTIFACT_DIR) && \
		export SPILO_IMAGE=$(SPILO_IMAGE) && \
		export SPILO_UPGRADE_FROM_IMAGE=$(SPILO_UPGRADE_FROM_IMAGE) && \
		cd acceptance && \
		go test ./spilo/... $(GO_TEST_FLAGS) -count=1 -v

.PHONY: spilo_pull_upgrade_image
spilo_pull_upgrade_image: ecr_login
	docker pull $(SPILO_UPGRADE_FROM_IMAGE)

.PHONY: spilo_acceptance_build_test
# Builds the spilo image then runs acceptance tests
spilo_acceptance_build_test: docker_build_local_spilo spilo_pull_upgrade_image spilo_acceptance_test

.PHONY: lint_acceptance
# Runs the go linter
lint_acceptance:
	docker run --rm -v $(CURDIR)/acceptance:/app -w /app golangci/golangci-lint:$(GOLANGCI_LINT_VERSION) \
		golangci-lint run --timeout 5m --out-format colored-line-number

.PHONY: lint_fix_acceptance
# Runs the go linter with the auto-fixer
lint_fix_acceptance:
	docker run --rm -v $(CURDIR)/acceptance:/app -w /app golangci/golangci-lint:$(GOLANGCI_LINT_VERSION) \
		golangci-lint run --fix
