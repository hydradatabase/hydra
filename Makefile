SHELL=/bin/bash -o pipefail

define clone_if_not_exist
	bash -c '[ -d "$(2)" ] && echo "$(2) exists, skip cloning" || git clone -b $(3) --single-branch $(1) $(2)'
endef

DOCKER_OPTS ?=
TARGET ?= default
TAG ?= latest
HYDRA_REPO ?= ghcr.io/hydrasdb/hydra
HYDRA_ALL_REPO ?= ghcr.io/hydrasdb/hydra-all

.PHONY: docker_push
docker_push: docker_build
	TAG=$(TAG) HYDRA_REPO=$(HYDRA_REPO) HYDRA_ALL_REPO=$(HYDRA_ALL_REPO) docker buildx bake $(DOCKER_OPTS) $(TARGET) --push

.PHONY: docker_build
docker_build: clone_projects
	TAG=$(TAG) HYDRA_REPO=$(HYDRA_REPO) HYDRA_ALL_REPO=$(HYDRA_ALL_REPO) docker buildx bake $(DOCKER_OPTS) $(TARGET)

.PHONY: clone_projects
clone_projects:
	@$(call clone_if_not_exist,git@github.com:HydrasDB/hydra-extension.git,$(CURDIR)/../hydra-extension,main)
	@$(call clone_if_not_exist,git@github.com:HydrasDB/citus.git,$(CURDIR)/../citus,master)
	@$(call clone_if_not_exist,git@github.com:zalando/spilo.git,$(CURDIR)/../spilo,2.1-p6)

.PHONY: acceptance_test
acceptance_test:
	go test ./... -count=1 -race -v
