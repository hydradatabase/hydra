SHELL=/bin/bash -o pipefail

define clone_if_not_exist
	bash -c '[ -d "$(2)" ] && echo "$(2) exists, skip cloning" || git clone -b $(3) --single-branch $(1) $(2)'
endef

DOCKER_OPTS ?=
TAG ?= latest
HYDRA_EXT_REPO ?= ghcr.io/hydrasco/hydra_ext
COLUMNAR_EXT_REPO ?= ghcr.io/hydrasco/columnar_ext
SPILO_REPO ?= ghcr.io/hydrasco/spilo
HYDRA_REPO ?= ghcr.io/hydrasco/hydra

.PHONY: docker_push_all
docker_push: docker_build_all
	docker push $(HYDRA_REPO):$(TAG)

.PHONY: docker_build_all
docker_build_all: clone_projects docker_build_hydra_ext docker_build_columnar_ext docker_build_spilo
	docker build \
		$(DOCKER_OPTS) \
		--build-arg HYDRA_EXT_IAMGE=$(HYDRA_EXT_REPO):$(TAG) \
		--build-arg COLUMNAR_EXT_IMAGE=$(COLUMNAR_EXT_REPO):$(TAG) \
		--build-arg SPILO_IMAGE=$(SPILO_REPO):$(TAG) \
		-f Dockerfile.all \
		-t $(HYDRA_REPO):$(TAG) \
		.

.PHONY: clone_projects
clone_projects:
	@$(call clone_if_not_exist,git@github.com:HydrasCo/hydra-extension.git,$(CURDIR)/../hydra-extension,main)
	@$(call clone_if_not_exist,git@github.com:HydrasCo/citus.git,$(CURDIR)/../citus,master)
	@$(call clone_if_not_exist,git@github.com:zalando/spilo.git,$(CURDIR)/../spilo,2.1-p6)

.PHONY: docker_build_spilo
docker_build_spilo:
	cd $(CURDIR)/../spilo/postgres-appliance && \
		docker build \
		$(DOCKER_OPTS) \
		--build-arg TIMESCALEDB="" \
		-t $(SPILO_REPO):$(TAG) \
		.

.PHONY: docker_build_hydra_ext
docker_build_hydra_ext:
	cd $(CURDIR)/../Hydras && docker build $(DOCKER_OPTS) -t $(HYDRA_EXT_REPO):$(TAG) .

.PHONY: docker_build_columnar_ext
docker_build_columnar_ext:
	cd $(CURDIR)/../citus && docker build $(DOCKER_OPTS) -t $(COLUMNAR_EXT_REPO):$(TAG) .

.PHONY: acceptance_test
acceptance_test:
	go test ./... -count=1 -race -v
