SHELL=/bin/bash -o pipefail

define clone_if_not_exist
	bash -c '[ -d "$(2)" ] && echo "$(2) exists, skip cloning" || git clone $(1) $(2)'
endef

TAG ?= latest
HYDRA_EXT_REPO ?= ghcr.io/hydrasco/hydra_ext
COLUMNAR_EXT_REPO ?= ghcr.io/hydrasco/columnar_ext
HYDRA_REPO ?= ghcr.io/hydrasco/hydra/hydra

.PHONY: docker_push
docker_push: docker_build
	docker push $(HYDRA_REPO):$(TAG)

.PHONY: docker_build
docker_build: clone_projects docker_build_hydra_ext docker_build_columnar_ext
	cd $(CURDIR)/../spilo-build && \
		docker build \
		--build-arg HYDRA_EXT_REPO=$(HYDRA_EXT_REPO):$(TAG) \
		--build-arg COLUMNAR_EXT_REPO=$(COLUMNAR_EXT_REPO):$(TAG) \
		--build-arg TIMESCALEDB= \
		-f Dockerfile.cloud \
		-t $(HYDRA_REPO):$(TAG) \
		.

.PHONY: clone_projects
clone_projects:
	@$(call clone_if_not_exist,git@github.com:HydrasCo/Hydras.git,$(CURDIR)/../Hydras)
	@$(call clone_if_not_exist,git@github.com:HydrasCo/citus.git,$(CURDIR)/../citus)
	@$(call clone_if_not_exist,git@github.com:HydrasCo/spilo.git,$(CURDIR)/../spilo-build)

.PHONY: docker_build_hydra_ext
docker_build_hydra_ext:
	cd $(CURDIR)/../Hydras && docker build -t $(HYDRA_EXT_REPO):$(TAG) .

.PHONY: docker_build_columnar_ext
docker_build_columnar_ext:
	cd $(CURDIR)/../citus && docker build -t $(COLUMNAR_EXT_REPO):$(TAG) .

.PHONY: acceptance_test
acceptance_test:
	go test ./... -count=1 -race -v
