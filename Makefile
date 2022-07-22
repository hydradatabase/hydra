SHELL=/bin/bash -o pipefail

define clone_if_not_exist
	bash -c '[ -d "$(2)" ] && echo "$(2) exists, skip cloning" || git clone $(1) $(2)'
endef

all: clone_projects build_hydra_ext build_columnar_ext build_spilo

.PHONY: clone_projects
clone_projects:
	@$(call clone_if_not_exist,git@github.com:HydrasCo/Hydras.git,$(CURDIR)/../Hydras)
	@$(call clone_if_not_exist,git@github.com:HydrasCo/citus.git,$(CURDIR)/../citus)
	@$(call clone_if_not_exist,git@github.com:HydrasCo/spilo.git,$(CURDIR)/../spilo)

TAG ?= latest
HYDR_REPO ?= hydra/hydra
COLUMNAR_REPO ?= hydra/columnar
SPILO_REPO ?= hydra/spilo

.PHONY: build_hydra_ext
build_hydra_ext:
	cd $(CURDIR)/../Hydras && docker build -t $(HYDR_REPO):$(TAG) .

.PHONY: build_columnar_ext
build_columnar_ext:
	cd $(CURDIR)/../citus && docker build -t $(COLUMNAR_REPO):$(TAG) .

.PHONY: build_spilo
build_spilo:
	cd $(CURDIR)/../spilo/postgres-appliance && docker build --build-arg HYDRA_EXT_REPO=$(HYDR_REPO):$(TAG) --build-arg COLUMNAR_EXT_REPO=$(COLUMNAR_REPO):$(TAG) -t $(SPILO_REPO):$(TAG) .

