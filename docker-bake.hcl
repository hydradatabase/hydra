variable "TAG" {
  default = "latest"
}

variable "HYDRA_REPO" {
  default = "ghcr.io/hydrasco/hydra"
}

variable "HYDRA_ALL_REPO" {
  default = "ghcr.io/hydrasco/hydra-all"
}

group "default" {
  targets = ["hydra", "hydra-all"]
}

target "hydra-all" {
  dockerfile = "Dockerfile.all"
  contexts = {
    spilobase = "target:spilo"
    columnar_ext = "target:columnar_ext"
    hydra_ext = "target:hydra_ext"
  }
  tags = ["${HYDRA_ALL_REPO}:${TAG}"]
}

target "hydra" {
  contexts = {
    spilobase = "target:spilo"
    columnar_ext = "target:columnar_ext"
  }
  tags = ["${HYDRA_REPO}:${TAG}"]
}

target "spilo" {
  context = "../spilo/postgres-appliance"
  args = {
    TIMESCALEDB = ""
  }
}

target "columnar_ext" {
  context = "../citus"
}

target "hydra_ext" {
  context = "../hydra-extension"
}
