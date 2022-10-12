variable "TAG" {
  default = "latest"
}

variable "HYDRA_REPO" {
  default = "ghcr.io/hydrasdb/hydra"
}

group "default" {
  targets = ["hydra"]
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
    PGOLDVERSIONS = 13
  }
}

target "columnar_ext" {
  context = "../citus"
}
