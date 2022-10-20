variable "POSTGRES_REPO" {
  default = "ghcr.io/hydrasdb/hydra"
}

variable "SPILO_REPO" {
  default = "011789831835.dkr.ecr.us-east-1.amazonaws.com/spilo"
}

variable "SPILO_VERSION" {
  default = "2.1-p7"
}

variable "POSTGRES_BASE_VERSION" {
  default = "14"
}

variable "SPILO_POSTGRES_VERSION" {
  default = "14"
}

variable "SPILO_POSTGRES_OLD_VERSIONS" {
  default = "13"
}

group "default" {
  targets = ["postgres", "spilo"]
}

target "shared" {
  platforms = [
    "linux/amd64",
    "linux/arm64/v8"
  ]
}

target "postgres" {
  inherits = ["shared"]

  contexts = {
    postgres_base = "docker-image://postgres:${POSTGRES_BASE_VERSION}"

    columnar = "target:columnar_${POSTGRES_BASE_VERSION}"
  }

  tags = [
    "${POSTGRES_REPO}:latest",
    "${POSTGRES_REPO}:${POSTGRES_BASE_VERSION}-latest"
  ]

  cache-to = ["type=local,dest=tmp/bake_cache/postgres"]
  cache-from = ["type=local,src=tmp/bake_cache/postgres"]
}

target "spilo" {
  inherits = ["shared"]

  dockerfile = "Dockerfile.spilo"

  contexts = {
    spilo_base = "target:spilo_base"
    columnar_13 = "target:columnar_13"
    columnar_14 = "target:columnar_14"
  }

  args = {
    POSTGRES_BASE_VERSION = "${SPILO_POSTGRES_VERSION}"
  }

  tags = [
    "${SPILO_REPO}:latest",
    "${SPILO_REPO}:${SPILO_VERSION}-latest"
  ]

  cache-to = ["type=local,dest=tmp/bake_cache/spilo"]
  cache-from = ["type=local,src=tmp/bake_cache/spilo"]
}

target "spilo_base" {
  inherits = ["shared"]

  context = "https://github.com/zalando/spilo.git#${SPILO_VERSION}:postgres-appliance"

  args = {
    TIMESCALEDB = ""
    PGVERSION = "${SPILO_POSTGRES_VERSION}"
    PGOLDVERSIONS = "${SPILO_POSTGRES_OLD_VERSIONS}"
  }

  cache-to = ["type=local,dest=tmp/bake_cache/spilo_base"]
  cache-from = ["type=local,src=tmp/bake_cache/spilo_base"]
}

target "columnar_13" {
  inherits = ["shared"]

  context = "columnar"

  args = {
    POSTGRES_BASE_VERSION = 13
  }

  cache-to = ["type=local,dest=tmp/bake_cache/columnar_13"]
  cache-from = ["type=local,src=tmp/bake_cache/columnar_13"]
}

target "columnar_14" {
  inherits = ["shared"]

  context = "columnar"

  args = {
    POSTGRES_BASE_VERSION = 14
  }

  cache-to = ["type=local,dest=tmp/bake_cache/columnar_14"]
  cache-from = ["type=local,src=tmp/bake_cache/columnar_14"]
}
