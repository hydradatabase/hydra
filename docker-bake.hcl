variable "TAG" {
  default = "latest"
}

variable "POSTGRES_REPO" {
  default = "ghcr.io/hydrasdb/hydra"
}

variable "SPILO_REPO" {
  default = "ghcr.io/hydrasdb/spilo"
}

variable "COLUMNAR_REPO" {
  default = "ghcr.io/hydrasdb/columnar"
}

variable "SPILO_VERSION" {
  default = "2.1-p7"
}

variable "POSTGRES_BASE_VERSION" {
  default = "13"
}

group "default" {
  targets = ["postgres"]
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

    columnar = "target:columnar"
  }

  tags = ["${POSTGRES_REPO}:${TAG}"]

  cache-to = ["type=local,dest=tmp/bake_cache/postgres"]
  cache-from = ["type=local,src=tmp/bake_cache/postgres"]
}

target "spilo" {
  inherits = ["shared"]

  dockerfile = "Dockerfile.spilo"

  contexts = {
    spilo_base = "docker-image://registry.opensource.zalan.do/acid/spilo-14:${SPILO_VERSION}"
    columnar = "target:columnar"
  }

  args = {
    POSTGRES_BASE_VERSION = "${POSTGRES_BASE_VERSION}"
  }

  tags = ["${SPILO_REPO}:${TAG}"]

  cache-to = ["type=local,dest=tmp/bake_cache/spilo"]
  cache-from = ["type=local,src=tmp/bake_cache/spilo"]
}

target "columnar" {
  inherits = ["shared"]

  context = "../citus"

  args = {
    POSTGRES_BASE_VERSION = "${POSTGRES_BASE_VERSION}"
  }

  tags = ["${COLUMNAR_REPO}:${TAG}"]

  cache-to = ["type=local,dest=tmp/bake_cache/columnar"]
  cache-from = ["type=local,src=tmp/bake_cache/columnar"]
}
