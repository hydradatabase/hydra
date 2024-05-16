variable "POSTGRES_REPO" {
  default = "ghcr.io/hydradatabase/hydra"
}

variable "SPILO_REPO" {
  default = "011789831835.dkr.ecr.us-east-1.amazonaws.com/spilo"
}

variable "SPILO_VERSION" {
  default = "3.2-p2"
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
    "linux/arm64"
  ]

  args = {
    TIMESTAMP = "${timestamp()}"
  }
}

target "postgres" {
  inherits = ["shared"]

  contexts = {
    columnar = "target:columnar_${POSTGRES_BASE_VERSION}"
    postgres_base = "docker-image://postgres:${POSTGRES_BASE_VERSION}-bookworm"
  }

  args = {
    POSTGRES_BASE_VERSION = "${POSTGRES_BASE_VERSION}"
  }

  tags = [
    "${POSTGRES_REPO}:latest",
    "${POSTGRES_REPO}:${POSTGRES_BASE_VERSION}"
  ]
}

target "spilo" {
  inherits = ["shared"]

  dockerfile = "Dockerfile.spilo"

  contexts = {
    spilo_base = "target:spilo_base"
    columnar_13 = "target:columnar_13"
    columnar_14 = "target:columnar_14"
    columnar_15 = "target:columnar_15"
  }

  args = {
    POSTGRES_BASE_VERSION = "${SPILO_POSTGRES_VERSION}"
  }

  tags = [
    "${SPILO_REPO}:latest",
    "${SPILO_REPO}:${SPILO_VERSION}-latest"
  ]
}

target "spilo_base" {
  inherits = ["shared"]

  context = "https://github.com/zalando/spilo.git#${SPILO_VERSION}:postgres-appliance"

  args = {
    TIMESCALEDB = ""
    PGVERSION = "${SPILO_POSTGRES_VERSION}"
    PGOLDVERSIONS = "${SPILO_POSTGRES_OLD_VERSIONS}"
  }
}

target "columnar" {
  inherits = ["shared"]
  context = "columnar"
  target = "output"
}

target "columnar_13" {
  inherits = ["columnar"]

  args = {
    POSTGRES_BASE_VERSION = 13
  }
}

target "columnar_14" {
  inherits = ["columnar"]

  args = {
    POSTGRES_BASE_VERSION = 14
  }
}

target "columnar_15" {
  inherits = ["columnar"]

  args = {
    POSTGRES_BASE_VERSION = 15
  }
}

target "columnar_16" {
  inherits = ["columnar"]

  args = {
    POSTGRES_BASE_VERSION = 16
  }
}
