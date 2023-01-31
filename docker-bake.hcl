variable "POSTGRES_REPO" {
  default = "ghcr.io/hydradatabase/hydra"
}

variable "SPILO_REPO" {
  default = "011789831835.dkr.ecr.us-east-1.amazonaws.com/spilo"
}

variable "SPILO_VERSION" {
  default = "2.1-p9"
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

variable "PYTHON_VERSION" {
  default = "3.9"
}

group "default" {
  targets = ["postgres", "spilo"]
}

target "shared" {
  platforms = [
    "linux/amd64",
    "linux/arm64"
  ]
}

target "postgres" {
  inherits = ["shared"]

  contexts = {
    postgres_base = "docker-image://postgres:${POSTGRES_BASE_VERSION}"

    columnar = "target:columnar_${POSTGRES_BASE_VERSION}"
    http = "target:http_${POSTGRES_BASE_VERSION}"
    mysql = "target:mysql_${POSTGRES_BASE_VERSION}"
    multicorn = "target:multicorn_${POSTGRES_BASE_VERSION}"
    s3 = "target:s3_${POSTGRES_BASE_VERSION}"
  }

  args = {
    PYTHON_VERSION = "${PYTHON_VERSION}"
  }

  tags = [
    "${POSTGRES_REPO}:latest",
    "${POSTGRES_REPO}:${POSTGRES_BASE_VERSION}"
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
    http_13 = "target:http_13"
    http_14 = "target:http_14"
    mysql_13 = "target:mysql_13"
    mysql_14 = "target:mysql_14"
    multicorn_13 = "target:multicorn_13"
    multicorn_14 = "target:multicorn_14"
    s3_13 = "target:s3_spilo_13"
    s3_14 = "target:s3_spilo_14"
  }

  args = {
    POSTGRES_BASE_VERSION = "${SPILO_POSTGRES_VERSION}"
    PYTHON_VERSION = "${PYTHON_VERSION}"
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

target "http" {
  inherits = ["shared"]
  context = "third-party/http"
  target = "output"

  args = {
    PGSQL_HTTP_TAG = "v1.5.0"
  }
}

target "http_13" {
  inherits = ["http"]

  args = {
    POSTGRES_BASE_VERSION = 13
  }

  cache-to = ["type=local,dest=tmp/bake_cache/http_13"]
  cache-from = ["type=local,src=tmp/bake_cache/http_13"]
}

target "http_14" {
  inherits = ["http"]

  args = {
    POSTGRES_BASE_VERSION = 14
  }

  cache-to = ["type=local,dest=tmp/bake_cache/http_14"]
  cache-from = ["type=local,src=tmp/bake_cache/http_14"]
}

target "s3" {
  inherits = ["shared"]
  context = "third-party/s3"
  target = "output"

  args = {
    ARROW_TAG = "apache-arrow-10.0.0"
    AWS_SDK_TAG = "1.10.4"
    PARQUET_S3_FDW_COMMIT = "15dc2c9f0c57dc9f699f6cc645ac82663cea9fe1"
  }
}

target "s3_13" {
  inherits = ["s3"]

  contexts = {
    postgres_base = "docker-image://postgres:13"
  }

  args = {
    POSTGRES_BASE_VERSION = 13
  }

  cache-to = ["type=local,dest=tmp/bake_cache/s3_13"]
  cache-from = ["type=local,src=tmp/bake_cache/s3_13"]
}

target "s3_14" {
  inherits = ["s3"]

  contexts = {
    postgres_base = "docker-image://postgres:14"
  }

  args = {
    POSTGRES_BASE_VERSION = 14
  }

  cache-to = ["type=local,dest=tmp/bake_cache/s3_14"]
  cache-from = ["type=local,src=tmp/bake_cache/s3_14"]
}

target "s3_spilo_13" {
  inherits = ["s3"]

  contexts = {
    postgres_base = "target:spilo_base"
  }

  args = {
    POSTGRES_BASE_VERSION = 13
  }

  cache-to = ["type=local,dest=tmp/bake_cache/s3_spilo_13"]
  cache-from = ["type=local,src=tmp/bake_cache/s3_spilo_13"]
}

target "s3_spilo_14" {
  inherits = ["s3"]

  contexts = {
    postgres_base = "target:spilo_base"
  }

  args = {
    POSTGRES_BASE_VERSION = 14
  }

  cache-to = ["type=local,dest=tmp/bake_cache/s3_spilo_14"]
  cache-from = ["type=local,src=tmp/bake_cache/s3_spilo_14"]
}

target "mysql" {
  inherits = ["shared"]
  context = "third-party/mysql"
  target = "output"

  args = {
    MYSQL_FDW_TAG = "REL-2_8_0"
  }
}

target "mysql_13" {
  inherits = ["mysql"]

  contexts = {
    postgres_base = "docker-image://postgres:13"
  }

  args = {
    POSTGRES_BASE_VERSION = 13
  }

  cache-to = ["type=local,dest=tmp/bake_cache/mysql_13"]
  cache-from = ["type=local,src=tmp/bake_cache/mysql_13"]
}

target "mysql_14" {
  inherits = ["mysql"]

  contexts = {
    postgres_base = "docker-image://postgres:14"
  }

  args = {
    POSTGRES_BASE_VERSION = 14
  }

  cache-to = ["type=local,dest=tmp/bake_cache/mysql_14"]
  cache-from = ["type=local,src=tmp/bake_cache/mysql_14"]
}

target "multicorn" {
  inherits = ["shared"]
  context = "third-party/multicorn"
  target = "output"

  args = {
    PYTHON_VERSION = "${PYTHON_VERSION}"
    MULTICORN_TAG  = "v2.4"
    S3CSV_FDW_COMMIT = "f64e24f9fe3f7dbd1be76f9b8b3b5208f869e5e3"
    GSPREADSHEET_FDW_COMMIT = "d5bc5ae0b2d189abd6d2ee4610bd96ec39602594"
  }
}

target "multicorn_13" {
  inherits = ["multicorn"]

  contexts = {
    postgres_base = "docker-image://postgres:13"
  }

  args = {
    POSTGRES_BASE_VERSION = 13
  }

  cache-to = ["type=local,dest=tmp/bake_cache/multicorn_13"]
  cache-from = ["type=local,src=tmp/bake_cache/multicorn_13"]
}

target "multicorn_14" {
  inherits = ["multicorn"]

  contexts = {
    postgres_base = "docker-image://postgres:14"
  }

  args = {
    POSTGRES_BASE_VERSION = 14
  }

  cache-to = ["type=local,dest=tmp/bake_cache/multicorn_14"]
  cache-from = ["type=local,src=tmp/bake_cache/multicorn_14"]
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

  cache-to = ["type=local,dest=tmp/bake_cache/columnar_13"]
  cache-from = ["type=local,src=tmp/bake_cache/columnar_13"]
}

target "columnar_14" {
  inherits = ["columnar"]

  args = {
    POSTGRES_BASE_VERSION = 14
  }

  cache-to = ["type=local,dest=tmp/bake_cache/columnar_14"]
  cache-from = ["type=local,src=tmp/bake_cache/columnar_14"]
}
