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
  default = "3.11"
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
    postgres_base = "docker-image://postgres:${POSTGRES_BASE_VERSION}-bookworm"

    columnar = "target:columnar_${POSTGRES_BASE_VERSION}"
    http = "target:http_${POSTGRES_BASE_VERSION}"
    mysql = "target:mysql_${POSTGRES_BASE_VERSION}"
    multicorn = "target:multicorn_${POSTGRES_BASE_VERSION}"
    s3 = "target:s3_${POSTGRES_BASE_VERSION}"
    ivm = "target:ivm_${POSTGRES_BASE_VERSION}"
  }

  args = {
    POSTGRES_BASE_VERSION = "${POSTGRES_BASE_VERSION}"
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
    columnar_15 = "target:columnar_15"
    http_13 = "target:http_13"
    http_14 = "target:http_14"
    http_15 = "target:http_15"
    mysql_13 = "target:mysql_13"
    mysql_14 = "target:mysql_14"
    mysql_15 = "target:mysql_15"
    multicorn_13 = "target:multicorn_13"
    multicorn_14 = "target:multicorn_14"
    multicorn_15 = "target:multicorn_15"
    s3_13 = "target:s3_spilo_13"
    s3_14 = "target:s3_spilo_14"
    s3_15 = "target:s3_spilo_15"
    ivm_13 = "target:ivm_13"
    ivm_14 = "target:ivm_14"
    ivm_15 = "target:ivm_15"
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

target "http_15" {
  inherits = ["http"]

  args = {
    POSTGRES_BASE_VERSION = 15
  }

  cache-to = ["type=local,dest=tmp/bake_cache/http_15"]
  cache-from = ["type=local,src=tmp/bake_cache/http_15"]
}

target "s3" {
  inherits = ["shared"]
  context = "third-party/s3"
  target = "output"

  args = {
    ARROW_TAG = "apache-arrow-10.0.0"
    AWS_SDK_TAG = "1.10.57"
    PARQUET_S3_FDW_COMMIT = "3798786831635e5b9cce5dbf33826541c3852809"
  }
}

target "s3_13" {
  inherits = ["s3"]

  contexts = {
    postgres_base = "docker-image://postgres:13-bookworm"
  }

  args = {
    POSTGRES_BASE_VERSION = 13
  }

  cache-to = ["type=local,dest=tmp/bake_cache/s3_13"]
  cache-from = ["type=local,src=tmp/bake_cache/s3_13"]
}

target "s3_15" {
  inherits = ["s3"]

  contexts = {
    postgres_base = "docker-image://postgres:15-bookworm"
  }

  args = {
    POSTGRES_BASE_VERSION = 15
  }

  cache-to = ["type=local,dest=tmp/bake_cache/s3_15"]
  cache-from = ["type=local,src=tmp/bake_cache/s3_15"]
}

target "s3_14" {
  inherits = ["s3"]

  contexts = {
    postgres_base = "docker-image://postgres:14-bookworm"
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

target "s3_spilo_15" {
  inherits = ["s3"]

  contexts = {
    postgres_base = "target:spilo_base"
  }

  args = {
    POSTGRES_BASE_VERSION = 15
  }

  cache-to = ["type=local,dest=tmp/bake_cache/s3_spilo_15"]
  cache-from = ["type=local,src=tmp/bake_cache/s3_spilo_15"]
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
    postgres_base = "docker-image://postgres:13-bookworm"
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
    postgres_base = "docker-image://postgres:14-bookworm"
  }

  args = {
    POSTGRES_BASE_VERSION = 14
  }

  cache-to = ["type=local,dest=tmp/bake_cache/mysql_14"]
  cache-from = ["type=local,src=tmp/bake_cache/mysql_14"]
}

target "mysql_15" {
  inherits = ["mysql"]

  contexts = {
    postgres_base = "docker-image://postgres:15-bookworm"
  }

  args = {
    POSTGRES_BASE_VERSION = 15
  }

  cache-to = ["type=local,dest=tmp/bake_cache/mysql_15"]
  cache-from = ["type=local,src=tmp/bake_cache/mysql_15"]
}

target "multicorn" {
  inherits = ["shared"]
  context = "third-party/multicorn"
  target = "output"

  args = {
    PYTHON_VERSION = "${PYTHON_VERSION}"
    MULTICORN_TAG  = "b68b75c253be72bdfd5b24bf76705c47c238d370"
    S3CSV_FDW_COMMIT = "f64e24f9fe3f7dbd1be76f9b8b3b5208f869e5e3"
    GSPREADSHEET_FDW_COMMIT = "d5bc5ae0b2d189abd6d2ee4610bd96ec39602594"
  }
}

target "multicorn_13" {
  inherits = ["multicorn"]

  contexts = {
    postgres_base = "docker-image://postgres:13-bookworm"
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
    postgres_base = "docker-image://postgres:14-bookworm"
  }

  args = {
    POSTGRES_BASE_VERSION = 14
  }

  cache-to = ["type=local,dest=tmp/bake_cache/multicorn_14"]
  cache-from = ["type=local,src=tmp/bake_cache/multicorn_14"]
}

target "multicorn_15" {
  inherits = ["multicorn"]

  contexts = {
    postgres_base = "docker-image://postgres:15-bookworm"
  }

  args = {
    POSTGRES_BASE_VERSION = 15
  }

  cache-to = ["type=local,dest=tmp/bake_cache/multicorn_15"]
  cache-from = ["type=local,src=tmp/bake_cache/multicorn_15"]
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

target "columnar_15" {
  inherits = ["columnar"]

  args = {
    POSTGRES_BASE_VERSION = 15
  }

  cache-to = ["type=local,dest=tmp/bake_cache/columnar_15"]
  cache-from = ["type=local,src=tmp/bake_cache/columnar_15"]
}

target "ivm" {
  inherits = ["shared"]
  context = "third-party/ivm"
  target = "output"

  args = {
    PGSQL_IVM_TAG = "v1.5.1"
  }
}

target "ivm_13" {
  inherits = ["ivm"]

  contexts = {
    postgres_base = "docker-image://postgres:13-bookworm"
  }

  args = {
    POSTGRES_BASE_VERSION = 13
  }

  cache-to = ["type=local,dest=tmp/bake_cache/ivm_13"]
  cache-from = ["type=local,src=tmp/bake_cache/ivm_13"]
}

target "ivm_14" {
  inherits = ["ivm"]

  contexts = {
    postgres_base = "docker-image://postgres:14-bookworm"
  }

  args = {
    POSTGRES_BASE_VERSION = 14
  }

  cache-to = ["type=local,dest=tmp/bake_cache/ivm_14"]
  cache-from = ["type=local,src=tmp/bake_cache/ivm_14"]
}

target "ivm_15" {
  inherits = ["ivm"]

  contexts = {
    postgres_base = "docker-image://postgres:15-bookworm"
  }

  args = {
    POSTGRES_BASE_VERSION = 15
  }

  cache-to = ["type=local,dest=tmp/bake_cache/ivm_15"]
  cache-from = ["type=local,src=tmp/bake_cache/ivm_15"]
}

target "ivm" {
  inherits = ["shared"]
  context = "third-party/ivm"
  target = "output"

  args = {
    PGSQL_IVM_TAG = "v1.5.1"
  }
}

target "ivm_13" {
  inherits = ["ivm"]

  contexts = {
    postgres_base = "docker-image://postgres:13-bookworm"
  }

  args = {
    POSTGRES_BASE_VERSION = 13
  }

  cache-to = ["type=local,dest=tmp/bake_cache/ivm_13"]
  cache-from = ["type=local,src=tmp/bake_cache/ivm_13"]
}

target "ivm_14" {
  inherits = ["ivm"]

  contexts = {
    postgres_base = "docker-image://postgres:14-bookworm"
  }

  args = {
    POSTGRES_BASE_VERSION = 14
  }

  cache-to = ["type=local,dest=tmp/bake_cache/ivm_14"]
  cache-from = ["type=local,src=tmp/bake_cache/ivm_14"]
}

target "ivm_15" {
  inherits = ["ivm"]

  contexts = {
    postgres_base = "docker-image://postgres:15-bookworm"
  }

  args = {
    POSTGRES_BASE_VERSION = 15
  }

  cache-to = ["type=local,dest=tmp/bake_cache/ivm_15"]
  cache-from = ["type=local,src=tmp/bake_cache/ivm_15"]
}
