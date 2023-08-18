#!/usr/bin/env bash

set -euo pipefail

main() {
  install_pgxman

  local _version
  for _version in "$@"; do
    install_extensions "${_version}"
  done
}

install_pgxman () {
  get_architecture || return 1
  local _arch="$RETVAL"

  echo "Installing PGXMan for ${_arch}..."

  wget -O "/tmp/pgxman_linux_${_arch}.deb" "https://github.com/pgxman/release/releases/latest/download/pgxman_linux_${_arch}.deb"
  apt install -y "/tmp/pgxman_linux_${_arch}.deb"
  pgxman update
}

install_extensions () {
  local _pg_version="$1"

  get_extensions $_pg_version || return 1
  local _extensions=( "${RETVAL[@]}" )

  echo "Installing extensions for PostgreSQL $_pg_version:"
  for _ext in "${_extensions[@]}"; do
    echo "- $_ext"
  done

  local _packages=()
  for _ext in "${_extensions[@]}"; do
    _packages+=("$(printf "%s@%s " "$_ext" "$_pg_version")")
  done
  printf -v _packages_args '%s ' "${_packages[@]}"

  pgxman install $_packages_args
}

get_extensions () {
  local _pg_version="$1"

  local _universal_extensions=(
    "pgvector=0.4.4"
  )
  # each is a space-delimited list
  local _per_version_extensions=(
    [13]="pg_hint_plan-13=1.3.8-1"
    [14]="pg_hint_plan-14=1.4.1-1"
    [15]="pg_hint_plan-15=1.5.0-1"
  )

  local _relevant_extensions="${_per_version_extensions[$_pg_version]}"

  # do not quote _relevant_extensions to allow word splitting
  RETVAL=( "${_universal_extensions[@]}" ${_relevant_extensions} )
}

get_architecture () {
  local _cputype _arch
  _cputype="$(uname -m)"

  case "$_cputype" in
  i386 | i486 | i686 | i786 | x86)
    _cputype=386
    ;;

  xscale | arm | armv6l | armv7l | armv8l)
    _cputype=arm
    ;;

  aarch64 | arm64)
    _cputype=arm64
    ;;

  x86_64 | x86-64 | x64 | amd64)
    _cputype=amd64
    ;;

  *)
    err "unknown CPU type: $_cputype"
    ;;
  esac

  RETVAL="$_cputype"
}

main "$@" || exit 1
