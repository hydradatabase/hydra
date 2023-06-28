#!/usr/bin/env bash

set -euo pipefail

main() {
  local pg_version="$1"

  get_architecture || return 1
  local _arch="$RETVAL"

  echo "Installing PGXMan extensions for PostgreSQL $pg_version..."

  wget -O "/tmp/pgxman_linux_${_arch}.deb" "https://github.com/pgxman/release/releases/latest/download/pgxman_linux_${_arch}.deb"
  apt install "/tmp/pgxman_linux_${_arch}.deb"

  pgxman update

  local _extensions=(
    "pgvector=0.4.4"
  )
  local _packages=()
  for _ext in "${_extensions[@]}"; do
    _packages+=("$(printf "%s@%s " "$_ext" "$pg_version")")
  done
  printf -v _packages_args '%s ' "${_packages[@]}"

  pgxman install $_packages_args
}

get_architecture() {
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
