#!/usr/bin/env bash

set -euo pipefail

main() {
  install_pgxman

  for _file in "$@"; do
    echo "Installing extensions from ${_file}..."
    pgxman install --file "$_file"
  done
}

install_pgxman() {
  get_architecture || return 1
  local _arch="$RETVAL"

  echo "Installing PGXMan for ${_arch}..."

  curl --silent --show-error --fail --location "https://github.com/pgxman/release/releases/latest/download/pgxman_linux_${_arch}.deb" --output "/tmp/pgxman_linux_${_arch}.deb"
  apt update
  apt install -y "/tmp/pgxman_linux_${_arch}.deb"
  pgxman update
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
