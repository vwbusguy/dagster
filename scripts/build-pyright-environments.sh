#!/usr/bin/env zsh

# This script builds the environments used by pyright for type-checking. These are a subset of the
# environments used in CI to run tests-- i.e. the ones specified by the `tox.ini` files in each
# package. It works by using `tox 

# The test environments used should be based on the default Python version used to test feature
# branches on BK-- as of 2022-12-13, that is Python 3.9, but this will likely change in the future,
# at which time `TARGET_PYTHON_VERSION` below should be updated.

TARGET_PYTHON_VERSION="py39"

while [[ $1 = -* ]]; do
  opts+=($1)
  shift
done

local rebuild
if (( $opts[(Ie)--rebuild] )); then
  rebuild=1
fi

if ! [[ $(python --version) == "Python 3.9"* ]]; then
    echo "This script must be run with Python 3.9"
    exit 1
fi

typeset -a failed

local packages
if [[ $# -eq 0 ]]; then
  packages=( $(git ls-files '*/tox.ini' | xargs -n1 dirname) )
else
  packages=( $@ )
fi

for pkg in $packages; do
  local tox_config="$pkg/tox.ini"
  echo "Building tox env for $(dirname $tox_config)..."
  printf -- '=%.0s' {1..80}; printf "\n"
  tox run ${rebuild:+-r} --notest -e $TARGET_PYTHON_VERSION -c $tox_config
  if [[ $? -ne 0 ]]; then
    failed+=($(dirname $tox_config))
  fi
  echo # blank line
done

echo "Failed to build environments for:"
echo ${(F)failed[@]}

