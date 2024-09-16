#!/bin/bash
set -eo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
packages=(
    gflags 
    tabulate
    rapidjson
)

source ${BASE_DIR}/repository.sh

OPTS="$(getopt \
    -n "$0" \
    -o 'j:h' \
    -l 'build-type:,prefix:,help' \
    -- "$@")"

if [[ $? -ne 0 ]]; then
    echo "Error parsing arguments."
    exit 1
fi
eval set -- "${OPTS}"

usage() {
    echo 'test'
}

BUILD_TYPE="Release"
SRC_DIR=${BASE_DIR}/src
INSTALL_DIR=${BASE_DIR}/installed
PARALLEL=1
while true; do
  case "$1" in
    --build-type)
        BUILD_TYPE=$1
        shift 2
        ;;
    --prefix)
        INSTALL_DIR=$1
        shift 2
        ;;
    -j)
        PARALLEL=$2
        shift 2
        ;;
    --help)
        usage
        exit 0

        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Usage: `basename $0` -h, --help for help"
        usage
        exit 1
        ;;
  esac
done

mkdir -p ${SRC_DIR} ${INSTALL_DIR}

CMAKE=cmake3
if ! which $CMAKE > /dev/null 2>&1; then
    CMAKE=cmake
fi

for package in "${packages[@]}"; do
    cd ${SRC_DIR}

    src="${SRC_DIR}/${package}"
    installed="${INSTALL_DIR}/${package}"
    name=$(echo "${package}" | tr '[:lower:]' '[:upper:]')
    repo="${name}_REPO"
    tag="${name}_TAG"

    echo "Build and install package: ${package}"

    git clone ${!repo}
    cd ${src}
    git checkout -b build-branch ${!tag}

    mkdir -p ${src}/bld
    cd ${src}/bld
    $CMAKE -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
           -DCMAKE_INSTALL_PREFIX=${INSTALL_DIR} ..
    
    $CMAKE --build . -j ${PARALLEL}
    $CMAKE --install .
done