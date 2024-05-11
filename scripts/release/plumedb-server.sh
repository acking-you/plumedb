#!/bin/bash
SCRIPT_DIR=$(cd `dirname $0`; pwd)
export PROJECT_DIR="$SCRIPT_DIR/../.."
export BIN_NAME="plumedb-server"
export TARGET_NAME="x86_64-unknown-linux-musl"

if [ $# -lt 1 ]; then
    echo "Usage: $0 <TAG>,we will use x86_64_musl as default"
    TAG="x86_64_musl"
else
    TAG="$1"
fi


echo "we use TAG:$TAG"

bash $SCRIPT_DIR/docker-image-release.sh $TAG