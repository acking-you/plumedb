#!/bin/bash
TAG ?= dev

build-plumedb-server:
	bash ./scripts/build/plumedb-server.sh

release-plumedb-server-docker-image: build-plumedb-server
	bash ./scripts/release/plumedb-server.sh ${TAG}

.PHONY: