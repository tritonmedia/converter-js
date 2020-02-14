# Required for globs to work correctly
SHELL=/bin/bash

.PHONY: render-circle
render-circle:
	@if [[ ! -e /tmp/jsonnet-libs ]]; then git clone git@github.com:tritonmedia/jsonnet-libs /tmp/jsonnet-libs; else cd /tmp/jsonnet-libs; git pull; fi
	JSONNET_PATH=/tmp/jsonnet-libs jsonnet .circleci/circle.jsonnet | yq . -y > .circleci/config.yml