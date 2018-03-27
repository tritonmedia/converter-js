#!/usr/bin/env bash

UPLOAD=6000
DOWNLOAD=250000

echo "Upload ${UPLOAD}KB/s cap. Download ${DOWNLOAD}KB/s cap."
echo ""
echo "starting trickle..."
trickle -s -u ${UPLOAD} -d ${DOWNLOAD} /usr/bin/docker-entrypoint