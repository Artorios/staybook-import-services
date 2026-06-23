#!/bin/sh

./import-service
echo "import-service finished (exit $?), container stays running"

exec sleep infinity
