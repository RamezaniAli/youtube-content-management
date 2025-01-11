#!/bin/bash
set -e

until mongo --eval 'db.runCommand({ ping: 1 })' &>/dev/null; do
  sleep 1
done

MONGO_USERNAME="${MONGO_INITDB_ROOT_USERNAME}"
MONGO_PASSWORD="${MONGO_INITDB_ROOT_PASSWORD}"
AUTH_STRING=""

if [ -n "$MONGO_USERNAME" ] && [ -n "$MONGO_PASSWORD" ]; then
  AUTH_STRING="--username $MONGO_USERNAME --password $MONGO_PASSWORD --authenticationDatabase admin"
fi

mongorestore $AUTH_STRING --drop --dir /dump --numInsertionWorkersPerCollection=5

echo "Done!"
