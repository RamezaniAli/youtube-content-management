#!/bin/bash
set -e 

DATABASE_NAME="utube"
CORRECT_COUNT=6328629
DUMP_DIRECTORY="/backup"

COLLECTIONS_COUNT=$(mongo --host localhost \
    -u "utube" \
    -p "utube" \
    --authenticationDatabase admin \
    "$DATABASE_NAME" \
    --quiet --eval "db.videos.count()")

if [ "$COLLECTIONS_COUNT" -eq 0 ]; then
    echo "Database is empty. Starting data restoration..."
    mongorestore --host localhost \
        -u "utube" \
        -p "utube" \
        --authenticationDatabase admin \
        --db "$DATABASE_NAME" \
        --numInsertionWorkersPerCollection 4 \
        --verbose "$DUMP_DIRECTORY"

    COLLECTIONS_COUNT=$(mongo --host localhost \
        -u "utube" \
        -p "utube" \
        --authenticationDatabase admin \
        "$DATABASE_NAME" \
        --quiet --eval "db.videos.count()")
    echo "=== $COLLECTIONS_COUNT video added to MongoDB"
    
elif [ "$COLLECTIONS_COUNT" -ne "$CORRECT_COUNT" ]; then
    echo "Database contains incorrect number of documents. Cleaning and restoring data..."
    mongo --host localhost \
        -u "utube" \
        -p "utube" \
        --authenticationDatabase admin \
        "$DATABASE_NAME" \
        --quiet --eval "db.dropDatabase()"
    echo "Database cleaned. Restoring..."
    mongorestore --host localhost \
        -u "utube" \
        -p "utube" \
        --authenticationDatabase admin \
        --db "$DATABASE_NAME" \
        --numInsertionWorkersPerCollection 4 \
        --verbose "$DUMP_DIRECTORY"
    echo "=== MongoDB data restoration completed after cleaning ==="
else
    echo "Database contains the correct number of documents. Skipping restoration."
fi
