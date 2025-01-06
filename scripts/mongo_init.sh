#!/bin/bash
set -e

echo "=== Starting MongoDB data restoration process ==="

COLLECTIONS_COUNT=$(mongo youtube --quiet --eval "db.getCollectionNames().length")

if [ "$COLLECTIONS_COUNT" -eq 0 ]; then
    echo "Database is empty. Starting data restoration..."
    # Restore data using mongorestore
    mongorestore --db youtube --verbose /dump
    echo "=== MongoDB data restoration completed ==="
else
    echo "Database already contains data. Skipping restoration."
fi