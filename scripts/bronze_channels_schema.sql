CREATE TABLE IF NOT EXISTS bronze.channels_test (
    id String,
    created_at DateTime64(3, 'UTC') DEFAULT toDateTime64(0, 3, 'UTC')
)
ENGINE = MergeTree
PRIMARY KEY (id)
ORDER BY (id);
