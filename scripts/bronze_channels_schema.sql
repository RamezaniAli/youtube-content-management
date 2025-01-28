CREATE TABLE IF NOT EXISTS bronze.channels
(
    id                      String,
    username                String,
    userid                  String,
    avatar_thumbnail        String,
    is_official             Nullable(UInt8),
    name                    String,
    bio_links               String,
    total_video_visit       Int64,
    video_count             Int32,
    start_date              DATE,
    start_date_timestamp    Int64,
    followers_count         Nullable(UInt64),
    following_count         Nullable(UInt64),
    is_deleted              UInt8,
    country                 LowCardinality(String),
    platform                LowCardinality(String),
    created_at              Nullable(DateTime('UTC')),
    updated_at              Nullable(DateTime('UTC')),
    update_count            UInt32,
    offset                  UInt64,
    --these two added:
    _source                 String DEFAULT 'postgres',
    _ingestion_ts           DateTime DEFAULT now(),
)
ENGINE = MergeTree()
ORDER BY (id, userid);
