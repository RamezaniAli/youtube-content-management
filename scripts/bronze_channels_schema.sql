CREATE TABLE IF NOT EXISTS bronze.channels
(
    id                      String,
    username                Nullable(String),
    userid                  Nullable(String),
    avatar_thumbnail        Nullable(String),
    is_official             Nullable(UInt8),
    name                    Nullable(String),
    bio_links               Nullable(String),
    total_video_visit       Nullable(Int64),
    video_count             Nullable(Int32),
    start_date              Nullable(DATE),
    start_date_timestamp    Nullable(Int64),
    followers_count         Nullable(UInt64),
    following_count         Nullable(UInt64),
    is_deleted              Nullable(UInt8),
    country                 Nullable(String),
    platform                Nullable(String),
    created_at              Nullable(DateTime('UTC')),
    updated_at              Nullable(DateTime('UTC')),
    update_count            Nullable(UInt32),
    offset                  UInt64,
    --these two added:
    _source                 String DEFAULT 'postgres',
    _ingestion_ts           DateTime DEFAULT now(),
)
ENGINE = MergeTree()
ORDER BY (id);

