CREATE TABLE IF NOT EXISTS bronze.channels
(
    id                      String PRIMARY KEY,
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
    followers_count         Nullable(Int64),
    following_count         Nullable(Int64),
    country                 LowCardinality(String),
    platform                LowCardinality(String),
    created_at              Nullable(DateTime('UTC')),
    update_count            Int32,
    --these two added:
    _source                 String DEFAULT 'postgres',
    _ingestion_ts           DateTime DEFAULT now(),

    -- for new data
    is_deleted             Nullable(UInt8),
    updated_at             Nullable(UInt64),

    -- incremental data loading
    offset                UInt64,
    
)
ENGINE = MergeTree()
ORDER BY (id, userid);
