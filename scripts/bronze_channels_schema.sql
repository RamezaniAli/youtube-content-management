CREATE TABLE IF NOT EXISTS bronze.channels_test
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
    start_date              DateTime('UTC'),  
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
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (userid);
