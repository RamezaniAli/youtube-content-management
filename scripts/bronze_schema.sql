-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS bronze;

-- Create the channels table if it doesn't exist
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
ORDER BY (userid, created_at);


-- Create the videos table if it doesn't exist
CREATE TABLE IF NOT EXISTS bronze.videos
(
    id                  UInt64,
    owner_username      String,
    owner_id            String,
    title               String,
    tags                Nullable(String),
    uid                 String,
    visit_count         UInt64,
    owner_name          String,
    poster              String,
    owener_avatar       Nullable(String), -- typo: owner_avatar
    duration            Nullable(UInt64),
    posted_date         DateTime('UTC'),
    posted_timestamp    UInt64,
    sdate_rss           DateTime('UTC'),
    sdate_rss_tp        UInt64,
    comments            Nullable(String),
    frame               Nullable(String),
    like_count          Nullable(UInt64),
    description         Nullable(String),
    is_deleted          UInt8,
    created_at          DateTime('UTC'),
    expire_at           DateTime('UTC'),
    is_produce_to_kafka UInt8,
    update_count        UInt32,

    _ingestion_ts       DateTime('UTC') DEFAULT now(),
    _source             String DEFAULT 'mongo',
    _raw_object         String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)     
ORDER BY (owner_id, created_at);
