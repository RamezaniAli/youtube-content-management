-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS silver;

-- Create the silver_youtube_data table
CREATE TABLE IF NOT EXISTS silver.events
(
    video_uid             String,
    channel_userid        String,
    channel_username      String,
    channel_name          String,
    channel_avatar_thumbnail String,
   -- channel_is_official   UInt8,  all values are NULL
    channel_bio_links     String,
    channel_total_video_visit Int64,
    channel_video_count   Int32,

    -- Standardized Date
    channel_start_date    DateTime64(3, 'UTC'),
    channel_start_date_ts Int64, -- Keeping original timestamp if needed

    channel_followers_count Int64, -- have null values, imputed with mean of other channels with approximatly same number of video_count, total_video_visit, country
   -- channel_following_count Nullable(Int64), all values are NULL
    channel_country       LowCardinality(String),
    channel_platform      LowCardinality(String),

    -- Standardized Date
   -- channel_created_at    Nullable(DateTime64(3, 'UTC')), all values are NULL
   -- channel_created_at_ts   Nullable(Int64), -- Keeping original timestamp if needed , all values are NULL
    channel_update_count  Int32,

    video_owner_username  String,
    video_title           String,
    -- video_tags            Nullable(Array(String)), -- Transforming the comma-separated string all values are Null
    video_visit_count     UInt64,
    video_owner_name      String,
    video_poster          String,
    video_duration        Nullable(UInt64), -- have null values, imputed with mean of other videos from this channel

    -- Standardized Date
    video_posted_date     DateTime64(3, 'UTC'),
    video_posted_date_ts    UInt64, -- Keeping original timestamp

    -- video_like_count      Nullable(UInt64), all values are NULL
    video_description     Nullable(String),
    video_is_deleted      UInt8,
    
    -- Standardized Date
    video_created_at      DateTime64(3, 'UTC'),
    video_created_at_ts   DateTime64(3, 'UTC'), -- standardize it
    video_expire_at       DateTime64(3, 'UTC'),
    video_expire_at_ts    DateTime64(3, 'UTC'), -- standardize it
    video_update_count    UInt32,

    -- Metadata from bronze tables
    video_ingestion_ts   DateTime64(3, 'UTC'), -- Standardized
    video_source         String,
    channel_ingestion_ts DateTime64(3, 'UTC'), -- Standardized
    channel_source       String,
    silver_ingestion_ts  DateTime64(3, 'UTC') DEFAULT now(),


    --deduolication with hashing
    record_hash String,

    -- Data Enrichment Columns
    engagement_rate       Nullable(Float64),
    popularity_category   Nullable(String),
    channel_tier          Nullable(String),
    days_since_update     Nullable(Int32)
)
ENGINE = ReplacingMergeTree(silver_ingestion_ts)
ORDER BY (channel_userid, video_uid, record_hash, silver_ingestion_ts)
PARTITION BY toYYYYMM(silver_ingestion_ts);