-- SILVER
-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS silver;

-- Create the silver_youtube_data table
CREATE TABLE IF NOT EXISTS silver.events
(

    channel_id                      String,
    channel_total_video_visit       Nullable(Int64),
    channel_video_count             Nullable(Int64),
    channel_start_date              Nullable(DateTime('UTC')),  -- changed in insert time from start_data timestamp
    channel_followers_count         Nullable(Int64),
    channel_country                 Nullable(String),
    channel_updated_at              Nullable(Date),
    channel_updated_count           Nullable(Int32),
    channel_ingestion_ts           DateTime DEFAULT now(),

    video_uid                  String,
    video_owner_username      String,
    video_visit_count         Int64,
    video_duration            Int64,
    video_posted_date         DateTime('UTC'),
    video_is_deleted          Int8,
    video_update_count        Int32,
    video_ingestion_ts       DateTime('UTC') DEFAULT now(),

    silver_ingestion_ts  DateTime('UTC') DEFAULT now(),

    deduppHash                UInt64,
    popularity_category       String,
    channel_tier              String,
    video_engagement_rate     Float32,
    is_trending               Int8,
    video_age                 Int32,
    channel_age               Int32,
    channel_region            LowCardinality(String)

)
ENGINE = ReplacingMergeTree(silver_ingestion_ts)
PARTITION BY (toYear(video_posted_date))
ORDER BY (channel_id, video_uid, silver_ingestion_ts);


--REMOVED COLs
    -- channel_following_count         Nullable(Int64),
    -- channel_start_date_timestamp    Int64,
    -- channel_avatar_thumbnail        String,
    -- channel_is_official             Nullable(UInt8),
    -- channel_bio_links               String,
    -- video_is_produce_to_kafka         UInt8,
    -- video_tags                        Nullable(String),
    -- video_owner_id                    String,
    -- video_id                         String,
    -- video_owner_name              String,
    -- video_poster                  String,
    -- video_owner_avatar           Nullable(String),
    -- video_posted_timestamp        UInt64,
    -- video_sdate_rss               DateTime('UTC'),
    -- video_sdate_rss_tp            UInt64,
    -- video_comments                Nullable(String),
    -- video_frame                   Nullable(String),
    -- video_like_count              Nullable(UInt64),
    -- video_description             Nullable(String),
    -- video_expire_at               DateTime('UTC'),
    -- video_title                   String,
    -- video_created_at              DateTime('UTC'),
    -- channel_username                Nullable(String),
    -- channel_name                    Nullable(String),
    -- channel_userid                  Nullable(String),
    -- channel_source                 String DEFAULT 'postgres',
    -- video_source                    String DEFAULT 'mongo',
