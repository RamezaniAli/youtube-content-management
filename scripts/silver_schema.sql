-- SILVER
-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS silver;

-- Create the silver_youtube_data table
CREATE TABLE IF NOT EXISTS silver.events
(
    channel_id                      String,
    channel_username                String,
    channel_userid                  String,
    channel_name                    String,
    channel_total_video_visit       Int64,
    channel_video_count             Int32,
    channel_start_date              DATE,
    channel_followers_count         Int64,
    channel_country                 LowCardinality(String),
    channel_platform                LowCardinality(String),
    channel_update_count            Int32,
    channel_source                 String DEFAULT 'postgres',
    channel_ingestion_ts           DateTime DEFAULT now(),

    video_uid                  String,
    video_owner_username      String,
    video_title               String,
    video_visit_count         UInt64,
    video_duration            UInt64,
    video_posted_date         DateTime('UTC'),
    video_description         Nullable(String),
    video_is_deleted          UInt8,
    video_created_at          DateTime('UTC'),
    video_expire_at           DateTime('UTC'),
    video_update_count        UInt32,
    video_ingestion_ts       DateTime('UTC') DEFAULT now(),
    video_source             String DEFAULT 'mongo',
    video_raw_object         String,

    silver_ingestion_ts  DateTime('UTC') DEFAULT now(),

    deduppHash UInt64,
    popularity_category     String,
    channel_tier            String,
    video_engagement_rate   Float32,
    is_trending             UInt8,
    days_since_last_update  Int32,
    video_age               Int32,
    channel_region          LowCardinality(String)

)
ENGINE = ReplacingMergeTree(silver_ingestion_ts)
PARTITION BY (toYYYYMM(silver_ingestion_ts), channel_region)
ORDER BY (channel_userid, video_uid, deduppHash, channel_region, silver_ingestion_ts);


--REMOVED COLs
    -- channel_following_count         Nullable(Int64),
    -- channel_start_date_timestamp    Int64,
    -- channel_avatar_thumbnail        String,
    -- channel_is_official             Nullable(UInt8),
    -- channel_bio_links               String,
    -- channel_created_at              Nullable(DateTime('UTC')),
    -- video_is_produce_to_kafka UInt8,
    -- video_tags                Nullable(String),
    -- video_owner_id            String,
    -- video_id                 String,
    -- video_owner_name          String,
    -- video_poster              String,
    -- video_owner_avatar       Nullable(String),
    -- video_posted_timestamp    UInt64,
    -- video_sdate_rss           DateTime('UTC'),
    -- video_sdate_rss_tp        UInt64,
    -- video_comments            Nullable(String),
    -- video_frame               Nullable(String),
    -- video_like_count          Nullable(UInt64),