-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS gold;

-- Channel Growth Over Time

CREATE TABLE IF NOT EXISTS gold.channel_growth
(
    channel_userid           String,
    channel_name             String,
    channel_country          LowCardinality(String),

    snapshot_date            Date,
    followers_count_state    AggregateFunction(sum, Int64),
    total_video_visits_state AggregateFunction(sum, Int64),
    video_count_state        AggregateFunction(sum, Int32),

    gold_ingestion_ts        DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (channel_country, channel_userid, snapshot_date);


-- top performing channels

CREATE TABLE IF NOT EXISTS gold.top_performing_channels
(
    channel_userid           String,
    channel_name             String,
    channel_country          LowCardinality(String),

    snapshot_date            Date,
    highest_followers_count  AggregateFunction(max, Int64),
    highest_video_visits     AggregateFunction(max, Int64),
    highest_video_count      AggregateFunction(max, Int32),

    gold_ingestion_ts        DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (channel_userid, snapshot_date);

-- video engagement metrics

CREATE TABLE IF NOT EXISTS gold.video_engagement_metrics
(
    video_uid               String,
    video_title             String,
    channel_userid          String,
    channel_name            String,
    video_posted_date       DateTime('UTC'),

    total_visits_state         AggregateFunction(sum, UInt64),
    avg_engagement_rate_state  AggregateFunction(avg, Float32),

    gold_ingestion_ts       DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(video_posted_date)
ORDER BY (video_uid, video_posted_date);


-- content popularity analysis

CREATE TABLE IF NOT EXISTS gold.content_popularity_analysis
(
    video_uid               String,
    video_title             String,
    visit_count_state       AggregateFunction(sum, UInt64),
    snapshot_date            Date,

    gold_ingestion_ts       DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (video_uid, snapshot_date);


-- geographic distribution of channels

CREATE TABLE IF NOT EXISTS gold.geographic_distribution_of_channels
(
    channel_country        LowCardinality(String),
    channel_region         LowCardinality(String),
    total_followers_count  AggregateFunction(sum, Int64),
    total_video_visits     AggregateFunction(sum, UInt64),
    total_channels_state   AggregateFunction(count, String),
    snapshot_date            Date,

    gold_ingestion_ts      DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = AggregatingMergeTree()
PARTITION BY (channel_region, toYYYYMM(snapshot_date))
ORDER BY (channel_region, channel_country, snapshot_date);


-- channel activity and update trends

CREATE TABLE IF NOT EXISTS gold.channel_activity_and_update_trends
(
    channel_userid              String,
    channel_name                String,
    channel_country             LowCardinality(String),
    snapshot_date               Date,

    total_update_count_state    AggregateFunction(sum, Int32),
    avg_video_age_state         AggregateFunction(avg, Int32),
    total_videos_state          AggregateFunction(sum, Int32),
    is_trending_state           AggregateFunction(sum, Int32),
    channel_start_date      DateTime64(3, 'UTC'),

    gold_ingestion_ts       DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (channel_country, channel_userid, snapshot_date);