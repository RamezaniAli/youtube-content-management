CREATE MATERIALIZED VIEW IF NOT EXISTS gold.video_engagement_metrics
ENGINE = AggregatingMergeTree()
ORDER BY ifNull(channel_country, 'Unknown')
PARTITION BY toYear(video_posted_date)
POPULATE
AS
SELECT
    -- Channel dimensions
    channel_id,
    channel_country,
    channel_tier,

    -- Video dimensions
    video_uid,
    video_posted_date,

    -- Base metrics
    video_visit_count,
    video_duration,
    video_age,

    -- Already calculated metrics from silver
    video_engagement_rate,
    is_trending,
    popularity_category,

    (video_visit_count::Float32 / nullIf(channel_followers_count, 0)) *
    CASE
        WHEN channel_tier = 'Tier 1' THEN log2(channel_followers_count + 1)
        WHEN channel_tier = 'Tier 2' THEN log2(channel_followers_count + 1) * 0.8
        ELSE log2(channel_followers_count + 1) * 0.6
    END as weighted_engagement_rate,

    -- Additional calculated metrics
    toFloat64(video_visit_count) / nullIf(video_age, 0) as views_per_day,

    -- Channel context
    channel_video_count,
    channel_followers_count,
    channel_total_video_visit

FROM silver.events
WHERE
    video_visit_count > 0
    AND video_is_deleted = 0
    AND channel_followers_count > 0;


CREATE MATERIALIZED VIEW IF NOT EXISTS gold.channel_trend_analysis
ENGINE = AggregatingMergeTree()
ORDER BY ( ifNull(channel_country, 'Unknown'), month_of_year, day_of_week, hour_of_day)
POPULATE
AS
SELECT
    -- Geographic dimension
    channel_country,
    channel_tier,

    -- Temporal dimensions
    toMonth(video_posted_date) as month_of_year,
    toDayOfWeek(video_posted_date) as day_of_week,
    toHour(video_posted_date) as hour_of_day,

    -- Channel productivity metrics
    channel_id,
    channel_video_count,
    dateDiff('day', channel_start_date, silver_ingestion_ts) as channel_age_days,
    channel_video_count / (dateDiff('day', channel_start_date, silver_ingestion_ts) + 1) as videos_per_day,

    -- Video metrics
    video_uid,
    video_visit_count,
    video_engagement_rate,
    is_trending,
    video_age,

    -- Normalized metrics
    video_visit_count / nullIf(video_age, 0) as daily_views

FROM silver.events
WHERE video_is_deleted = 0;


CREATE MATERIALIZED VIEW IF NOT EXISTS gold.geographical_distribution
ENGINE = AggregatingMergeTree()
ORDER BY (country, region)
POPULATE
AS
SELECT
    ifNull(channel_country, 'Unknown') AS country,
    ifNull(channel_region, 'Unknown') AS region,

    -- Channel metrics
    count(DISTINCT channel_id) AS total_channels,
    sum(channel_followers_count) AS total_followers,
    sum(channel_video_count) AS total_videos,
    sum(channel_total_video_visit) AS total_views,

    -- Average metrics
    round(sum(channel_followers_count) / count(DISTINCT channel_id), 2) AS avg_followers_per_channel,
    round(sum(channel_video_count) / count(DISTINCT channel_id), 2) AS avg_videos_per_channel,
    round(sum(channel_total_video_visit) / count(DISTINCT channel_id), 2) AS avg_views_per_channel,

    -- Channel size distribution
    sum(if(channel_followers_count < 1000, 1, 0)) AS micro_channels,
    sum(if(channel_followers_count >= 1000 AND channel_followers_count < 10000, 1, 0)) AS small_channels,
    sum(if(channel_followers_count >= 10000 AND channel_followers_count < 100000, 1, 0)) AS medium_channels,
    sum(if(channel_followers_count >= 100000 AND channel_followers_count < 1000000, 1, 0)) AS large_channels,
    sum(if(channel_followers_count >= 1000000, 1, 0)) AS mega_channels

FROM
(
    -- Deduplicate channels to get their latest stats
    SELECT
        channel_id,
        channel_country,
        channel_region,
        argMax(channel_followers_count, silver_ingestion_ts) AS channel_followers_count,
        argMax(channel_video_count, silver_ingestion_ts) AS channel_video_count,
        argMax(channel_total_video_visit, silver_ingestion_ts) AS channel_total_video_visit
    FROM silver.events
    GROUP BY channel_id, channel_country, channel_region
)
GROUP BY country, region;


CREATE MATERIALIZED VIEW IF NOT EXISTS gold.top_channels_metrics
ENGINE = AggregatingMergeTree()
ORDER BY (channel_id)
POPULATE
AS
SELECT
    channel_id,
    any(channel_country) AS country,
    any(channel_region) AS region,
    max(channel_followers_count) AS followers_count,
    max(video_visit_count) AS total_views,
    count(DISTINCT video_uid) AS video_count
FROM silver.events
GROUP BY channel_id;


CREATE MATERIALIZED VIEW IF NOT EXISTS gold.channel_metrics
ENGINE = AggregatingMergeTree()
ORDER BY (ifNull(channel_region, 'Unknown'), ifNull(channel_country, 'Unknown'))
PARTITION BY ifNull(channel_region, 'Unknown')
POPULATE
AS
SELECT
    channel_id,
    channel_country,
    channel_region,
    channel_tier,
    channel_start_date,
    channel_followers_count,
    channel_video_count,
    channel_total_video_visit,

    -- Derived metrics
    toFloat64(channel_video_count) / nullIf(channel_followers_count, 0) as videos_per_follower,
    toFloat64(channel_total_video_visit) / nullIf(channel_followers_count, 0) as visits_per_follower,
    toFloat64(channel_video_count) / nullIf(dateDiff('day', channel_start_date, silver_ingestion_ts) + 1, 0) as videos_per_day,
    toFloat64(channel_followers_count) / nullIf(dateDiff('day', channel_start_date, silver_ingestion_ts) + 1, 0) as followers_per_day
FROM silver.events
WHERE channel_followers_count > 0;
