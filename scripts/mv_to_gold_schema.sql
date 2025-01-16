CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_video_distribution_aggregated
ENGINE = AggregatingMergeTree()
PARTITION BY distribution_year
ORDER BY (distribution_date)
AS
SELECT
    toDate(video_posted_date) AS distribution_date,
    toDayOfWeek(video_posted_date) AS distribution_day_of_week,
    toStartOfMonth(video_posted_date) AS distribution_month,
    toQuarter(video_posted_date) AS distribution_quarter,
    toYear(video_posted_date) AS distribution_year,
    count() AS total_videos
FROM silver.events
GROUP BY distribution_date, distribution_day_of_week, distribution_month, distribution_quarter, distribution_year;


CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_video_age_impact_on_visits
ENGINE = AggregatingMergeTree()
ORDER BY (video_age)
AS
SELECT
    video_age,
    avg(video_visit_count) AS avg_visit_count
FROM silver.events
GROUP BY video_age;


CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_engagement_rate_hourly
ENGINE = AggregatingMergeTree()
ORDER BY (hour)
AS
SELECT
toHour(video_posted_date) AS hour,
avg(video_engagement_rate) AS avg_engagement_rate
FROM silver.events
GROUP BY hour;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_engagement_rate_daily
ENGINE = AggregatingMergeTree()
ORDER BY (day_of_week)
AS
SELECT
    toDayOfWeek(video_posted_date) AS day_of_week,
    avg(video_engagement_rate) AS avg_engagement_rate
FROM silver.events
GROUP BY day_of_week;


CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_trending_videos_ratio_aggregated
ENGINE = AggregatingMergeTree()
PARTITION BY year
ORDER BY (post_date, hour_of_day)
AS
SELECT
    toDate(video_posted_date) AS post_date,
    toDayOfWeek(video_posted_date) AS day_of_week,
    toStartOfMonth(video_posted_date) AS month_of_year,
    toYear(video_posted_date) AS year,
    toHour(video_posted_date) AS hour_of_day,
    sum(is_trending) AS trending_count,
    count() AS total_videos,
    sum(is_trending) / count() AS trending_ratio
FROM silver.events
GROUP BY post_date, day_of_week, month_of_year, year, hour_of_day;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_avg_video_visits_by_region
ENGINE = AggregatingMergeTree()
PARTITION BY channel_region
ORDER BY (channel_country, channel_region)
AS
SELECT
    channel_country,
    channel_region,
    avg(video_visit_count) AS avg_visit_count
FROM silver.events
GROUP BY channel_country, channel_region;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_engagement_rate_distribution_by_location
ENGINE = AggregatingMergeTree()
PARTITION BY channel_region
ORDER BY (channel_country, channel_region)
AS
SELECT
    channel_country,
    channel_region,
    avg(video_engagement_rate) AS avg_engagement_rate
FROM silver.events
GROUP BY channel_country, channel_region;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_trending_videos_ratio_by_region
ENGINE = AggregatingMergeTree()
PARTITION BY channel_region
ORDER BY (channel_country, channel_region)
AS
SELECT
    channel_country,
    channel_region,
    sum(is_trending) / count() AS trending_ratio
FROM silver.events
GROUP BY channel_country, channel_region;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_preferred_video_duration_by_region
ENGINE = AggregatingMergeTree()
PARTITION BY channel_region
ORDER BY (channel_country, channel_region)
AS
SELECT
    channel_country,
    channel_region,
    avg(video_duration) AS avg_video_duration
FROM silver.events
GROUP BY channel_country, channel_region;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_video_density_by_region
ENGINE = AggregatingMergeTree()
PARTITION BY channel_region
ORDER BY (channel_country, channel_region)
AS
SELECT
    channel_country,
    channel_region,
    count() AS total_videos

FROM silver.events
GROUP BY channel_country, channel_region;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_video_success_rate_by_region
ENGINE = AggregatingMergeTree()
PARTITION BY channel_region
ORDER BY (channel_country, channel_region)
AS
SELECT
    channel_country,
    channel_region,
    sum(if(video_visit_count > 1000, 1, 0)) / count() AS success_rate
FROM silver.events
GROUP BY channel_country, channel_region;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_channel_tier_distribution_by_region
ENGINE = AggregatingMergeTree()
PARTITION BY channel_region
ORDER BY (channel_country, channel_region, channel_tier)
AS
SELECT
    channel_country,
    channel_region,
    channel_tier,
    count() AS total_channels
FROM silver.events
GROUP BY channel_country, channel_region, channel_tier;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_avg_channel_followers_by_region
ENGINE = AggregatingMergeTree()
PARTITION BY channel_region
ORDER BY (channel_country, channel_region)
AS
SELECT
    channel_country,
    channel_region,
    avg(channel_followers_count) AS avg_followers_count
FROM silver.events
GROUP BY channel_country, channel_region;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_total_channel_visits_by_region
ENGINE = AggregatingMergeTree()
PARTITION BY channel_region
ORDER BY (channel_country, channel_region)
AS
SELECT
    channel_country,
    channel_region,
    sum(channel_total_video_visit) AS total_channel_visits
FROM silver.events
GROUP BY channel_country, channel_region;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_channel_density_by_region
ENGINE = AggregatingMergeTree()
PARTITION BY channel_region
ORDER BY (channel_country, channel_region)
AS
SELECT
    channel_country,
    channel_region,
    count(DISTINCT channel_id) AS total_channels
FROM silver.events
GROUP BY channel_country, channel_region;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_channel_platform_distribution_by_region
ENGINE = AggregatingMergeTree()
PARTITION BY channel_region
ORDER BY (channel_country, channel_region, channel_platform)
AS
SELECT
    channel_country,
    channel_region,
    channel_platform,
    count(DISTINCT channel_id) AS total_channels
FROM silver.events
GROUP BY channel_country, channel_region, channel_platform;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_channel_success_rate_by_region
ENGINE = AggregatingMergeTree()
PARTITION BY channel_region
ORDER BY (channel_country, channel_region)
AS
SELECT
    channel_country,
    channel_region,
    sum(if(channel_total_video_visit > 100000, 1, 0)) / count(DISTINCT channel_id) AS success_rate
FROM silver.events
GROUP BY channel_country, channel_region;



CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_top_performing_channels_grouped
ENGINE = AggregatingMergeTree()
ORDER BY (start_year, channel_region, channel_country, performance_score)
AS
SELECT
    toYear(channel_start_date) AS start_year,
    channel_region,
    channel_country,
    channel_id,
    channel_username,
    channel_name,
    sum(channel_total_video_visit) AS total_video_visit,
    any(channel_followers_count) AS latest_followers_count,
    avg(video_engagement_rate) AS avg_engagement_rate,
    sum(channel_video_count) AS total_video_count,
    (
        (sum(channel_total_video_visit) * 0.4) +
        (any(channel_followers_count) * 0.3) +
        (avg(video_engagement_rate) * 0.3) +
        (sum(channel_video_count) * 0.1)
    ) AS performance_score,
    RANK() OVER (PARTITION BY toYear(channel_start_date), channel_region ORDER BY (
        (sum(channel_total_video_visit) * 0.4) +
        (any(channel_followers_count) * 0.3) +
        (avg(video_engagement_rate) * 0.3) +
        (sum(channel_video_count) * 0.1)
    ) DESC) AS ranking
FROM silver.events
GROUP BY start_year, channel_region, channel_country, channel_id, channel_username, channel_name;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.gold_channel_activity_update_trends
ENGINE = AggregatingMergeTree()
PARTITION BY channel_region
ORDER BY (channel_country, channel_region, start_year)
AS
SELECT
    toYear(channel_start_date) AS start_year,
    channel_country,
    channel_region,
    channel_id,
    anyLast(channel_update_count) AS last_update_count,
    anyLast(days_since_last_update) AS last_days_since_update,
    sum(channel_video_count) / (today() - min(channel_start_date)) AS content_release_rate
FROM silver.events
GROUP BY channel_id, channel_country, channel_region, start_year;

-- channel growth
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_channel_growth
ENGINE = AggregatingMergeTree()
ORDER BY (channel_userid, snapshot_date)
AS
SELECT
    channel_userid,
    any(channel_name) AS channel_name,
    any(channel_country) AS channel_country,
    toDate(silver_ingestion_ts) AS snapshot_date,
    sum(channel_followers_count) AS channel_followers_count,
    sum(CAST(channel_total_video_visit AS UInt64)) AS channel_total_video_visit,
    sum(channel_video_count) AS channel_video_count,
    now() AS gold_ingestion_ts
FROM silver.events
GROUP BY
    channel_userid,
    snapshot_date;

-- top performing channels
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_top_performing_channels
ENGINE = AggregatingMergeTree()
ORDER BY (channel_userid, snapshot_date)
AS
SELECT
    channel_userid,
    any(channel_name) AS channel_name,
    any(channel_country) AS channel_country,
    toDate(silver_ingestion_ts) AS snapshot_date,
    max(channel_followers_count) AS channel_followers_count,
    max(channel_total_video_visit) AS channel_total_video_visit,
    max(channel_video_count) AS channel_video_count,
    now() AS gold_ingestion_ts
FROM silver.events
GROUP BY
    channel_userid,
    snapshot_date;

-- video engagement metrics

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_video_engagement_metrics
ENGINE = AggregatingMergeTree()
ORDER BY (video_uid, video_posted_date)
AS
SELECT
    video_uid,
    any(video_title) AS video_title,
    any(channel_userid) AS channel_userid,
    any(channel_name) AS channel_name,
    toDate(video_posted_date) AS video_posted_date,
    sum(video_visit_count) AS video_visit_count,
    avg(video_engagement_rate) AS video_engagement_rate,
    now() AS gold_ingestion_ts
FROM silver.events
GROUP BY
    video_uid,
    video_posted_date;

-- content popularity analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_content_popularity_analysis
ENGINE = AggregatingMergeTree()
ORDER BY (video_uid, snapshot_date)
AS
SELECT
    video_uid,
    any(video_title) AS video_title,
    sum(video_visit_count) AS video_visit_count,
    toDate(silver_ingestion_ts) AS snapshot_date,
    now() AS gold_ingestion_ts
FROM silver.events
GROUP BY video_uid, snapshot_date;

-- geographic distribution of channels
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_geographic_distribution_of_channels
ENGINE = AggregatingMergeTree()
ORDER BY (channel_region, channel_country, snapshot_date)
AS
SELECT
    channel_country,
    channel_region,
    sum(channel_followers_count) AS channel_followers_count,
    sum(channel_total_video_visit) AS channel_total_video_visit,
    count(DISTINCT channel_userid) AS total_channels,
    toDate(silver_ingestion_ts) AS snapshot_date,
    now() AS gold_ingestion_ts
FROM silver.events
GROUP BY channel_region, channel_country, snapshot_date;

-- channel activity and update trends
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_channel_activity_and_update_trends
ENGINE = AggregatingMergeTree()
ORDER BY (channel_userid, snapshot_date)
AS
SELECT
    channel_userid,
    any(channel_name) AS channel_name,
    any(channel_country) AS channel_country,
    toDate(silver_ingestion_ts) AS snapshot_date,
    sum(channel_update_count) AS channel_update_count,
    avg(video_age) AS avg_video_age,
    sum(channel_video_count) AS channel_video_count,
    sum(is_trending) AS total_trending,
    any(channel_start_date) AS channel_start_date,
    now() AS gold_ingestion_ts
FROM silver.events
GROUP BY channel_userid, snapshot_date ;