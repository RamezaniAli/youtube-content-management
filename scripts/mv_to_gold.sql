-- Channel Growth Over Time

CREATE MATERIALIZED VIEW gold.mv_channel_growth
TO gold.channel_growth
AS
SELECT
    channel_userid,
    channel_name,
    channel_country,

    toDate(silver_ingestion_ts) AS snapshot_date,
    sumState(channel_followers_count) AS followers_count_state,
    sumState(toUInt64(channel_total_video_visit)) AS total_video_visits_state,
    sumState(channel_video_count) AS video_count_state,

    now() AS gold_ingestion_ts
FROM silver.events
WHERE channel_followers_count IS NOT NULL
GROUP BY
    channel_country,
    channel_userid,
    channel_name,
    snapshot_date;


-- top performing channels

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_top_performing_channels
TO gold.top_performing_channels
AS
SELECT
    channel_userid,
    channel_name,
    channel_country,

    toDate(silver_ingestion_ts) AS snapshot_date,
    maxState(channel_followers_count) AS highest_followers_count,
    maxState(channel_total_video_visit) AS highest_video_visits,
    maxState(channel_video_count) AS highest_video_count,

    now() AS gold_ingestion_ts
FROM silver.events
WHERE channel_followers_count IS NOT NULL
GROUP BY
    channel_country,
    channel_userid,
    channel_name,
    snapshot_date;


-- video engagement metrics

CREATE MATERIALIZED VIEW gold.mv_video_engagement_metrics
TO gold.video_engagement_metrics
AS
SELECT
    video_uid,
    video_title,
    channel_userid,
    channel_name,
    toDate(video_posted_date) AS video_posted_date,

    sumState(video_visit_count) AS visit_count_state,
    avgState(video_engagement_rate) AS avg_engagement_rate_state,

    now() AS gold_ingestion_ts
FROM silver.events
WHERE video_visit_count IS NOT NULL
GROUP BY video_uid, toDate(video_posted_date);


-- content popularity analysis

CREATE MATERIALIZED VIEW gold.mv_content_popularity_analysis
TO gold.content_popularity_analysis
AS
SELECT
    video_uid,
    video_title,
    sumState(video_visit_count) AS visit_count_state,
    toDate(silver_ingestion_ts) AS snapshot_date,

    now() AS gold_ingestion_ts
FROM silver.events
WHERE video_visit_count IS NOT NULL
GROUP BY video_uid, video_title;


-- geographic distribution of channels

CREATE MATERIALIZED VIEW gold.mv_geographic_distribution_of_channels
TO gold.geographic_distribution_of_channels
AS
SELECT
    channel_country,
    channel_region,
    sumState(channel_followers_count) AS total_followers_count,
    sumState(channel_total_video_visit) AS total_video_visits,
    countState(channel_userid) AS total_channels_state,
    toDate(silver_ingestion_ts) AS snapshot_date,

    now() AS gold_ingestion_ts

FROM silver.events
WHERE channel_country IS NOT NULL
GROUP BY channel_region, channel_country;


-- channel activity and update trends

CREATE MATERIALIZED VIEW gold.mv_channel_activity_and_update_trends
TO gold.channel_activity_and_update_trends
AS
SELECT

    channel_userid,
    any(channel_name) AS channel_name,
    any(channel_country) AS channel_country,
    toDate(silver_ingestion_ts) AS snapshot_date,

    sumState(channel_update_count) AS total_update_count_state,
    avgState(video_age) AS avg_video_age_state,
    sumState(channel_video_count) AS total_videos_state,
    sumState(is_trending) AS is_trending,
    any(channel_start_date) AS channel_start_date,

    now() AS gold_ingestion_ts
FROM silver.events
WHERE channel_userid IS NOT NULL
GROUP BY channel_userid, snapshot_date ;