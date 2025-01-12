CREATE MATERIALIZED VIEW silver.mv_silver_events TO silver.events AS
SELECT

    v.uid AS video_uid,
    c.userid AS channel_userid,
    c.username AS channel_username,
    c.name AS channel_name,
    c.avatar_thumbnail AS channel_avatar_thumbnail,
    c.bio_links AS channel_bio_links,
    c.total_video_visit AS channel_total_video_visit,
    c.video_count AS channel_video_count,
    toDateTime64(c.start_date, 3, 'UTC') AS channel_start_date,
    c.start_date_timestamp AS channel_start_date_ts,

    -- Impute channel_followers_count
    ifNull(c.followers_count, (SELECT avg(followers_count) FROM bronze.channels 
                                WHERE video_count BETWEEN c.video_count - 10 AND c.video_count + 10 AND total_video_visit
                                BETWEEN c.total_video_visit - 1000 AND c.total_video_visit + 1000 AND country = c.country))
                                AS channel_followers_count,

    c.country AS channel_country,
    c.platform AS channel_platform,
    c.update_count AS channel_update_count,
    v.owner_username AS video_owner_username,
    v.title AS video_title,
    v.visit_count AS video_visit_count,
    v.owner_name AS video_owner_name,
    v.poster AS video_poster,

    -- Impute video_duration
    ifNull(v.duration, (SELECT avg(duration) FROM bronze.videos WHERE owner_id = v.owner_id)) AS video_duration,
    toDateTime64(v.posted_date, 3, 'UTC') AS video_posted_date,
    v.posted_timestamp AS video_posted_date_ts,

    v.description AS video_description,
    v.is_deleted AS video_is_deleted,
    toDateTime64(v.created_at, 3, 'UTC') AS video_created_at,
    toDateTime64(v.created_at, 3, 'UTC') AS video_created_at_ts,
    toDateTime64(v.expire_at, 3, 'UTC') AS video_expire_at,
    toDateTime64(v.expire_at, 3, 'UTC') AS video_expire_at_ts,
    v.update_count AS video_update_count,
    toDateTime64(v._ingestion_ts, 3, 'UTC') AS video_ingestion_ts,
    v._source AS video_source,
    toDateTime64(c._ingestion_ts, 3, 'UTC') AS channel_ingestion_ts,
    c._source AS channel_source,
    now() AS silver_ingestion_ts,
    cityHash64(c.userid, v.uid) AS record_hash,

    CASE
        WHEN v.visit_count > 0 THEN (coalesce(v.like_count, 0) + length(coalesce(v.comments, ''))) / v.visit_count
        ELSE 0
    END AS engagement_rate,

    CASE
        WHEN v.visit_count > 100000 THEN 'High' -- validate numbers based our data
        WHEN v.visit_count > 10000 THEN 'Medium' -- validate numbers based our data
        ELSE 'Low'
    END AS popularity_category,

     CASE
        WHEN c.followers_count > 1000000 THEN 'Tier 1' -- validate numbers based our data
        WHEN c.followers_count > 100000 THEN 'Tier 2' -- validate numbers based our data
        ELSE 'Tier 3'
    END AS channel_tier,

    dateDiff('day', toDate(c._ingestion_ts), toDate(now())) AS days_since_update

FROM bronze.videos AS v
INNER JOIN bronze.channels AS c ON v.owner_id = c.userid;
