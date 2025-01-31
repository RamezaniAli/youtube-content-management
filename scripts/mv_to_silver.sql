CREATE MATERIALIZED VIEW IF NOT EXISTS silver.events_mv2
to silver.events2
AS
    WITH channel_averages AS (
    SELECT
        ch2.country,
        avg(CAST(ch2.followers_count AS Float64)) AS avg_followers  -- Changed CAST syntax
    FROM bronze.channels ch2
    WHERE ch2.followers_count IS NOT NULL
        AND ch2.followers_count > 0
    GROUP BY ch2.country
)
SELECT
    -- Channel Columns
    lower(c.id) AS channel_id,

    CAST(
        multiIf(
            c.total_video_visit IS NOT NULL AND c.total_video_visit >= 0,
                toInt64(c.total_video_visit),
            c.userid IS NOT NULL,
                sum(assumeNotNull(CAST(v.visit_count AS Int64))) OVER (PARTITION BY c.userid),
            toInt64(0)
        ) AS Int64
    ) AS channel_total_video_visit,

    CAST(
        multiIf(
            c.video_count IS NOT NULL AND c.video_count >= 0,
                CAST(c.video_count AS Int64),
            c.id IS NOT NULL AND c.userid IS NOT NULL,
                CAST(count(*) OVER (PARTITION BY c.userid) AS Int64),
            CAST(0 AS Int64)
        ) AS Int64
    ) AS channel_video_count,

    multiIf(
        c.start_date_timestamp IS NOT NULL AND c.start_date_timestamp != 0,
            toDateTime(c.start_date_timestamp),
        c.created_at IS NOT NULL,
            c.created_at,
        NULL
    ) AS channel_start_date,

    CAST(
        multiIf(
            c.followers_count IS NOT NULL AND c.followers_count >= 0,
                CAST(c.followers_count AS Float64),
            c.country IS NOT NULL,
                CAST(ca.avg_followers AS Float64),
            CAST(0 AS Float64)
        ) AS Float64
    ) AS channel_followers_count,

    lower(c.country)                    AS channel_country,
    c.update_count                      AS channel_updated_count,
    c.updated_at                        AS channel_update_at,
    c._ingestion_ts                     AS channel_ingestion_ts,

    -- Video Columns
    lower(v.uid)                        AS video_uid,
    lower(v.owner_username)             AS video_owner_username,
    CAST(v.visit_count AS Int64)        AS video_visit_count,

    -- Handle duration NULL values
    CAST(
    ifNull(
        assumeNotNull(v.duration),
        assumeNotNull(
            avg(assumeNotNull(v.duration)) OVER (PARTITION BY v.owner_id)
        )
    ),
    'Int64'
)                                                 AS video_duration,

    v.posted_date                                 AS video_posted_date,
    CAST(v.is_deleted AS Int8)                    AS video_is_deleted,
    CAST(v.update_count AS Int32)                 AS video_update_count,
    v._ingestion_ts                               AS video_ingestion_ts,

    now()                                         AS silver_ingestion_ts,
    cityHash64(lower(c.id), lower(v.uid), v.posted_date, v.update_count)                                AS deduppHash,

    -- Calculated columns
    multiIf(
        v.visit_count > 100000, 'High',
        v.visit_count > 50000, 'Medium',
        'Low'
    )                                                                                             AS popularity_category,

    multiIf(
        c.followers_count > 1000000, 'Tier 1',
        c.followers_count > 500000, 'Tier 2',
        'Tier 3'
    )                                                                                                 AS channel_tier,

    if(c.followers_count > 0, v.visit_count / c.followers_count, 0)                                   AS video_engagement_rate,

    CAST(v.visit_count > 100000 AS Int8)                                                               AS is_trending,
    dateDiff('day', v.posted_date, now())                                                              AS video_age,

    dateDiff('day', multiIf(
        c.created_at IS NOT NULL, c.created_at,
        c.start_date_timestamp != 0, toDateTime(c.start_date_timestamp),
        NULL ), now())                                                                                  AS channel_age,

    multiIf(
    lower(c.country) IN ('us', 'usa', 'united states', 'ca', 'canada', 'mx', 'mexico'), 'North America',
    lower(c.country) IN ('br', 'brazil', 'ar', 'argentina', 'co', 'colombia'), 'South America',
    lower(c.country) IN (
        'de', 'germany',
        'gb', 'united kingdom',
        'fr', 'france',
        'it', 'italy',
        'es', 'spain'
    ), 'Europe',
    lower(c.country) IN (
        'cn', 'china',
        'in', 'india',
        'jp', 'japan',
        'kr', 'south korea',
        'id', 'indonesia',
        'my', 'malaysia'
    ), 'Asia',
    lower(c.country) IN ('ng', 'nigeria', 'za', 'south africa', 'et', 'ethiopia', 'eg', 'egypt', 'cd', 'congo'), 'Africa',
    lower(c.country) IN ('au', 'australia', 'pg', 'papua new guinea', 'nz', 'new zealand'), 'Oceania',
    lower(c.country) IN ('ir', 'iran', 'tr', 'turkey', 'sa', 'saudi arabia', 'ae', 'united arab emirates', 'iq', 'iraq'), 'Middle East',
    lower(c.country) IN ('gt', 'guatemala', 'hn', 'honduras', 'cr', 'costa rica'), 'Central America',
    'Other'
)                                                                                                                       AS channel_region

    FROM bronze.videos AS v
    INNER JOIN bronze.channels AS c ON v.owner_id = c.userid
    LEFT JOIN channel_averages ca ON lower(ca.country) = lower(c.country);
