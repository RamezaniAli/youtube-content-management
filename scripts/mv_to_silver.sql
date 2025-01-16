INSERT INTO silver.events
    SELECT
    -- Channel Columns
    lower(c.id)                         AS channel_id,
    lower(c.username)                   AS channel_username,
    lower(c.userid)                     AS channel_userid,
    lower(c.name)                       AS channel_name,
    c.total_video_visit                 AS channel_total_video_visit,
    c.video_count                       AS channel_video_count,
    c.start_date                        AS channel_start_date,

    -- Handle followers_count NULL values
    CAST(
        ifNull(
            assumeNotNull(c.followers_count),
            assumeNotNull(
                avg(assumeNotNull(c.followers_count)) OVER (
                    PARTITION BY
                        c.country,
                        floor(c.video_count/10)*10,
                        toStartOfDay(c.start_date)
                )
            )
        ),
        'Float64'
    )                                   AS channel_followers_count,

    lower(c.country)                    AS channel_country,
    lower(c.platform)                   AS channel_platform,
    c.update_count                      AS channel_update_count,
    lower(c._source)                    AS channel_source,
    c._ingestion_ts                     AS channel_ingestion_ts,

    -- Video Columns
    lower(v.uid)                        AS video_uid,
    lower(v.owner_username)             AS video_owner_username,
    lower(v.title)                      AS video_title,
    v.visit_count                       AS video_visit_count,

    -- Handle duration NULL values
    CAST(
        ifNull(
            assumeNotNull(v.duration),
            assumeNotNull(
                avg(assumeNotNull(v.duration)) OVER (PARTITION BY v.owner_id)
            )
        ),
        'Float64'
    )                                   AS video_duration,

    v.posted_date                       AS video_posted_date,
    v.description                       AS video_description,
    v.is_deleted                        AS video_is_deleted,
    v.created_at                        AS video_created_at,
    v.expire_at                         AS video_expire_at,
    v.update_count                      AS video_update_count,
    v._ingestion_ts                     AS video_ingestion_ts,
    v._source                           AS video_source,
    v._raw_object                       AS video_raw_object,

    now()                               AS silver_ingestion_ts,
    cityHash64(lower(c.id), lower(v.uid), v.posted_date) AS deduppHash,

    -- Calculated columns
    multiIf(
        v.visit_count > 100000, 'High',
        v.visit_count > 50000, 'Medium',
        'Low'
    )                                   AS popularity_category,

    -- Use the pre-calculated followers_count for channel_tier
    multiIf(
        CAST(
            ifNull(
                assumeNotNull(c.followers_count),
                assumeNotNull(
                    avg(assumeNotNull(c.followers_count)) OVER (
                        PARTITION BY
                            c.country,
                            floor(c.video_count/10)*10,
                            toStartOfDay(c.start_date)
                    )
                )
            ),
            'Float64'
        ) > 1000000, 'Tier 1',
        CAST(
            ifNull(
                assumeNotNull(c.followers_count),
                assumeNotNull(
                    avg(assumeNotNull(c.followers_count)) OVER (
                        PARTITION BY
                            c.country,
                            floor(c.video_count/10)*10,
                            toStartOfDay(c.start_date)
                    )
                )
            ),
            'Float64'
        ) > 500000, 'Tier 2',
        'Tier 3'
    )                                   AS channel_tier,

    dateDiff('day', toDateTime(c._ingestion_ts), now()) AS days_since_update,

    -- Calculate engagement rate with proper NULL handling
    if(
        CAST(
            ifNull(
                assumeNotNull(c.followers_count),
                assumeNotNull(
                    avg(assumeNotNull(c.followers_count)) OVER (
                        PARTITION BY
                            c.country,
                            floor(c.video_count/10)*10,
                            toStartOfDay(c.start_date)
                    )
                )
            ),
            'Float64'
        ) > 0,
        CAST(v.visit_count, 'Float64') / CAST(
            ifNull(
                assumeNotNull(c.followers_count),
                assumeNotNull(
                    avg(assumeNotNull(c.followers_count)) OVER (
                        PARTITION BY
                            c.country,
                            floor(c.video_count/10)*10,
                            toStartOfDay(c.start_date)
                    )
                )
            ),
            'Float64'
        ),
        0
    )                                   AS video_engagement_rate,

    v.visit_count > 10000               AS is_trending,
    dateDiff('day', v.posted_date, now()) AS days_since_last_update,
    dateDiff('day', v.posted_date, now()) AS video_age,

    multiIf(
        lower(c.country) IN ('us', 'ca', 'mx'), 'North America',
        lower(c.country) IN ('br', 'ar', 'co'), 'South America',
        lower(c.country) IN ('de', 'gb', 'fr', 'it', 'es'), 'Europe',
        lower(c.country) IN ('cn', 'in', 'jp', 'kr', 'id'), 'Asia',
        lower(c.country) IN ('ng', 'za', 'et', 'eg', 'cd'), 'Africa',
        lower(c.country) IN ('au', 'pg', 'nz'), 'Oceania',
        lower(c.country) IN ('ir', 'tr', 'sa', 'ae', 'iq'), 'Middle East',
        lower(c.country) IN ('gt', 'hn', 'cr'), 'Central America',
        'Other'
    )                                   AS channel_region

    FROM bronze.videos AS v
    INNER JOIN bronze.channels AS c ON v.owner_id = c.userid
    WHERE modulo(cityHash64(lower(c.id), lower(v.uid)), 10) = 0;



CREATE MATERIALIZED VIEW IF NOT EXISTS silver.events_mv
    TO silver.events
    AS
    SELECT
    -- Channel Columns
    lower(c.id)                         AS channel_id,
    lower(c.username)                   AS channel_username,
    lower(c.userid)                     AS channel_userid,
    lower(c.name)                       AS channel_name,
    c.total_video_visit                 AS channel_total_video_visit,
    c.video_count                       AS channel_video_count,
    c.start_date                        AS channel_start_date,

    -- Handle followers_count NULL values
    CAST(
        ifNull(
            assumeNotNull(c.followers_count),
            assumeNotNull(
                avg(assumeNotNull(c.followers_count)) OVER (
                    PARTITION BY
                        c.country,
                        floor(c.video_count/10)*10,
                        toStartOfDay(c.start_date)
                )
            )
        ),
        'Float64'
    )                                   AS channel_followers_count,

    lower(c.country)                    AS channel_country,
    lower(c.platform)                   AS channel_platform,
    c.update_count                      AS channel_update_count,
    lower(c._source)                    AS channel_source,
    c._ingestion_ts                     AS channel_ingestion_ts,

    -- Video Columns
    lower(v.uid)                        AS video_uid,
    lower(v.owner_username)             AS video_owner_username,
    lower(v.title)                      AS video_title,
    v.visit_count                       AS video_visit_count,

    -- Handle duration NULL values
    CAST(
        ifNull(
            assumeNotNull(v.duration),
            assumeNotNull(
                avg(assumeNotNull(v.duration)) OVER (PARTITION BY v.owner_id)
            )
        ),
        'Float64'
    )                                   AS video_duration,

    v.posted_date                       AS video_posted_date,
    v.description                       AS video_description,
    v.is_deleted                        AS video_is_deleted,
    v.created_at                        AS video_created_at,
    v.expire_at                         AS video_expire_at,
    v.update_count                      AS video_update_count,
    v._ingestion_ts                     AS video_ingestion_ts,
    v._source                           AS video_source,
    v._raw_object                       AS video_raw_object,

    now()                               AS silver_ingestion_ts,
    cityHash64(lower(c.id), lower(v.uid), v.posted_date) AS deduppHash,

    -- Calculated columns
    multiIf(
        v.visit_count > 100000, 'High',
        v.visit_count > 50000, 'Medium',
        'Low'
    )                                   AS popularity_category,

    -- Channel tier calculation
    multiIf(
        CAST(
            ifNull(
                assumeNotNull(c.followers_count),
                assumeNotNull(
                    avg(assumeNotNull(c.followers_count)) OVER (
                        PARTITION BY
                            c.country,
                            floor(c.video_count/10)*10,
                            toStartOfDay(c.start_date)
                    )
                )
            ),
            'Float64'
        ) > 1000000, 'Tier 1',
        CAST(
            ifNull(
                assumeNotNull(c.followers_count),
                assumeNotNull(
                    avg(assumeNotNull(c.followers_count)) OVER (
                        PARTITION BY
                            c.country,
                            floor(c.video_count/10)*10,
                            toStartOfDay(c.start_date)
                    )
                )
            ),
            'Float64'
        ) > 500000, 'Tier 2',
        'Tier 3'
    )                                   AS channel_tier,


    -- Engagement rate calculation
    if(
        CAST(
            ifNull(
                assumeNotNull(c.followers_count),
                assumeNotNull(
                    avg(assumeNotNull(c.followers_count)) OVER (
                        PARTITION BY
                            c.country,
                            floor(c.video_count/10)*10,
                            toStartOfDay(c.start_date)
                    )
                )
            ),
            'Float64'
        ) > 0,
        CAST(v.visit_count, 'Float64') / CAST(
            ifNull(
                assumeNotNull(c.followers_count),
                assumeNotNull(
                    avg(assumeNotNull(c.followers_count)) OVER (
                        PARTITION BY
                            c.country,
                            floor(c.video_count/10)*10,
                            toStartOfDay(c.start_date)
                    )
                )
            ),
            'Float64'
        ),
        0
    )                                   AS video_engagement_rate,

    v.visit_count > 10000               AS is_trending,
    dateDiff('day', v.posted_date, now()) AS days_since_last_update,
    dateDiff('day', v.posted_date, now()) AS video_age,

    multiIf(
        lower(c.country) IN ('us', 'ca', 'mx'), 'North America',
        lower(c.country) IN ('br', 'ar', 'co'), 'South America',
        lower(c.country) IN ('de', 'gb', 'fr', 'it', 'es'), 'Europe',
        lower(c.country) IN ('cn', 'in', 'jp', 'kr', 'id'), 'Asia',
        lower(c.country) IN ('ng', 'za', 'et', 'eg', 'cd'), 'Africa',
        lower(c.country) IN ('au', 'pg', 'nz'), 'Oceania',
        lower(c.country) IN ('ir', 'tr', 'sa', 'ae', 'iq'), 'Middle East',
        lower(c.country) IN ('gt', 'hn', 'cr'), 'Central America',
        'Other'
    )                                   AS channel_region

FROM bronze.videos AS v
INNER JOIN bronze.channels AS c ON v.owner_id = c.userid;