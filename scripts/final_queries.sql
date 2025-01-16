-- Data Count Queries

SELECT count()
FROM bronze.channels;  -- 2_442_854

SELECT count()
FROM bronze.videos;  -- 6_328_629

SELECT count()
FROM
    bronze.videos
INNER JOIN
    bronze.channels
ON
    bronze.videos.owner_id = bronze.channels.userid;  -- 3_224_903
    
SELECT count()
FROM
    silver.events;  -- 2_045_575


-- Gold Layer Query

SELECT count(*)
FROM gold.top_performing_channels;  --860_644

SELECT count(*)
FROM gold.channel_growth;  --860_644

SELECT count(*)
FROM gold.channel_activity_and_update_trends; --860_645

SELECT count(*)
FROM gold.content_popularity_analysis; --2_045_569

SELECT count(*)
FROM gold.geographic_distribution_of_channels; --223

SELECT count(*)
FROM gold.video_engagement_metrics; --2_045_569

