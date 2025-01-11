SELECT
    id,
    username,
    userid,
    avatar_thumbnail,
    COALESCE(is_official, false) AS is_official,       -- Replace empty with false
    name,
    COALESCE(bio_links, '[]') AS bio_links,           -- Replace empty with '[]'
    total_video_visit,
    video_count,
    start_date,
    start_date_timestamp,
    COALESCE(followers_count, 0) AS followers_count,  -- Replace empty with 0
    COALESCE(following_count, 0) AS following_count,  -- Replace empty with 0
    country,
    platform,
    COALESCE(created_at, '1970-01-01 00:00:00') AS created_at -- Default timestamp
FROM channels;
