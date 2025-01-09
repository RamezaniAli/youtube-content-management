CREATE DATABASE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.employee
(
    id UInt64,  
    username String,  
    userid String,  
    avatar_thumbnail String, 
    is_official UInt8,  
    name String,  
    bio_links String, 
    total_video_visit UInt64,
    video_count UInt32,  
    start_date Date,  
    start_date_timestamp UInt64,  
    followers_count UInt64, 
    following_count UInt64,  
    country String, 
    platform String,  
    created_at DateTime,  
    update_count UInt32 
)
ENGINE = MergeTree()
ORDER BY id;  

CREATE TABLE IF NOT EXISTS bronze.video
(
    "owner_username" String,
    "owner_id" String,
    "title" String,
    "tags" String,
    "uid" String,
    "visit_count" UInt64,
    "owner_name" String,
    "poster" String,
    "owener_avatar" String,
    "duration" UInt64,
    "posted_date" DateTime,
    "posted_timestamp" UInt64,
    "sdate_rss" DateTime,
    "sdate_rss_tp" UInt64,
    "comments" String,
    "frame" String,
    "like_count" UInt64,
    "description" String,
    "is_deleted" UInt8,
    "created_at" DateTime,
    "expire_at" DateTime,
    "is_produce_to_kafka" UInt8,
    "update_count" UInt32
)
