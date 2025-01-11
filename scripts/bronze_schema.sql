-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS bronze;

-- Create the channels table if it doesn't exist
CREATE TABLE IF NOT EXISTS bronze.channels
(
    id String,  
    username String,  
    userid String,  
    avatar_thumbnail String, 
    is_official Bool, 
    name String,  
    bio_links String,  
    total_video_visit Int64,  
    video_count Int32,  
    start_date Date,  
    start_date_timestamp Int64,  
    followers_count Int64,  
    following_count Int64, 
    country String, 
    platform String, 
    created_at DateTime('UTC'),  
    update_count Int32  
)
ENGINE = MergeTree()
ORDER BY id;


-- Create the videos table if it doesn't exist
CREATE TABLE IF NOT EXISTS bronze.videos
(
    "id" UInt64,
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
ENGINE = MergeTree()
PRIMARY KEY (created_at)       
ORDER BY (created_at);
