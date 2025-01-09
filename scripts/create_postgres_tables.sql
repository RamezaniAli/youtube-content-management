\c utube;

CREATE TABLE channels (
    id TEXT PRIMARY KEY,
    username TEXT,
    userid TEXT UNIQUE,
    avatar_thumbnail TEXT,
    is_official BOOLEAN,
    name TEXT,
    bio_links TEXT,
    total_video_visit BIGINT,
    video_count INTEGER,
    start_date DATE,
    start_date_timestamp BIGINT,
    followers_count BIGINT,
    following_count BIGINT,
    country VARCHAR(38),
    platform VARCHAR(7),
    created_at TIMESTAMPTZ,
    update_count INTEGER
);
