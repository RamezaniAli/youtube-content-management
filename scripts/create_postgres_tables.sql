\c utube;

CREATE TABLE channels (
    id TEXT,
    username TEXT,
    userid TEXT,
    avatar_thumbnail TEXT,
    is_official BOOLEAN DEFAULT FALSE,
    name TEXT,
    bio_links TEXT DEFAULT '[]',
    total_video_visit BIGINT,
    video_count INTEGER,
    start_date DATE,
    start_date_timestamp BIGINT,
    followers_count BIGINT DEFAULT 0,
    following_count BIGINT DEFAULT 0,
    is_deleted BOOLEAN DEFAULT FALSE,
    country VARCHAR(38),
    platform VARCHAR(7),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    update_count INTEGER,
    offset_val SERIAL UNIQUE
);
