#!/bin/bash

psql -U utube -d utube -c "COPY channels (id, username, userid, avatar_thumbnail, is_official, name, bio_links, total_video_visit, video_count, start_date, start_date_timestamp, followers_count, following_count, country, platform, created_at, update_count) FROM '/dump/extracted_channels_export.csv' DELIMITER ',' CSV HEADER;"
