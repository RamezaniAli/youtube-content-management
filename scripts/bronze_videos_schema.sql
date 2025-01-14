CREATE TABLE IF NOT EXISTS bronze.videos_test_3
(
    id String PRIMARY KEY,
    owner_username      String,
    owner_id            String,
    title               String,
    tags                Nullable(String),
    uid                 String,
    visit_count         UInt64,
    owner_name          String,
    poster              String,
    owener_avatar       Nullable(String), -- typo: owner_avatar
    duration            Nullable(UInt64),
    posted_date         DateTime('UTC'),
    posted_timestamp    UInt64,
    sdate_rss           DateTime('UTC'),
    sdate_rss_tp        UInt64,
    comments            Nullable(String),
    frame               Nullable(String),
    like_count          Nullable(UInt64),
    description         Nullable(String),
    is_deleted          UInt8,
    created_at          Nullable(DateTime('UTC')),
    expire_at           DateTime('UTC'),
    is_produce_to_kafka UInt8,
    update_count        UInt32,

    _ingestion_ts       DateTime('UTC') DEFAULT now(),
    _source             String DEFAULT 'mongo',
    _raw_object         String
)
ENGINE = MergeTree()
ORDER BY (id);
