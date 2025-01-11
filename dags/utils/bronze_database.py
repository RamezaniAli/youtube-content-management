from utils.connections import Postgres, Mongo, ClickHouse
import logging
import os

class BronzeIntegration:
    def __init__(self):
        self.postgres = Postgres()
        self.mongo = Mongo()
        self.clickhouse = ClickHouse()
    
    def read_from_postgres(self):
        pg_conn = self.postgres.connect()
        click_conn = self.clickhouse.connect()

        BATCH_SIZE = 1000
        OFFSET = 0

        query = """
        SELECT
            id,
            username,
            userid,
            avatar_thumbnail,
            CAST(COALESCE(is_official, false) AS BOOLEAN) AS is_official,
            name,
            COALESCE(bio_links, '[]') AS bio_links,
            total_video_visit::BIGINT,
            video_count::INTEGER,
            start_date,
            start_date_timestamp::BIGINT,
            COALESCE(followers_count, 0)::BIGINT AS followers_count,
            COALESCE(following_count, 0)::BIGINT AS following_count,
            country,
            platform,
            CAST(COALESCE(created_at, '1970-01-01 00:00:00') AS TIMESTAMP) AS created_at,
            update_count::INTEGER
        FROM channels
        LIMIT {batch_size} OFFSET {offset};
        """

        while True:
            formatted_query = query.format(batch_size=BATCH_SIZE, offset=OFFSET)
            rows = self.postgres.execute(formatted_query)
            logging.info("Reading data from Postgres from offset %s", OFFSET)
            logging.info("row:{}".format(rows))
            
            if not rows:
                break

            click_conn.execute(
                'INSERT INTO bronze.channels VALUES', rows
            )
            logging.info("Inserted data to ClickHouse from offset %s", OFFSET)

            OFFSET += BATCH_SIZE

        logging.info("Finished reading data from Postgres")
        self.postgres.close()

    def read_from_mongo(self):
        try:
            mongo_conn = self.mongo.connect()
            click_conn = self.clickhouse.connect()

            DATABASE_NAME = os.getenv('MONGO_DB', 'utube')
            db = mongo_conn[DATABASE_NAME]
            collection = db['videos']

            documents = list(collection.find())
            logging.info("Retrieved %d documents from MongoDB", len(documents))

            logging.info("Reading data from MongoDB")

            BATCH_SIZE = 1000
            batch = []

            for doc in collection.find():
                data = {
                    "id": doc["_id"],
                    "object": doc["object"],
                    "created_at": doc.get("created_at"),
                    "expire_at": doc.get("expire_at"),
                    "is_produce_to_kafka": doc.get("is_produce_to_kafka", False),
                    "update_count": doc.get("update_count", 0),
                }

                for field in ("_", "platform"):
                    data.pop(field, None)

                record = (
                    data["id"],
                    data["object"],
                    data["created_at"],
                    data["expire_at"],
                    data["is_produce_to_kafka"],
                    data["update_count"],
                )
                batch.append(record)

                if len(batch) >= BATCH_SIZE:
                    click_conn.execute('INSERT INTO bronze.videos VALUES', batch)
                    logging.info("Inserted batch of %d records into ClickHouse", len(batch))
                    batch.clear()

            if batch:
                click_conn.execute('INSERT INTO bronze.videos VALUES', batch)
                logging.info("Inserted final batch of %d records into ClickHouse", len(batch))

            logging.info("Finished reading data from MongoDB")

        except Exception as e:
            logging.error("Error occurred: %s", str(e))
        finally:
            mongo_conn.close()
            logging.info("MongoDB connection closed")


    def create_clickhouse_schema(self):
        self.clickhouse.connect()
        self.clickhouse.create_schema()
