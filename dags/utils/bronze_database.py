from utils.connections import Postgres, Mongo, ClickHouse
import logging

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

        while True:
            query = f'SELECT * FROM channels LIMIT {BATCH_SIZE} OFFSET {OFFSET}'
            rows = self.postgres.execute(query)
            logging.info("Reading data from Postgres from offset %s", OFFSET)
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
        mongo_conn = self.mongo.connect()
        click_conn = self.clickhouse.connect()

        db = mongo_conn['youtube']
        collection = db['videos']

        logging.info("Reading data from MongoDB")
        for doc in collection.find():
            data = doc["object"]
            created_at = doc["created_at"]
            expire_at = doc["expire_at"]
            is_produce_to_kafka = doc["is_produce_to_kafka"]
            update_count = doc["update_count"]

            data + [created_at, expire_at, is_produce_to_kafka, update_count]

            fields_to_delete = ("_", "platform")
            for field in fields_to_delete:
                data.pop(field, None)

            logging.info("Data: %s", data)                
            
            logging.info("Inserting data to ClickHouse")
            click_conn.execute(
                'INSERT INTO bronze.videos VALUES', data
            )

        logging.info("Finished reading data from MongoDB")

    def create_clickhouse_schema(self):
        self.clickhouse.connect()
        self.clickhouse.create_schema()