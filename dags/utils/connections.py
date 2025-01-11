import os
import psycopg2
import pymongo
import logging
from clickhouse_driver import Client

class Postgres:
    def __init__(self):
        self.user = os.getenv('POSTGRES_USER', 'utube')
        self.password = os.getenv('POSTGRES_PASSWORD', 'utube')
        self.db = os.getenv('POSTGRES_DB', 'utube')
        self.host = os.getenv('POSTGRES_HOST', 'postgres')

    def connect(self):
        self.pg_conn = psycopg2.connect(
            database=self.db,
            user=self.user,
            password=self.password,
            host=self.host
        )
        logging.info("Connected to Postgres")
        return self.pg_conn
        
    
    def close(self):
        self.pg_conn.close()
        logging.info("Connection to Postgres closed")

    def execute(self, query):
        with self.pg_conn.cursor() as pg_cursor:
            pg_cursor.execute(query)
            result = pg_cursor.fetchall()
        logging.info("Query Executed")
        return result


class Mongo:
    def __init__(self):
        self.host = os.getenv('MONGO_HOST', 'mongodb')
    
    def connect(self):
        client = pymongo.MongoClient(self.host, 27017)
        logging.info("Connected to MongoDB")
        return client
    

class ClickHouse:
    def __init__(self):
        self.host = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
    
    def connect(self):
        self.client = Client(host=self.host, port=9000)
        logging.info("Connected to ClickHouse")
        return self.client

    def create_schema(self, path: str = './scripts/bronze_schema.sql'):
        with open(path, 'r') as f:
            ddl = f.read()

        statements = ddl.split(';')

        for statement in statements:
            statement = statement.strip()
            if statement:
                try:
                    self.client.execute(statement)
                    logging.info(f"Executed statement: {statement}")
                except Exception as e:
                    logging.error(f"Failed to execute statement: {statement}. Error: {str(e)}")
                    raise e

        
        

