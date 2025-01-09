import os
import psycopg2
import pymongo
import logging
from clickhouse_driver import Client

class Postgres:
    def __init__(self):
        self.user = os.getenv('POSTGRES_USER')
        self.password = os.getenv('POSTGRES_PASSWORD')
        self.db = os.getenv('POSTGRES_DB')
        self.host = os.getenv('HOST')

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
        pg_cursor = self.pg_conn.cursor()
        pg_cursor.execute(query)
        pg_cursor.close()
        logging.info("Query executed")


class Mongo:
    def __init__(self):
        self.host = os.getenv('HOST')
    
    def connect(self):
        client = pymongo.MongoClient(self.host, 27017)
        logging.info("Connected to MongoDB")
        return client
    

class ClickHouse:
    def __init__(self):
        self.host = os.getenv('HOST')
    
    def connect(self):
        self.client = Client(host=self.host, port=9000)
        logging.info("Connected to ClickHouse")
        return self.client
    
    def create_schema(self, path:str='scripts/bronze_schema.sql'):
        with open(path, 'r') as f:
            ddl = f.read()
        self.client.execute(ddl)
        
        

