import sqlalchemy
import psycopg2
import pymongo
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import requests
import io
import os

class ETL:
    
    def __init__(self):
        self.postgres_config = {
            'dbname': 'imdb',
            'user': 'postgres',
            'password': os.environ.get('POSTGRES_PASSWORD'),
            'host': 'localhost'
            }
        self.mongo_config = {
            'dbname': 'imdb',
            'collection': 'imdb',
            'host': 'localhost',
            'port': '27017'
            }
        self.file_names = {
            'name.basics': 'name_basics',
            'title.akas': 'title_akas',
            'title.basics': 'title_basics',
            'title.crew': 'title_crew',
            'title.episode': 'title_episode',
            'title.principals': 'title_principals',
            'title.ratings': 'title_ratings'
        }
        self.PROJECT_ID = 'bigdataproject-420019'
        self.CHUNKSIZE = 10000

        
    def load_postgres(self):
        postgres_conn = sqlalchemy.create_engine(
                f"postgresql+psycopg2://{self.postgres_config['user']}:{self.postgres_config['password']}@{self.postgres_config['host']}/{self.postgres_config['dbname']}")

        for file_name in self.file_names.keys():
            url = f'https://datasets.imdbws.com/{file_name}.tsv.gz'
            response = requests.get(url)
            chunks = pd.read_csv(io.BytesIO(response.content), compression='gzip', sep='\t', low_memory=False, na_values="\\N",chunksize=self.CHUNKSIZE)
            first_chunk = True
            for chunk in chunks:
                try:
                    if first_chunk:
                        chunk.to_sql(name=self.file_names[file_name], con=postgres_conn, if_exists='replace', index=False, chunksize=self.CHUNKSIZE)
                        first_chunk = False
                    else:
                        chunk.to_sql(name=self.file_names[file_name], con=postgres_conn, if_exists='append', index=False, chunksize=self.CHUNKSIZE)
                except Exception as e:
                    print("Error when converting df to postgres:", e)
                    break
            print(f'Filename {file_name}: Converted to postgres')

    def load_mongo(self):
        pass

    def load_bigquery(self):
        postgres_conn = sqlalchemy.create_engine(
                f"postgresql+psycopg2://{self.postgres_config['user']}:{self.postgres_config['password']}@{self.postgres_config['host']}/{self.postgres_config['dbname']}")
        for file in self.file_names.values():
            df = pd.read_sql(file, con=postgres_conn)
            print(f"Ingesting {file} into BigQuery")
            pandas_gbq.to_gbq(df, f'{self.postgres_config['dbname']}.{file}',project_id=self.PROJECT_ID, if_exists='replace')
            print(f"Successfully ingested {file} to BigQuery")

etl = ETL()
etl.load_bigquery()

