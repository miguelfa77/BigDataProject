import sqlalchemy
import psycopg2
from pymongo import MongoClient
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import requests
import io
import os
import json
import pprint

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
        self.bigquery_config = {
            'project_id': 'bigdataproject-420019',
            'dbname': 'imdb'
            }
        self.postgres_file_names = {
            'name.basics': 'name_basics',
            'title.akas': 'title_akas',
            'title.basics': 'title_basics',
            'title.crew': 'title_crew',
            'title.episode': 'title_episode',
            'title.principals': 'title_principals',
            'title.ratings': 'title_ratings'
        }
        self.mongo_file_names = {'movies.json': 'movies_json'}
        self.CHUNKSIZE = 10000

        
    def load_postgres(self):
        postgres_conn = sqlalchemy.create_engine(
                f"postgresql+psycopg2://{self.postgres_config['user']}:{self.postgres_config['password']}@{self.postgres_config['host']}/{self.postgres_config['dbname']}")
        
        for file_name in self.postgres_file_names.keys():
            url = f'https://datasets.imdbws.com/{file_name}.tsv.gz'
            response = requests.get(url)
            chunks = pd.read_csv(io.BytesIO(response.content), compression='gzip', sep='\t', low_memory=False, na_values="\\N",chunksize=self.CHUNKSIZE)
            first_chunk = True
            for chunk in chunks:
                try:
                    if first_chunk:
                        chunk.to_sql(name=self.postgres_file_names[file_name], con=postgres_conn, if_exists='replace', index=False, chunksize=self.CHUNKSIZE)
                        first_chunk = False
                    else:
                        chunk.to_sql(name=self.postgres_file_names[file_name], con=postgres_conn, if_exists='append', index=False, chunksize=self.CHUNKSIZE)
                except Exception as e:
                    print("Error when converting df to PostgreSQL:", e)
                    break
            print(f'Filename {file_name}: Converted to PostgreSQL')
        

    def load_mongo(self):
        try:
            mongo_conn = MongoClient('localhost', 27017)
            db = mongo_conn[self.mongo_config['dbname']]
            collection = db[self.mongo_config['collection']]

            for file_name in self.mongo_file_names.keys():
                with open(self.mongo_file_names[file_name], 'r') as f:
                    df = json.loads(f.read())
            
            collection.insert_many(df)
            print(f"Filename {self.mongo_file_names[file_name]}: Converted to MongoDB")
            mongo_conn.close()
        except Exception as e:
            print(f"Filename {self.mongo_file_names[file_name]}: Failed to load into MongoDB ({e})")

    def load_bigquery(self):
        
        try:
            mongo_conn = MongoClient('localhost', 27017)
            db = mongo_conn[self.mongo_config['dbname']]
            collection = db[self.mongo_config['collection']]
            mongo_data = list(collection.find())

            for element in mongo_data:
                for key, value in element.items():
                    if isinstance(value, list):
                        element[key] = ', '.join(map(str, value))

            mongo_df = pd.DataFrame(mongo_data)
            mongo_df.drop('_id', axis=1, inplace=True)
            for file in self.mongo_file_names.keys(): 
                pandas_gbq.to_gbq(mongo_df, f"{self.bigquery_config['dbname']}.{self.mongo_file_names[file]}",project_id=self.bigquery_config['project_id'], if_exists='replace')
            print("Successfully loaded MongoDB data to BigQuery")
            mongo_conn.close()
        except Exception as e:
            print(f"Failed to load MongoDB data to BigQuery: {e}")
        
        try:
            postgres_conn = sqlalchemy.create_engine(
                    f"postgresql+psycopg2://{self.postgres_config['user']}:{self.postgres_config['password']}@{self.postgres_config['host']}/{self.postgres_config['dbname']}")
            for file in self.postgres_file_names.values():
                print(f"Reading SQL table {file} into Pandas")
                postgres_df = pd.read_sql_table(file, con=postgres_conn)
                print(f"Ingesting {file} into BigQuery")
                pandas_gbq.to_gbq(postgres_df, f"{self.bigquery_config['dbname']}.{file}",project_id=self.bigquery_config['project_id'], if_exists='replace')
                print(f"Successfully ingested {file} to BigQuery")
        except Exception as e:
            print(f"Failed to load PostgreSQL data to BigQuery: {e}")
    

