from etl.etl_class import ETL

etl = ETL()
etl.load_postgres()
etl.load_mongo()
etl.load_bigquery()