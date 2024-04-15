from etl_class import ETL

print("Starting ETL script...")

etl = ETL()

print("Loading data to Postgres...")
etl.load_postgres()

print("Loading data to MongoDB...")
etl.load_mongo()

print("Loading data to BigQuery")
etl.load_bigquery()

print("Successfully ran ETL script...")