import os
import time
import pandas as pd
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(
    filename='logs/ingestion_db.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)

# Create the database engine
engine = create_engine('sqlite:///inventory.db')

# Function to ingest DataFrame into database
def ingest_db(df, table_name, engine):
    """This function will ingest the dataframe into database table"""
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

# Function to load CSV files and ingest into DB
def load_raw_data():
    """This function will load the CSVs as dataframe and ingest into db"""
    start = time.time()

    for file in os.listdir('data'):
        if file.endswith('.csv'):
            logging.info(f"Ingesting file: {file}")
            df = pd.read_csv(f"data/{file}")
            ingest_db(df, file[:-4], engine)  # Remove .csv extension for table name
            logging.info(f"{file} ingested into DB")

    end = time.time()
    total_time = (end - start) / 60
    logging.info(f"\nTotal Time Taken: {total_time} minutes")
    logging.info("\nIngestion Complete\n")

# Run the script
if __name__ == '__main__':
    load_raw_data()
