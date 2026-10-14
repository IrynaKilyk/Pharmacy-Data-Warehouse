import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables from .env
load_dotenv()

# PostgreSQL connection string built from .env values
DB_URL = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

engine = create_engine(DB_URL)

# Tables to load (each has a matching file data_lake/raw_<name>.parquet)
TABLES = ['patients', 'medicines', 'pharmacies', 'sales', 'prescriptions', 'reviews']


def load_data_to_dwh():
    with engine.connect() as conn:
        # Create the "raw" schema if it doesn't exist
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.commit()

    # Extract & Load: read each Parquet file and write it to the raw schema
    # if_exists='replace' makes the script idempotent (safe to re-run, full reload)
    for name in TABLES:
        print(f"Loading {name}...")
        df = pd.read_parquet(f"data_lake/raw_{name}.parquet")
        df.to_sql(name, engine, schema='raw', if_exists='replace', index=False)

    print("Data successfully loaded into schema 'raw'!")


if __name__ == "__main__":
    load_data_to_dwh()