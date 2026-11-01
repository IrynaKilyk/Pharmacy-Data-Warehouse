import os
import random

import pandas as pd
from dotenv import load_dotenv
from faker import Faker
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# Project root: airflow/dags/scripts/data_generator.py -> scripts -> dags -> airflow -> root
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)
DATA_LAKE_DIR = os.path.join(PROJECT_ROOT, "data_lake")

# Load environment variables from the .env file in the project root
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# Tables to load (each has a matching file data_lake/raw_<name>.parquet)
TABLES = ['patients', 'medicines', 'pharmacies', 'sales', 'prescriptions', 'reviews']

# ==========================================
# 1. DIMENSION TABLES
# ==========================================

# --- Dimension 1: Patients ---
def generate_patients(fake, n):
    print(f"Generating {n} patients...")
    data = []
    for i in range(1, n + 1):
        data.append({
            'patient_id': i,
            'patient_name': fake.name(),
            'birth_date': fake.date_of_birth(minimum_age=1, maximum_age=90),
            'city': fake.city()
        })
    return pd.DataFrame(data)


# --- Dimension 2: Medicines ---
def generate_medicines(n):
    print(f"Generating {n} medicines...")
    names = ['Aspirin', 'Paracetamol', 'Ibuprofen', 'Amoxicillin', 'Loratadine',
             'Omeprazole', 'Metformin', 'Cetirizine', 'Naproxen', 'Vitamin C']
    doses = ['100 mg', '250 mg', '500 mg']
    data = []
    for i in range(1, n + 1):
        data.append({
            'medicine_id': i,
            'medicine_name': f"{random.choice(names)} {random.choice(doses)}",
            'price': round(random.uniform(5.0, 500.0), 2),
            'is_active': random.choice([True, True, False])
        })
    return pd.DataFrame(data)


# --- Dimension 3: Pharmacies / Locations ---
def generate_pharmacies(fake, n):
    print(f"Generating {n} pharmacies (with 'dirty' data)...")
    data = []
    for i in range(1, n + 1):
        # ~10% missing addresses (to be handled with dbt tests)
        address = fake.address() if random.random() > 0.1 else None
        data.append({
            'pharmacy_id': i,
            'pharmacy_name': fake.company(),
            'address': address,
            'city': fake.city(),
            'registration_date': fake.date_between(start_date='-2y', end_date='today')
        })
    df = pd.DataFrame(data)

    # Add ~5% duplicates on purpose
    duplicates = df.sample(frac=0.05, random_state=42)
    df = pd.concat([df, duplicates], ignore_index=True)
    return df


# ==========================================
# 2. FACT TABLES
# ==========================================

# --- Fact 1: Sales ---
def generate_sales(fake, n, patient_ids, medicine_ids, pharmacy_ids):
    print(f"Generating {n} sales...")
    data = []
    for _ in range(n):
        data.append({
            'sale_id': fake.uuid4(),
            'patient_id': random.choice(patient_ids),     # FK -> patients
            'medicine_id': random.choice(medicine_ids),   # FK -> medicines
            'pharmacy_id': random.choice(pharmacy_ids),   # FK -> pharmacies
            'sale_date': fake.date_time_between(start_date='-1y', end_date='now'),
            'quantity': random.randint(1, 5),
            'status': random.choices(['completed', 'pending', 'cancelled'],
                                     weights=[80, 15, 5])[0]
        })
    return pd.DataFrame(data)


# --- Fact 2: Prescriptions ---
def generate_prescriptions(fake, n, patient_ids, medicine_ids):
    print(f"Generating {n} prescriptions...")
    doctors = ['Dr. James Wilson', 'Dr. Emily Parker', 'Dr. Sarah Bennett']
    dosages = ['1 pill/day', 'twice a day', '1 tablet every morning',
               'one tablet before bedtime']
    data = []
    for _ in range(n):
        data.append({
            'prescription_id': fake.uuid4(),
            'patient_id': random.choice(patient_ids),     # FK -> patients
            'medicine_id': random.choice(medicine_ids),   # FK -> medicines
            # ~20% missing doctor names
            'doctor_name': random.choice(doctors) if random.random() > 0.2 else None,
            'dosage': random.choice(dosages),
            'prescription_date': fake.date_time_between(start_date='-1y', end_date='now')
        })
    return pd.DataFrame(data)


# --- Fact 3: Reviews ---
def generate_reviews(fake, n, patient_ids, medicine_ids):
    print(f"Generating {n} reviews...")
    comments = ['Works well', 'Easy to swallow', 'Very effective',
                'Mild taste', 'No side effects', 'Good quality', 'Affordable price']
    data = []
    for _ in range(n):
        data.append({
            'review_id': fake.uuid4(),
            'patient_id': random.choice(patient_ids),     # FK -> patients
            'medicine_id': random.choice(medicine_ids),   # FK -> medicines
            'rating': random.randint(1, 5),
            # ~20% missing comments
            'comment': random.choice(comments) if random.random() > 0.2 else None,
            'review_date': fake.date_time_between(start_date='-1y', end_date='now')
        })
    return pd.DataFrame(data)


# ==========================================
# 3. GENERATION ORCHESTRATION AND SAVING
# ==========================================

def generate_raw_data():
    """Generate synthetic data and save it as Parquet files in the data lake."""
    # Initialize Faker and seeds inside the function (no side effects on import)
    fake = Faker('en_US')
    Faker.seed(42)
    random.seed(42)

    os.makedirs(DATA_LAKE_DIR, exist_ok=True)
    print("Starting data generation...")

    df_patients = generate_patients(fake, 500)
    df_medicines = generate_medicines(100)
    df_pharmacies = generate_pharmacies(fake, 500)

    # Facts use unique IDs only, because duplicates were added to pharmacies on purpose
    patient_ids = df_patients['patient_id'].dropna().unique().tolist()
    medicine_ids = df_medicines['medicine_id'].tolist()
    pharmacy_ids = df_pharmacies['pharmacy_id'].dropna().unique().tolist()

    df_sales = generate_sales(fake, 5000, patient_ids, medicine_ids, pharmacy_ids)
    df_prescriptions = generate_prescriptions(fake, 1500, patient_ids, medicine_ids)
    df_reviews = generate_reviews(fake, 1000, patient_ids, medicine_ids)

    # Save as Parquet (Data Lake simulation)
    print("Saving files in Parquet format...")
    tables = {
        'patients': df_patients,
        'medicines': df_medicines,
        'pharmacies': df_pharmacies,
        'sales': df_sales,
        'prescriptions': df_prescriptions,
        'reviews': df_reviews,
    }
    for name, df in tables.items():
        df.to_parquet(os.path.join(DATA_LAKE_DIR, f"raw_{name}.parquet"), index=False)

    print(f"Generation completed successfully! Files saved to {DATA_LAKE_DIR}")


# ==========================================
# 4. LOAD TO POSTGRESQL
# ==========================================

def load_parquet_to_postgres():
    """Read each Parquet file from the data lake and write it to the 'raw' schema."""
    # URL.create safely handles special characters in the password
    db_url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB"),
    )
    engine = create_engine(db_url)

    with engine.connect() as conn:
        # Create the "raw" schema if it doesn't exist
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.commit()

    # if_exists='replace' makes the load idempotent (safe to re-run, full reload)
    for name in TABLES:
        print(f"Loading {name}...")
        df = pd.read_parquet(os.path.join(DATA_LAKE_DIR, f"raw_{name}.parquet"))

        # Check whether the table already exists in the raw schema
        with engine.connect() as conn:
            exists = conn.execute(
                text(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'raw' AND table_name = :t)"
                ),
                {"t": name},
            ).scalar()

        if exists:
            # TRUNCATE keeps the table (and dependent dbt views) intact
            with engine.begin() as conn:
                conn.execute(text(f'TRUNCATE TABLE raw."{name}"'))
            df.to_sql(name, engine, schema="raw", if_exists="append", index=False)
        else:
            df.to_sql(name, engine, schema="raw", if_exists="fail", index=False)


def load_data_to_dwh():
    """Airflow entry point: generate raw data, then load it into PostgreSQL."""
    generate_raw_data()
    load_parquet_to_postgres()


if __name__ == "__main__":
    load_data_to_dwh()