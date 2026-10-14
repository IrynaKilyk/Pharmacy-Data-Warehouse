import pandas as pd
from faker import Faker
import random
import os

# Initialize Faker
fake = Faker('en_US')
Faker.seed(42)
random.seed(42)

# Create the Data Lake folder if it doesn't exist
os.makedirs('data_lake', exist_ok=True)
print("Starting data generation...")

# ==========================================
# 1. DIMENSION TABLES
# ==========================================

# --- Dimension 1: Patients ---
def generate_patients(n):
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
def generate_pharmacies(n):
    print(f"Generating {n} pharmacies (with 'dirty' data)...")
    data = []
    for i in range(1, n + 1):
        # ~10% missing addresses (to be handled with dbt tests in Lab 2)
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
    duplicates = df.sample(frac=0.05)
    df = pd.concat([df, duplicates], ignore_index=True)
    return df


# ==========================================
# 2. FACT TABLES
# ==========================================

# --- Fact 1: Sales ---
def generate_sales(n, patient_ids, medicine_ids, pharmacy_ids):
    print(f"Generating {n} sales...")
    data = []
    for i in range(1, n + 1):
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
def generate_prescriptions(n, patient_ids, medicine_ids):
    print(f"Generating {n} prescriptions...")
    doctors = ['Dr. James Wilson', 'Dr. Emily Parker', 'Dr. Sarah Bennett']
    dosages = ['1 pill/day', 'twice a day', '1 tablet every morning',
               'one tablet before bedtime']
    data = []
    for i in range(1, n + 1):
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
def generate_reviews(n, patient_ids, medicine_ids):
    print(f"Generating {n} reviews...")
    comments = ['Works well', 'Easy to swallow', 'Very effective',
                'Mild taste', 'No side effects', 'Good quality', 'Affordable price']
    data = []
    for i in range(1, n + 1):
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

df_patients = generate_patients(500)
df_medicines = generate_medicines(100)
df_pharmacies = generate_pharmacies(500)

# Facts use unique IDs only, because duplicates were added to pharmacies on purpose
patient_ids = df_patients['patient_id'].dropna().unique().tolist()
medicine_ids = df_medicines['medicine_id'].tolist()
pharmacy_ids = df_pharmacies['pharmacy_id'].dropna().unique().tolist()

df_sales = generate_sales(5000, patient_ids, medicine_ids, pharmacy_ids)
df_prescriptions = generate_prescriptions(1500, patient_ids, medicine_ids)
df_reviews = generate_reviews(1000, patient_ids, medicine_ids)

# Save as Parquet (Data Lake simulation)
print("Saving files in Parquet format...")
df_patients.to_parquet('data_lake/raw_patients.parquet', index=False)
df_medicines.to_parquet('data_lake/raw_medicines.parquet', index=False)
df_pharmacies.to_parquet('data_lake/raw_pharmacies.parquet', index=False)
df_sales.to_parquet('data_lake/raw_sales.parquet', index=False)
df_prescriptions.to_parquet('data_lake/raw_prescriptions.parquet', index=False)
df_reviews.to_parquet('data_lake/raw_reviews.parquet', index=False)
print("Generation completed successfully! Files saved to /data_lake")