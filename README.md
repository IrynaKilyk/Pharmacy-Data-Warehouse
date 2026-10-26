# Pharmacy Data Warehouse

Labs 1-2 of the *Data Warehouse Technologies* course: building a Data Lake, an ingestion (Extract & Load) layer and a transformation layer (dbt) for a pharmacy chain, following the ELT approach.

The project generates synthetic transactional data, stores it as Parquet files (a simulated Data Lake), loads it **as is** into the `raw` schema of a PostgreSQL data warehouse running in Docker, and then cleans and models it with **dbt** into a star schema in the `analytics` schema.

## Architecture

```
Python (Faker + Pandas)  -->  Parquet files (data_lake/)  -->  PostgreSQL in Docker (schema: raw)  -->  dbt  -->  schema: analytics
   data_generator.py                                               load_to_dwh.py                      staging -> marts
```

## Tech stack

- Python 3.9+
- Pandas, Faker, PyArrow
- SQLAlchemy, psycopg2
- python-dotenv
- dbt-core, dbt-postgres
- Docker / Docker Compose, PostgreSQL 16

## Data model (raw layer)

**Dimensions**

| Table | Description | Rows |
|---|---|---|
| `patients` | Patients (name, birth date, city) | 500 |
| `medicines` | Medicines (name, price, active flag) | 100 |
| `pharmacies` | Pharmacies / locations (name, address, city, registration date) | 525* |

**Facts**

| Table | Description | Rows |
|---|---|---|
| `sales` | Sales of medicines (patient, medicine, pharmacy, quantity, status) | 5000 |
| `prescriptions` | Issued prescriptions (patient, medicine, doctor, dosage) | 1500 |
| `reviews` | Patient reviews of medicines (rating 1-5, comment) | 1000 |

Fact tables reference the dimensions through foreign keys: `patient_id`, `medicine_id`, `pharmacy_id`.

\* 500 unique pharmacies + 25 intentional duplicates.

### Intentionally "dirty" data

The Raw layer keeps data exactly as it comes from the source, so the following issues are generated on purpose and are handled in the dbt staging layer (Lab 2):

- about 10% of pharmacies have a `NULL` address
- about 5% of pharmacies are duplicated rows
- about 20% of prescriptions have a `NULL` doctor name
- about 20% of reviews have a `NULL` comment

## dbt project

The dbt project lives in `pharmacy_dbt/` and reads from the `raw` schema, writing to the `analytics` schema.

```
pharmacy_dbt/
├── dbt_project.yml
├── profiles.yml              # connection settings, credentials come from env vars
└── models/
    ├── staging/              # cleaning layer (views)
    │   ├── src_pharmacy.yml  # raw sources
    │   ├── stg_patients.sql
    │   ├── stg_medicines.sql
    │   ├── stg_pharmacies.sql
    │   ├── stg_sales.sql
    │   ├── stg_prescriptions.sql
    │   └── stg_reviews.sql
    └── marts/                # star schema (tables)
        ├── dim_patients.sql
        ├── dim_medicines.sql
        ├── dim_pharmacies.sql
        └── fct_sales.sql
```

### Staging layer

Staging models perform light transformations only: renaming, type casting and handling of bad values.

| Model | What it does |
|---|---|
| `stg_patients` | selects patient columns |
| `stg_medicines` | splits the strength out of the name into a separate `dosage` column (e.g. `Naproxen 250 mg` becomes `Naproxen` and `250 mg`), clamps negative prices to 0, casts price to `numeric(10,2)` |
| `stg_pharmacies` | removes duplicates (keeps the earliest `registration_date` per `pharmacy_id`), replaces empty addresses with `Unknown`, renames `address` to `pharmacy_address` |
| `stg_sales` | selects sale columns, renames `status` to `sale_status` |
| `stg_prescriptions` | replaces empty doctor names with `Unknown`, renames `dosage` to `intake_instructions` (it holds intake instructions like "twice a day", not a strength) |
| `stg_reviews` | replaces empty comments with `No comments`, renames `comment` to `user_comment` |

> Note: `medicines.dosage` is the strength of the drug (mg), while `prescriptions.intake_instructions` describes how to take it. They are named differently on purpose to avoid confusion.

### Marts layer (star schema)

| Model | Type | Description |
|---|---|---|
| `dim_patients` | dimension | patients |
| `dim_medicines` | dimension | medicines with cleaned price and separate `dosage` column |
| `dim_pharmacies` | dimension | deduplicated pharmacies |
| `fct_sales` | fact | sales joined with patients and medicines, with `total_amount = quantity * price` |

`fct_sales` uses inner joins to `stg_patients` and `stg_medicines`, so only sales with existing patients and medicines are kept (data integrity).

## Project structure

```
pharmacy-data-warehouse/
├── data_generator.py     # generates dimensions and facts, saves Parquet files
├── load_to_dwh.py        # loads Parquet files into the raw schema
├── pharmacy_dbt/         # dbt project (staging + marts)
├── docker-compose.yml    # PostgreSQL service
├── .dist.env             # template for environment variables
├── requirements.txt
├── .gitignore
├── LICENSE
└── README.md
```

## Getting started

### Prerequisites

- Python 3.9 or newer
- Git
- Docker Desktop (running)

### 1. Clone the repository

```bash
git clone https://github.com/IrynaKilyk/Pharmacy-Data-Warehouse.git
cd pharmacy-data-warehouse
```

### 2. Create a virtual environment and install dependencies

```bash
python -m venv venv

# Windows (PowerShell)
venv\Scripts\activate
# macOS / Linux
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Configure environment variables

Copy the template to `.env`:

```bash
# Windows (PowerShell)
copy .dist.env .env
# macOS / Linux
cp .dist.env .env
```

Then open `.env` and set your own values (at least the password). The file contains these variables:

| Variable | Description |
|---|---|
| `POSTGRES_USER` | Database user |
| `POSTGRES_PASSWORD` | Database password (replace the placeholder) |
| `POSTGRES_DB` | Database name (`pharmacy_dwh`) |
| `POSTGRES_HOST` | Database host (`localhost`) |
| `POSTGRES_PORT` | Port on your machine mapped to PostgreSQL |

The `.env` file is ignored by git and must never be committed.

### 4. Start PostgreSQL

```bash
docker-compose up -d
docker ps
```

The container should have the status `Up`.

### 5. Generate data

```bash
python data_generator.py
```

Six Parquet files appear in the `data_lake/` folder.

### 6. Load data into the warehouse

```bash
python load_to_dwh.py
```

The script creates the `raw` schema and loads all six tables. It uses `if_exists='replace'`, so it is idempotent: it can be safely re-run and will not duplicate data.

### 7. Run dbt transformations

dbt does not read `.env` by itself, so load the variables into the current terminal session first and point dbt to the profile inside the project.

```powershell
# Windows (PowerShell), from the repository root
cd pharmacy_dbt
Get-Content ..\.env | Where-Object { $_ -match '^\s*[^#\s][^=]*=' } | ForEach-Object {
    $name, $value = $_ -split '=', 2
    Set-Item -Path "Env:$($name.Trim())" -Value $value.Trim().Trim('"')
}
$env:DBT_PROFILES_DIR = "."
```

```bash
# macOS / Linux, from the repository root
cd pharmacy_dbt
set -a; source ../.env; set +a
export DBT_PROFILES_DIR=.
```

Then check the connection and build the models:

```bash
dbt debug    # all checks must pass
dbt run      # creates 6 views (staging) and 4 tables (marts) in the analytics schema
```

## Verifying the result

Run queries inside the container. Use the container name from `docker ps` and the user and database from your `.env`:

```bash
docker exec -it pharmacy-data-warehouse-dwh_postgres-1 psql -U <POSTGRES_USER> -d pharmacy_dwh -c "SELECT COUNT(*) FROM raw.sales;"
```

Expected results:

| Check | Expected |
|---|---|
| `raw.sales` rows | 5000 |
| `raw.pharmacies` rows | 525 |
| `analytics.dim_pharmacies` rows | 500 (duplicates removed) |
| `analytics.fct_sales` rows | 5000 |

To stop the database:

```bash
docker-compose down        # keeps the data volume
docker-compose down -v     # also deletes the data volume
```

## Git workflow

- `develop`: the working branch, all changes are committed here as separate, focused commits
- `main`: stable version, updated by merging `develop` through a pull request

Commit messages follow the Conventional Commits style (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`).

## Roadmap

- [x] Lab 1: Data Lake and Ingestion (Extract & Load)
- [x] Lab 2: transformations with dbt (staging and star schema)
- [ ] Lab 3: orchestration with Apache Airflow
- [ ] Lab 4: BI dashboards with Metabase

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.