# Pharmacy Data Warehouse

Lab 1 of the *Data Warehouse Technologies* course: building a Data Lake and an ingestion (Extract & Load) layer for a pharmacy chain, following the ELT approach.

The project generates synthetic transactional data, stores it as Parquet files (a simulated Data Lake) and loads it **as is** into the `raw` schema of a PostgreSQL data warehouse running in Docker. Cleaning and transformations are planned for the next labs (dbt).

## Architecture

```
Python (Faker + Pandas)  -->  Parquet files (data_lake/)  -->  PostgreSQL in Docker (schema: raw)
   data_generator.py                                               load_to_dwh.py
```

## Tech stack

- Python 3.9+
- Pandas, Faker, PyArrow
- SQLAlchemy, psycopg2
- python-dotenv
- Docker / Docker Compose, PostgreSQL 16

## Data model

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

The Raw layer keeps data exactly as it comes from the source, so the following issues are generated on purpose and will be handled in Lab 2 (dbt):

- about 10% of pharmacies have a `NULL` address
- about 5% of pharmacies are duplicated rows
- about 20% of prescriptions have a `NULL` doctor name
- about 20% of reviews have a `NULL` comment

## Project structure

```
pharmacy-data-warehouse/
├── data_generator.py     # generates dimensions and facts, saves Parquet files
├── load_to_dwh.py        # loads Parquet files into the raw schema
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
git clone https://github.com/IrynaKilyk/pharmacy-data-warehouse.git
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

## Verifying the result

Run a query inside the container. Use the container name from `docker ps` and the user and database from your `.env`:

```bash
docker exec -it pharmacy-data-warehouse-dwh_postgres-1 psql -U <POSTGRES_USER> -d pharmacy_dwh -c "SELECT COUNT(*) FROM raw.sales;"
```

Expected row counts: `sales` = 5000, `pharmacies` = 525, `reviews` = 1000.

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
- [ ] Lab 2: transformations and data quality tests with dbt
- [ ] Lab 3: orchestration with Apache Airflow
- [ ] Lab 4: BI dashboards with Metabase

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.