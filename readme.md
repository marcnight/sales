# Sales Data ETL Pipeline

A **Data Engineering** project that extracts sales data from Excel spreadsheets, validates and transforms it, and loads it into a PostgreSQL database while preventing duplicate records.

## Features

- **Extract** sales data from `.xlsx` files using pandas
- **Validate** required columns, data types, and handle missing/null values
- **Deduplicate** records based on invoice number (`numero_nota`) and product code (`codigo_produto`)
- **Transform** date formats for database compatibility
- **Load** only new records into a PostgreSQL database, avoiding re-insertion of existing data
- **Log** every step of the pipeline to `app.log` for audit and debugging

## Requirements

- Python 3.8+
- Microsoft Visual C++ 14.0 or later (required by psycopg2 on Windows)
- PostgreSQL server running

## Dependencies

Install the required Python packages:

```bash
pip install -r requirements.txt
```

## Setup

1. **Database setup** — Run the `database_setup.sql` script in your PostgreSQL instance to create the target table:

```bash
psql -U your_user -d your_database -f database_setup.sql
```

2. **Configure connection** — Edit `config.py` to set your database host, port, and database name (lines are commented to guide you).

3. **Prepare the data file** — Place your Excel spreadsheet as `sales_data_with_dates.xlsx` in the project root, or update the path in `main.py`.

## Usage

Run the pipeline with:

```bash
python main.py
```

You will be prompted to enter your PostgreSQL username and password. The script will then:

1. Read and validate the Excel file
2. Connect to the database
3. Check for previously inserted records
4. Insert only new, non-duplicate records
5. Commit the changes and log the results

## File Structure

| File | Description |
|---|---|
| `main.py` | Pipeline orchestrator |
| `data_processing.py` | Excel reading, validation, and transformation |
| `db_operations.py` | Database queries, comparison, and insertion |
| `config.py` | PostgreSQL connection settings |
| `loggin_setup.py` | Logging configuration |
| `database_setup.sql` | SQL script to create the target table |
| `requirements.txt` | Python package dependencies |
| `sales_data_with_dates.xlsx` | Input sales data spreadsheet |
