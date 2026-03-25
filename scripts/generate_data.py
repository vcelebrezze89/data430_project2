import os

from generate_customers import generate_customers
from generate_suppliers import generate_suppliers
from generate_products import generate_products
from generate_daily_sales import generate_daily_sales


RAW_DATA_DIR = "/opt/airflow/data/raw"


def ensure_directories():
    os.makedirs(RAW_DATA_DIR, exist_ok=True)


def main():
    print("Starting data generation...")
    ensure_directories()

    print("Generating customers...")
    generate_customers()

    print("Generating suppliers...")
    generate_suppliers()

    print("Generating products...")
    generate_products()

    print("Generating daily sales...")
    generate_daily_sales()

    print("All datasets generated successfully.")


if __name__ == "__main__":
    main()