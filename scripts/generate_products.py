import csv
import random
from faker import Faker

fake = Faker()

OUTPUT_FILE = "/opt/airflow/data/raw/products.csv"
NUM_RECORDS = 100000

CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports",
    "Books",
    "Toys",
    "Food & Beverage",
    "Health & Beauty",
    "Automotive",
    "Office Supplies",
]

HEADERS = [
    "product_id",
    "product_name",
    "category",
    "price",
    "cost",
    "sku",
    "weight_kg",
    "stock_quantity",
    "supplier_id",
    "created_date",
]


def maybe_null(value, probability=0.05):
    return None if random.random() < probability else value


def maybe_whitespace(value, probability=0.05):
    if value is not None and random.random() < probability:
        return f"  {value}  "
    return value


def maybe_bad_price(value, probability=0.03):
    if random.random() < probability:
        return round(random.uniform(-100.0, -1.0), 2)
    return value


def maybe_bad_cost(value, probability=0.03):
    if random.random() < probability:
        return round(random.uniform(-50.0, -1.0), 2)
    return value


def maybe_bad_stock(value, probability=0.03):
    if random.random() < probability:
        return random.randint(-100, -1)
    return value


def maybe_bad_supplier_id(value, probability=0.03):
    if random.random() < probability:
        return random.randint(100001, 110000)
    return value


def maybe_bad_date(value, probability=0.03):
    if random.random() < probability:
        return fake.date(pattern="%m/%d/%Y")
    return value


def generate_products():
    skus = []

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()

        for i in range(1, NUM_RECORDS + 1):
            price = round(random.uniform(5.0, 500.0), 2)
            cost = round(price * random.uniform(0.3, 0.7), 2)
            sku = fake.bothify(text="???-#####").upper()

            # duplicate SKUs
            if random.random() < 0.02 and skus:
                sku = random.choice(skus)

            skus.append(sku)

            created_date = fake.date_between(
                start_date="-2y", end_date="today"
            ).isoformat()

            writer.writerow(
                {
                    "product_id": i,
                    "product_name": maybe_whitespace(
                        maybe_null(fake.catch_phrase())
                    ),
                    "category": maybe_whitespace(
                        maybe_null(random.choice(CATEGORIES))
                    ),
                    "price": maybe_bad_price(price),
                    "cost": maybe_bad_cost(cost),
                    "sku": maybe_whitespace(
                        maybe_null(sku)
                    ),
                    "weight_kg": round(random.uniform(0.1, 25.0), 2),
                    "stock_quantity": maybe_bad_stock(
                        random.randint(0, 500)
                    ),
                    "supplier_id": maybe_bad_supplier_id(
                        random.randint(1, 100000)
                    ),
                    "created_date": maybe_bad_date(created_date),
                }
            )

    print(f"Generated {NUM_RECORDS} product records in {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_products()