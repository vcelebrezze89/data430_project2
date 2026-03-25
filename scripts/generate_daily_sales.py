import csv
import random
from faker import Faker

fake = Faker()

OUTPUT_FILE = "/opt/airflow/data/raw/daily_sales.csv"
NUM_RECORDS = 100000

PAYMENT_METHODS = ["Credit Card", "Debit Card", "Cash", "PayPal", "Apple Pay"]
CHANNELS = ["Online", "In-Store", "Phone", "Mobile App"]

HEADERS = [
    "sale_id",
    "sale_date",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
    "discount_pct",
    "payment_method",
    "channel",
    "region",
]


def maybe_null(value, probability=0.03):
    return None if random.random() < probability else value


def maybe_whitespace(value, probability=0.05):
    if value is not None and random.random() < probability:
        return f"  {value}  "
    return value


def maybe_bad_customer_id(value, probability=0.03):
    if random.random() < probability:
        return random.randint(100001, 110000)
    return value


def maybe_bad_product_id(value, probability=0.03):
    if random.random() < probability:
        return random.randint(100001, 110000)
    return value


def maybe_bad_quantity(value, probability=0.03):
    if random.random() < probability:
        return random.randint(-10, -1)
    return value


def maybe_bad_discount(value, probability=0.03):
    if random.random() < probability:
        return random.choice([-5, 110, 999])
    return value


def maybe_bad_payment_method(value, probability=0.03):
    if random.random() < probability:
        return random.choice(["Crypto", "Unknown", "N/A", ""])
    return value


def maybe_bad_date(value, probability=0.03):
    if random.random() < probability:
        return fake.date(pattern="%m/%d/%Y")
    return value


def generate_daily_sales():
    prior_rows = []

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()

        for i in range(1, NUM_RECORDS + 1):
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(5.0, 500.0), 2)
            discount = random.choice([0, 0, 0, 5, 10, 15, 20, 25])

            quantity = maybe_bad_quantity(quantity)
            discount = maybe_bad_discount(discount)

            try:
                total = round(quantity * unit_price * (1 - discount / 100), 2)
            except Exception:
                total = None

            sale_date = fake.date_between(
                start_date="-1y", end_date="today"
            ).isoformat()

            row = {
                "sale_id": i,
                "sale_date": maybe_bad_date(sale_date),
                "customer_id": maybe_bad_customer_id(
                    random.randint(1, 100000)
                ),
                "product_id": maybe_bad_product_id(
                    random.randint(1, 100000)
                ),
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total,
                "discount_pct": discount,
                "payment_method": maybe_bad_payment_method(
                    maybe_null(random.choice(PAYMENT_METHODS))
                ),
                "channel": maybe_whitespace(
                    maybe_null(random.choice(CHANNELS))
                ),
                "region": maybe_whitespace(
                    maybe_null(fake.state_abbr())
                ),
            }

            # occasional duplicate full row
            if random.random() < 0.02 and prior_rows:
                row = random.choice(prior_rows).copy()

            prior_rows.append(row)
            writer.writerow(row)

    print(f"Generated {NUM_RECORDS} daily sales records in {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_daily_sales()