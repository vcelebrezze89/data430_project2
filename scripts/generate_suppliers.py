import csv
import random
from faker import Faker

fake = Faker()

OUTPUT_FILE = "/opt/airflow/data/raw/suppliers.csv"
NUM_RECORDS = 100000

INDUSTRIES = [
    "Manufacturing",
    "Wholesale",
    "Distribution",
    "Import/Export",
    "Agriculture",
    "Technology",
    "Textiles",
    "Chemicals",
    "Packaging",
    "Raw Materials",
]

HEADERS = [
    "supplier_id",
    "company_name",
    "contact_name",
    "email",
    "phone",
    "address",
    "city",
    "state",
    "country",
    "industry",
    "rating",
    "contract_start_date",
]


def maybe_null(value, probability=0.05):
    return None if random.random() < probability else value


def maybe_whitespace(value, probability=0.05):
    if value is not None and random.random() < probability:
        return f"  {value}  "
    return value


def maybe_bad_phone(value, probability=0.03):
    if random.random() < probability:
        return "INVALID"
    return value


def maybe_bad_rating(value, probability=0.03):
    if random.random() < probability:
        return random.choice([-1, 0, 6, 9.9])
    return value


def maybe_bad_date(value, probability=0.03):
    if random.random() < probability:
        return fake.date(pattern="%m/%d/%Y")
    return value


def generate_suppliers():
    emails = []

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()

        for i in range(1, NUM_RECORDS + 1):
            email = fake.company_email()

            # create duplicate emails
            if random.random() < 0.02 and emails:
                email = random.choice(emails)

            emails.append(email)

            contract_date = fake.date_between(
                start_date="-5y", end_date="today"
            ).isoformat()

            writer.writerow(
                {
                    "supplier_id": i,
                    "company_name": maybe_whitespace(
                        maybe_null(fake.company())
                    ),
                    "contact_name": maybe_whitespace(
                        maybe_null(fake.name())
                    ),
                    "email": maybe_null(email),
                    "phone": maybe_bad_phone(
                        maybe_null(fake.phone_number())
                    ),
                    "address": maybe_null(fake.street_address()),
                    "city": maybe_whitespace(
                        maybe_null(fake.city())
                    ),
                    "state": maybe_null(fake.state_abbr()),
                    "country": maybe_whitespace(
                        maybe_null(fake.country())
                    ),
                    "industry": maybe_whitespace(
                        maybe_null(random.choice(INDUSTRIES))
                    ),
                    "rating": maybe_bad_rating(
                        round(random.uniform(1.0, 5.0), 1)
                    ),
                    "contract_start_date": maybe_bad_date(contract_date),
                }
            )

    print(f"Generated {NUM_RECORDS} supplier records in {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_suppliers()