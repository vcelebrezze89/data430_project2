import csv
import random
from faker import Faker

fake = Faker()

OUTPUT_FILE = "/opt/airflow/data/raw/customers.csv"
NUM_RECORDS = 100000

HEADERS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "address",
    "city",
    "state",
    "zip_code",
    "date_of_birth",
    "registration_date",
]


def maybe_null(value, probability=0.05):
    return None if random.random() < probability else value


def maybe_whitespace(value, probability=0.05):
    if random.random() < probability:
        return f"  {value}  "
    return value


def maybe_bad_phone(value, probability=0.03):
    if random.random() < probability:
        return "INVALID"
    return value


def maybe_bad_zip(value, probability=0.03):
    if random.random() < probability:
        return "XXXXX"
    return value


def maybe_bad_date(value, probability=0.03):
    if random.random() < probability:
        return fake.date(pattern="%m/%d/%Y")
    return value


def generate_customers():
    emails = []

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()

        for i in range(1, NUM_RECORDS + 1):

            email = fake.email()

            # create duplicates
            if random.random() < 0.02 and emails:
                email = random.choice(emails)

            emails.append(email)

            dob = fake.date_of_birth(
                minimum_age=18, maximum_age=80
            ).isoformat()

            reg_date = fake.date_between(
                start_date="-3y", end_date="today"
            ).isoformat()

            writer.writerow(
                {
                    "customer_id": i,
                    "first_name": maybe_whitespace(
                        maybe_null(fake.first_name())
                    ),
                    "last_name": maybe_whitespace(
                        maybe_null(fake.last_name())
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
                    "zip_code": maybe_bad_zip(
                        maybe_null(fake.zipcode())
                    ),
                    "date_of_birth": maybe_bad_date(dob),
                    "registration_date": maybe_bad_date(reg_date),
                }
            )

    print(f"Generated {NUM_RECORDS} customer records in {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_customers()