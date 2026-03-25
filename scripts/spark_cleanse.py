import sys
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from spark_utils import get_spark_session


def clean_customers(spark, input_path, output_path):
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df = (
        df.dropDuplicates()
        .withColumn("first_name", F.trim(F.col("first_name")))
        .withColumn("last_name", F.trim(F.col("last_name")))
        .withColumn("city", F.trim(F.col("city")))
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("email").isNotNull())
        .withColumn("registration_date", F.to_date("registration_date"))
        .withColumn("date_of_birth", F.to_date("date_of_birth"))
    )

    df.write.mode("overwrite").parquet(output_path)
    print(f"Cleaned customers written to {output_path}")


def clean_suppliers(spark, input_path, output_path):
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df = (
        df.dropDuplicates()
        .withColumn("company_name", F.trim(F.col("company_name")))
        .withColumn("contact_name", F.trim(F.col("contact_name")))
        .withColumn("industry", F.trim(F.col("industry")))
        .filter(F.col("supplier_id").isNotNull())
        .filter(F.col("rating").between(1.0, 5.0))
        .withColumn("contract_start_date", F.to_date("contract_start_date"))
    )

    df.write.mode("overwrite").parquet(output_path)
    print(f"Cleaned suppliers written to {output_path}")


def clean_products(spark, input_path, output_path):
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df = (
        df.dropDuplicates()
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.trim(F.col("category")))
        .withColumn("sku", F.trim(F.col("sku")))
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("price") > 0)
        .filter(F.col("cost") > 0)
        .filter(F.col("stock_quantity") >= 0)
        .withColumn("created_date", F.to_date("created_date"))
    )

    df.write.mode("overwrite").parquet(output_path)
    print(f"Cleaned products written to {output_path}")


def clean_daily_sales(spark, input_path, output_path):
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df = (
        df.dropDuplicates()
        .withColumn("payment_method", F.trim(F.col("payment_method")))
        .withColumn("channel", F.trim(F.col("channel")))
        .withColumn("region", F.trim(F.col("region")))
        .filter(F.col("sale_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("discount_pct").between(0, 100))
        .withColumn("sale_date", F.to_date("sale_date"))
    )

    df.write.mode("overwrite").parquet(output_path)
    print(f"Cleaned daily sales written to {output_path}")


def main():
    if len(sys.argv) != 4:
        print("Usage: spark_cleanse.py <dataset> <input_path> <output_path>")
        sys.exit(1)

    dataset = sys.argv[1]
    input_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = get_spark_session(f"clean_{dataset}")

    if dataset == "customers":
        clean_customers(spark, input_path, output_path)
    elif dataset == "suppliers":
        clean_suppliers(spark, input_path, output_path)
    elif dataset == "products":
        clean_products(spark, input_path, output_path)
    elif dataset == "daily_sales":
        clean_daily_sales(spark, input_path, output_path)
    else:
        print(f"Unknown dataset: {dataset}")
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()