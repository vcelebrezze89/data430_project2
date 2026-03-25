from pyspark.sql import functions as F
from spark_utils import get_spark_session


def customer_lifetime_value(spark, sales_path, output_path):
    sales_df = spark.read.parquet(sales_path)

    clv_df = (
        sales_df.groupBy("customer_id")
        .agg(
            F.count("sale_id").alias("total_orders"),
            F.round(F.sum("total_amount"), 2).alias("lifetime_value"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.max("sale_date").alias("last_order_date"),
        )
    )

    clv_df.write.mode("overwrite").parquet(f"{output_path}/customer_lifetime_value")
    print("Customer lifetime value analytics written.")


def sales_trend(spark, sales_path, output_path):
    sales_df = spark.read.parquet(sales_path)

    trend_df = (
        sales_df.groupBy("sale_date")
        .agg(
            F.count("sale_id").alias("total_transactions"),
            F.round(F.sum("total_amount"), 2).alias("total_sales"),
            F.round(F.sum("discount_pct"), 2).alias("total_discount"),
        )
        .orderBy("sale_date")
    )

    trend_df.write.mode("overwrite").parquet(f"{output_path}/sales_trend")
    print("Sales trend analytics written.")


def category_performance(spark, sales_path, products_path, output_path):
    sales_df = spark.read.parquet(sales_path)
    products_df = spark.read.parquet(products_path)

    joined_df = sales_df.join(products_df, on="product_id", how="inner")

    category_df = (
        joined_df.groupBy("category")
        .agg(
            F.countDistinct("product_id").alias("total_products"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("price"), 2).alias("avg_price"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    category_df.write.mode("overwrite").parquet(f"{output_path}/category_performance")
    print("Category performance analytics written.")


def main():
    spark = get_spark_session("spark_analytics")

    cleaned_base = "/opt/spark/data/cleaned"
    analytics_base = "/opt/spark/data/analytics"

    customer_lifetime_value(
        spark,
        f"{cleaned_base}/daily_sales",
        analytics_base,
    )

    sales_trend(
        spark,
        f"{cleaned_base}/daily_sales",
        analytics_base,
    )

    category_performance(
        spark,
        f"{cleaned_base}/daily_sales",
        f"{cleaned_base}/products",
        analytics_base,
    )

    spark.stop()


if __name__ == "__main__":
    main()