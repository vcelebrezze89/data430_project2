from pyspark.sql import SparkSession


def get_spark_session(app_name="ETL_Spark_App"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    return spark
