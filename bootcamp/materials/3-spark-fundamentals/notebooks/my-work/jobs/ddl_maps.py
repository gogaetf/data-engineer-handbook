from pyspark.sql import SparkSession


query_drop = """

DROP TABLE PURGE IF EXISTS bootcamp.maps

"""

query_create = """

CREATE TABLE IF NOT EXISTS bootcamp.maps (
    mapid STRING,
    name STRING,
    description STRING
)
USING iceberg

"""


def ddl_maps(spark):
    spark.sql(query_drop)
    spark.sql(query_create)


def main():
    spark = SparkSession.builder \
    .master("local") \
    .appName("bootcampmain") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "50") \
    .getOrCreate()

    # spark.sql("SHOW TABLES IN bootcamp").show()
    ddl_maps(spark)
    # spark.sql("SHOW TABLES IN bootcamp").show()

    spark.stop()


if __name__ == '__main__':
    main()
