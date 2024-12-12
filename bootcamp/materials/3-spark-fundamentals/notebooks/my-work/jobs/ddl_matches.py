from pyspark.sql import SparkSession


query_drop = """

DROP TABLE IF EXISTS bootcamp.matches

"""

query_create = """

CREATE TABLE IF NOT EXISTS bootcamp.matches (
    match_id STRING,
    mapid STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    game_variant_id STRING,
    is_match_over BOOLEAN,
    completion_date DATE,
    match_duration DOUBLE,
    game_mode STRING,
    map_variant_id STRING
)
USING iceberg
PARTITIONED BY (bucket(4, match_id))

"""


def ddl_matches(spark):
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

    # Create a Spark session with Iceberg REST catalog configuration
    # spark = SparkSession.builder \
    #     .master("local") \
    #     .appName("Iceberg with Docker") \
    #     .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
    #     .config("spark.sql.catalog.rest.type", "rest") \
    #     .config("spark.sql.catalog.rest.uri", "http://localhost:8181") \
    #     .config("spark.sql.catalog.rest.warehouse", "s3://warehouse/") \
    #     .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    #     .config("spark.sql.catalog.rest.s3.endpoint", "http://localhost:9000") \
    #     .config("spark.sql.catalog.rest.s3.access-key-id", "admin") \
    #     .config("spark.sql.catalog.rest.s3.secret-access-key", "password") \
    #     .config("spark.sql.catalog.rest.s3.region", "us-east-1") \
    #     .config("spark.executor.memory", "4g") \
    #     .config("spark.driver.memory", "4g") \
    #     .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    #     .config("spark.sql.shuffle.partitions", "200") \
    #     .config("spark.sql.files.maxPartitionBytes", "134217728") \
    #     .config("spark.dynamicAllocation.enabled", "true") \
    #     .config("spark.dynamicAllocation.minExecutors", "1") \
    #     .config("spark.dynamicAllocation.maxExecutors", "50") \
    #     .getOrCreate()

    # spark.sql("SHOW TABLES IN bootcamp").show()
    ddl_matches(spark)
    # spark.sql("SHOW TABLES IN bootcamp").show()

    spark.stop()
    # : org.apache.spark.SparkClassNotFoundException:
    # [DATA_SOURCE_NOT_FOUND] Failed to find the data source: iceberg.
    # Please find packages at `https://spark.apache.org/third-party-projects.html`.


if __name__ == '__main__':
    main()
