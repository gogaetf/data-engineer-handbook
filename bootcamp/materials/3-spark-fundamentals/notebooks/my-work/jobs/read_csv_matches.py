from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os


def read_matches(spark, file_path):
    schema = StructType([
        StructField("match_id", StringType(), True),
        StructField("mapid", StringType(), True),
        StructField("is_team_game", BooleanType(), True),
        StructField("playlist_id", StringType(), True),
        StructField("game_variant_id", StringType(), True),
        StructField("is_match_over", BooleanType(), True),
        StructField("completion_date", DateType(), True),
        StructField("match_duration", DoubleType(), True),
        StructField("game_mode", StringType(), True),
        StructField("map_variant_id", StringType(), True)
    ])

    df = spark.read.csv(file_path, header=True, schema=schema)
    return df


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

    file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..\\",
        'data',
        'matches.csv'
        )

    print('------ Processing matches')
    print('Path to file', file_path)
    matches_df = read_matches(spark, file_path)
    # matches_df.show()

    spark.stop()


if __name__ == '__main__':
    main()
