from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os


def read_medals_matches_players(spark, file_path):
    schema = StructType([
        StructField("match_id", StringType(), True),
        StructField("player_gamertag", StringType(), True),
        StructField("medal_id", LongType(), True),
        StructField("count", LongType(), True)
    ])

    df = spark.read.csv(file_path, header=True, schema=schema)
    return df


def main():
    spark = SparkSession.builder \
    .master("local") \
    .appName("medals") \
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
        "..\\..\\..\\",
        'data',
        'medals_matches_players.csv'
        )

    print('------ Processing medals_matches_player')
    print('Path to file', file_path)
    medals_matches_players_df = read_medals_matches_players(spark, file_path)
    # medals_matches_players_df.show()

    spark.stop()


if __name__ == '__main__':
    main()
