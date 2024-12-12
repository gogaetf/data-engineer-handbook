from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os


def read_match_details(spark, file_path):
    schema = StructType([
        StructField("match_id", StringType(), True),
        StructField("player_gamertag", StringType(), True),
        StructField("previous_spartan_rank", LongType(), True),
        StructField("spartan_rank", LongType(), True),
        StructField("previous_total_xp", LongType(), True),
        StructField("total_xp", LongType(), True),
        StructField("previous_csr_tier", LongType(), True),
        StructField("previous_csr_designation", LongType(), True),
        StructField("previous_csr", LongType(), True),
        StructField("previous_csr_percent_to_next_tier", LongType(), True),
        StructField("previous_csr_rank", LongType(), True),
        StructField("current_csr_tier", LongType(), True),
        StructField("current_csr_designation", LongType(), True),
        StructField("current_csr", LongType(), True),
        StructField("current_csr_percent_to_next_tier", LongType(), True),
        StructField("current_csr_rank", LongType(), True),
        StructField("player_rank_on_team", LongType(), True),
        StructField("player_finished", BooleanType(), True),
        StructField("player_average_life", StringType(), True),
        StructField("player_total_kills", LongType(), True),
        StructField("player_total_headshots", LongType(), True),
        StructField("player_total_weapon_damage", DoubleType(), True),
        StructField("player_total_shots_landed", LongType(), True),
        StructField("player_total_melee_kills", LongType(), True),
        StructField("player_total_melee_damage", DoubleType(), True),
        StructField("player_total_assassinations", LongType(), True),
        StructField("player_total_ground_pound_kills", LongType(), True),
        StructField("player_total_shoulder_bash_kills", LongType(), True),
        StructField("player_total_grenade_damage", DoubleType(), True),
        StructField("player_total_power_weapon_damage", DoubleType(), True),
        StructField("player_total_power_weapon_grabs", LongType(), True),
        StructField("player_total_deaths", LongType(), True),
        StructField("player_total_assists", LongType(), True),
        StructField("player_total_grenade_kills", LongType(), True),
        StructField("did_win", LongType(), True),
        StructField("team_id", LongType(), True)
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
        'match_details.csv'
        )

    print('------ Processing match details')
    print('Path to file', file_path)
    match_details_df = read_match_details(spark, file_path)
    # match_details_df.show()

    spark.stop()


if __name__ == '__main__':
    main()
