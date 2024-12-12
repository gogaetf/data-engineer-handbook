from pyspark.sql import SparkSession


query_drop = """

DROP TABLE IF EXISTS bootcamp.match_details

"""

query_create = """

CREATE TABLE IF NOT EXISTS bootcamp.match_details (
    match_id STRING,
    player_gamertag STRING,
    previous_spartan_rank LONG,
    spartan_rank LONG,
    previous_total_xp LONG,
    total_xp LONG,
    previous_csr_tier LONG,
    previous_csr_designation LONG,
    previous_csr LONG,
    previous_csr_percent_to_next_tier LONG,
    previous_csr_rank LONG,
    current_csr_tier LONG,
    current_csr_designation LONG,
    current_csr LONG,
    current_csr_percent_to_next_tier LONG,
    current_csr_rank LONG,
    player_rank_on_team LONG,
    player_finished BOOLEAN,
    player_average_life STRING,
    player_total_kills LONG,
    player_total_headshots LONG,
    player_total_weapon_damage DOUBLE,
    player_total_shots_landed LONG,
    player_total_melee_kills LONG,
    player_total_melee_damage DOUBLE,
    player_total_assassinations LONG,
    player_total_ground_pound_kills LONG,
    player_total_shoulder_bash_kills LONG,
    player_total_grenade_damage DOUBLE,
    player_total_power_weapon_damage DOUBLE,
    player_total_power_weapon_grabs LONG,
    player_total_deaths LONG,
    player_total_assists LONG,
    player_total_grenade_kills LONG,
    did_win LONG,
    team_id LONG
)
USING iceberg
PARTITIONED BY (team_id, bucket(4, match_id))

"""


def ddl_match_details(spark):
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
    ddl_match_details(spark)
    # spark.sql("SHOW TABLES IN bootcamp").show()

    spark.stop()


if __name__ == '__main__':
    main()
