from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from ddl_maps import ddl_maps
from ddl_match_details import ddl_match_details
from ddl_matches import ddl_matches
from ddl_medals_matches_players import ddl_medals_matches_players
from ddl_medals import ddl_medals
from read_csv_maps import read_maps
from read_csv_match_details import read_match_details
from read_csv_matches import read_matches
from read_csv_medals_matches_players import read_medals_matches_players
from read_csv_medals import read_medals


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

    ddl_maps(spark)
    ddl_match_details(spark)
    ddl_matches(spark)
    ddl_medals_matches_players(spark)
    ddl_medals(spark)
    spark.sql("SHOW TABLES IN bootcamp").show()

    maps_df = read_maps(spark, '../data/maps.csv')
    match_details_df = read_match_details(spark, '../data/match_details.csv')
    matches_df = read_matches(spark, '../data/matches.csv')
    medals_matches_players_df = read_medals_matches_players(spark, '../data/medals_matches_players.csv')
    medals_df = read_medals(spark, '../data/medals.csv')


    maps_df.select(maps_df.columns).write.format("iceberg").mode("overwrite") \
        .saveAsTable("bootcamp.maps")
    match_details_df.select(match_details_df.columns).write.format("iceberg").mode("overwrite") \
        .partitionBy("team_id") \
        .bucketBy(4, "match_id").saveAsTable("bootcamp.match_details")
    matches_df.select(matches_df.columns).write.format("iceberg").mode("overwrite") \
        .partitionBy(["is_team_game", "completion_date"]) \
        .bucketBy(4, "match_id").saveAsTable("bootcamp.matches")
    medals_matches_players_df.select(medals_matches_players_df.columns).write.format("iceberg").mode("overwrite") \
        .bucketBy(4, "match_id").saveAsTable("bootcamp.medals_matches_players")
    medals_df.select(medals_df.columns).write.format("iceberg").mode("overwrite") \
        .saveAsTable("bootcamp.medals")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    # broadcast join matches and maps
    matches_maps_join_df = matches_df.join(
        broadcast(maps_df),
        maps_df("mapid")==matches_df("left")
        ).show()

    # matches_maps_join_df.explain()

    # broadcast join medals_matches_players and medals
    medals_matches_players_medals_join_df = medals_matches_players_df.join(
        broadcast(medals_df),
        medals_df("mapid")==medals_matches_players_df("left")
        ).show()

    # medals_matches_players_medals_join_df.explain()

    spark.stop()


if __name__ == '__main__':
    main()