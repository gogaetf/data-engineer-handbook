from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, countDistinct, col
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
    # spark.sql("SHOW TABLES IN bootcamp").show()

    maps_df = read_maps(spark, '../data/maps.csv')
    match_details_df = read_match_details(spark, '../data/match_details.csv')
    matches_df = read_matches(spark, '../data/matches.csv')
    medals_matches_players_df = read_medals_matches_players(spark, '../data/medals_matches_players.csv')
    medals_df = read_medals(spark, '../data/medals.csv')

    match_details_df.repartition("team_id") \
        .sortWithinPartitions(col("player_gamertag"))
    matches_df.repartition("is_team_game", "completion_date") \
        .sortWithinPartitions(col("mapid"))

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
    matches_maps_join_df = matches_df.join(broadcast(maps_df.alias("maps")), "mapid", "left") \
        .drop("maps.mapid") \
        .withColumnRenamed("description", "map_description") \
        .withColumnRenamed("name", "map_name")
    # matches_maps_join_df.printSchema()
    # matches_maps_join_df.show()
    # matches_maps_join_df.explain()

    # broadcast join medals_matches_players and medals
    medals_matches_players_medals_join_df = \
        medals_matches_players_df.join(broadcast(medals_df.alias("medals")), "medal_id", "left") \
            .drop("medals.medal_id") \
            .withColumnRenamed("description", "medal_description") \
            .withColumnRenamed("name", "medal_name")
    # medals_matches_players_medals_join_df.printSchema()
    # medals_matches_players_medals_join_df.show()
    # medals_matches_players_medals_join_df.explain()

    # bucket join of all dataframes
    mid_join_df = matches_maps_join_df.join(
        match_details_df.alias("match_details"), "match_id", "left") \
        .drop("match_details.match_id")
    # mid_join_df.printSchema()

    large_join_df = mid_join_df.join(
        medals_matches_players_medals_join_df.drop("player_gamertag").alias("medals_matches_players"),
        "match_id",
        "left"
        ).drop("medals_matches_players.match_id")
    # large_join_df.printSchema()
    # large_join_df.show()

    total_kills_per_player = large_join_df.groupBy("player_gamertag", "match_id")\
        .avg("player_total_kills") \
        .withColumnRenamed("avg(player_total_kills)", "player_total_kills") \
        .groupBy("player_gamertag") \
        .avg("player_total_kills") \
        .withColumnRenamed("avg(player_total_kills)", "avg_player_total_kills") \
        .orderBy("avg_player_total_kills", ascending=False)
    print("Which player averages the most kills per game?")
    total_kills_per_player.show()
    # total_kills_per_player.explain()

    playlist_most_played = large_join_df.groupBy("playlist_id") \
        .agg(countDistinct("match_id")) \
        .withColumnRenamed("count(DISTINCT match_id)", "num_matches") \
        .orderBy("num_matches", ascending=False)
    print("Which playlist gets played the most?")
    playlist_most_played.show()
    # playlist_most_played.explain()

    map_most_played = large_join_df.groupBy("mapid") \
        .agg(countDistinct("match_id")) \
        .withColumnRenamed("count(DISTINCT match_id)", "num_matches") \
        .orderBy("num_matches", ascending=False)
    print("Which map gets played the most?")
    map_most_played.show()
    # map_most_played.explain()

    map_most_selected_medal = large_join_df.where(large_join_df["medal_name"]=="Killing Spree") \
        .groupBy("mapid", "medal_id", "medal_name") \
        .agg(countDistinct("match_id")) \
        .withColumnRenamed("count(DISTINCT match_id)", "num_matches") \
        .orderBy("num_matches", ascending=False)
    print("Which map do players get the most Killing Spree medals on?")
    map_most_selected_medal.show()
    # map_most_selected_medal.explain()

    spark.stop()


if __name__ == '__main__':
    main()
