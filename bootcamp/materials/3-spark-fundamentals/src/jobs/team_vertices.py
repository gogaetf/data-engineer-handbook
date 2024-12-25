from pyspark.sql import SparkSession

query = """

WITH teams_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY team_id) as row_num
    FROM teams
)
SELECT
    team_id AS identifier,
    'team' AS vertex_type,
    to_json(named_struct(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
        )) AS properties
FROM teams_deduped
WHERE row_num = 1

"""


def do_team_vertices_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("teams")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("team_vertices") \
      .getOrCreate()
    output_df = do_team_vertices_transformation(spark, spark.table("teams"))
    output_df.write.mode("overwrite").insertInto("vertices")
