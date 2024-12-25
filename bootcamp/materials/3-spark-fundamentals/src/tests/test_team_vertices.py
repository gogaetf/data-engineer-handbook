from chispa.dataframe_comparer import *
from ..jobs.team_vertices import do_team_vertices_transformation
from collections import namedtuple
import json
from pyspark.sql.types import *


Teams = namedtuple(
    "Teams",
    "league_id team_id min_year max_year abbreviation nickname yearfounded city arena arenacapacity owner generalmanager headcoach dleagueaffiliation")
TeamsJson = namedtuple("TeamsJson", "identifier vertex_type properties")


def test_team_vertices_generation(spark):
    source_data = [
        Teams(0, 1610612737, 1949, 2019, "ATL", "Hawks", 1949, "Atlanta", "State Farm Arena", 18729, "Tony Ressler", "Travis Schlenk", "Lloyd Pierce", "Erie Bayhawks"),
        Teams(0, 1610612737, 1949, 2019, "ATL", "Hawks", 1949, "Atlanta", "State Farm Arena", 18729, "Tony Ressler", "Travis Schlenk", "Lloyd Pierce", "Erie Bayhawks"),
        Teams(0, 1610612737, 1949, 2019, "ATL", "Hawks", 1949, "Atlanta", "State Farm Arena", 18729, "Tony Ressler", "Travis Schlenk", "Lloyd Pierce", "Erie Bayhawks"),
        Teams(0, 1610612737, 1949, 2019, "ATL", "Hawks", 1949, "Atlanta", "State Farm Arena", 18729, "Tony Ressler", "Travis Schlenk", "Lloyd Pierce", "Erie Bayhawks")
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_team_vertices_transformation(spark, source_df)
    expected_data_json_str = str(json.dumps({
        "abbreviation": "ATL",
        "nickname": "Hawks",
        "city": "Atlanta",
        "arena": "State Farm Arena",
        "year_founded": 1949
        })).replace(", ", ",").replace(": ", ":")
    expected_data = [
        TeamsJson(1610612737, "team", expected_data_json_str)
    ]

    schema = StructType([
        StructField("identifier", LongType(), True),
        StructField("vertex_type", StringType(), False),
        StructField("properties", StringType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, schema=schema)

    # assert_df_equality(actual_df, expected_df, ignore_nullable=True)
    assert_df_equality(actual_df, expected_df)
