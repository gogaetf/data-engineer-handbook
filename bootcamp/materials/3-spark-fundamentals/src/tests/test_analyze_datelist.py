from chispa.dataframe_comparer import *
from ..jobs.analyze_datelist import do_analyze_datelist_transformation
from collections import namedtuple
from pyspark.sql.types import *


UsersCumulated = namedtuple(
    "UsersCumulated",
    "user_id dates_active date")
UserEvents = namedtuple("UserEvents", "user_id datelist_bin monthly_active l32 weekly_active l7 weekly_active_previous_week")


def test_team_vertices_generation(spark):
    source_data = [
        UsersCumulated("15486602137298500000", ["2023-01-02"], "2023-01-02"),
        UsersCumulated("15486602137298500000", ["2023-01-02"], "2023-01-03"),
        UsersCumulated("15486602137298500000", ["2023-01-02"], "2023-01-04"),
        UsersCumulated("15486602137298500000", ["2023-01-02"], "2023-01-05"),
        UsersCumulated("15486602137298500000", ["2023-01-02"], "2023-01-06"),
        UsersCumulated("15486602137298500000", ["2023-01-02"], "2023-01-07"),
        UsersCumulated("15486602137298500000", ["2023-01-02"], "2023-01-08"),
        UsersCumulated("15486602137298500000", ["2023-01-09","2023-01-02"], "2023-01-09"),
        UsersCumulated("15486602137298500000", ["2023-01-09","2023-01-02"], "2023-01-10"),
        UsersCumulated("15486602137298500000", ["2023-01-09","2023-01-02"], "2023-01-11"),
        UsersCumulated("15486602137298500000", ["2023-01-09","2023-01-02"], "2023-01-12"),
        UsersCumulated("15486602137298500000", ["2023-01-09","2023-01-02"], "2023-01-13"),
        UsersCumulated("15486602137298500000", ["2023-01-09","2023-01-02"], "2023-01-14"),
        UsersCumulated("15486602137298500000", ["2023-01-09","2023-01-02"], "2023-01-15"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-16"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-17"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-18"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-19"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-20"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-21"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-22"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-23"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-24"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-25"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-26"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-27"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-28"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-29"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-30"),
        UsersCumulated("15486602137298500000", ["2023-01-16","2023-01-09","2023-01-02"], "2023-01-31")
    ]
    # user_id
    # datelist_bin
    # monthly_active
    # l32
    # weekly_active
    # l7
    # weekly_active_previous_week
    source_df = spark.createDataFrame(source_data)

    actual_df = do_analyze_datelist_transformation(spark, source_df)
    expected_data = [
        UserEvents("15486602137298500000", "00000000000000100000010000001000", True, 3, False, 0, False)
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("datelist_bin", StringType(), True),
        StructField("monthly_active", BooleanType(), True),
        StructField("l32", IntegerType(), True),
        StructField("weekly_active", BooleanType(), True),
        StructField("l7", IntegerType(), True),
        StructField("weekly_active_previous_week", BooleanType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, schema=schema)

    actual_df.show()
    expected_df.show()

    # assert_df_equality(actual_df, expected_df, ignore_nullable=True)
    assert_df_equality(actual_df, expected_df)
