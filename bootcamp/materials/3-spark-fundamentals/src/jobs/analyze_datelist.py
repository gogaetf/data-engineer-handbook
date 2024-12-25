from pyspark.sql import SparkSession


date_series_query = """
    SELECT sequence(to_date('2023-01-01'), to_date('2023-01-31'), interval 1 day) AS valid_dates
"""

dates_query = """
    SELECT explode(valid_dates) AS valid_date
    FROM date_series
"""

main_query = """
    WITH starter AS (
        SELECT array_contains(uc.dates_active, CAST(d.valid_date AS STRING)) AS is_active,
            datediff(DATE('2023-01-31'), d.valid_date) AS days_since,
            uc.user_id
        FROM users_cumulated uc
        CROSS JOIN dates d
        WHERE uc.date = DATE('2023-01-31')
    ),
    bits AS (
        SELECT user_id,
            CAST(
                SUM(
                    CASE
                        WHEN is_active THEN POW(2, 32 - days_since)
                        ELSE 0
                    END
                ) AS LONG
            )
            AS datelist_int
        FROM starter
        GROUP BY user_id
    )
    SELECT
        user_id,
        lpad(CAST(bin(datelist_int) AS STRING), 32, '0') AS datelist_bin,
        BIT_COUNT(datelist_int) > 0 AS monthly_active,
        BIT_COUNT(datelist_int) AS l32,
        BIT_COUNT(datelist_int & (CAST(4261412864 AS LONG))) > 0 AS weekly_active,
        BIT_COUNT(datelist_int & CAST(4261412864 AS LONG)) AS l7,
        BIT_COUNT(datelist_int & CAST(33292288 AS LONG)) > 0 AS weekly_active_previous_week
    FROM bits
"""


def do_analyze_datelist_transformation(spark, dataframe):

    # Register DataFrame as a temporary view
    dataframe.createOrReplaceTempView("users_cumulated")

    # Generate series of dates and register as a temporary view
    spark.sql(date_series_query).createOrReplaceTempView("date_series")

    # Explode the date series to get individual dates
    spark.sql(dates_query).createOrReplaceTempView("dates")

    # Perform the transformations using Spark SQL and return it
    return spark.sql(main_query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("events_analysis") \
      .getOrCreate()
    output_df = do_analyze_datelist_transformation(spark, spark.table("users_cumulated"))
    output_df.write.mode("overwrite").insertInto("events_analysis")
