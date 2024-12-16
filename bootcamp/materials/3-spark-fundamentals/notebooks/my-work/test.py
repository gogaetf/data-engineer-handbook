from pyspark.sql import SparkSession
import os
import sys
# import findspark

# findspark.init()
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# print (os.environ['PYSPARK_PYTHON'])
# print (os.environ['PYSPARK_DRIVER_PYTHON'])

spark = SparkSession.builder \
    .appName("testlocal") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "1") \
    .config("spark.ui.showConsoleProgress", "true") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.executor.heartbeatInterval", "30s") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .getOrCreate()


# sc = spark.sparkContext

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)

df.show()
# df.printSchema()

spark.stop()
