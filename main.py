from pyspark.sql import SparkSession
import os, sys

# os.environ["PYSPARK_PYTHON"] = sys.executable
# os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("Simple Spark Job") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

data = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]

df = spark.createDataFrame(data, ["Name", "Age"])

df.filter(df.Age > 30).show()

spark.stop()