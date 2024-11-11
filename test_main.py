
from main import extract, create, read, update, delete
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit



def test_all():
    SparkSession.builder.getOrCreate().stop()
    spark = SparkSession.builder.appName("CancerDB").getOrCreate()
    url = "Cancer_Data.csv"
    df = spark.read.csv(url, header=True, inferSchema=True)
    

    assert extract(df) is not None

if __name__ == "__main__":
    test_all()