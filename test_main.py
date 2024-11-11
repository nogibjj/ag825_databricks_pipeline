from main import extract, create, read, update, delete
from pyspark.sql import SparkSession

# from pyspark.sql.functions import when, col, lit


def test_all():
    url = "Cancer_Data.csv"
    #    df = spark.read.csv(url, header=True, inferSchema=True)
    op = extract(url)

    assert op.count()>0

    op1=create(op)
    assert op1.count()>0
    
    op1=read(op1)
    assert op1.count()>0
    
    op1=update(op1)
    assert op1.count()>0
    
    op1=delete(op1)
    assert op1.count()>0


if __name__ == "__main__":
    SparkSession.builder.getOrCreate().stop()
    spark = SparkSession.builder.appName("CancerDB").getOrCreate()
    test_all()
