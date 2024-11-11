from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit

SparkSession.builder.getOrCreate().stop()
spark = SparkSession.builder.appName("CancerDB").getOrCreate()

columns = ["id", "diagnosis", "radius_mean", "texture_mean", "perimeter_mean", "area_mean", "smoothness_mean"]

df=None

def extract(url):
    df = spark.read.csv(url, header=True, inferSchema=True)
    print("The top 5 rows of the table is below:")
    df.show(5)

    print("The Schema of the table is below:")
    df.printSchema()
    return df

def create(df):
    new_record = [("123123124", "B", 15.123, 18.456, 10.789, 20.123, 19.987)]
    new_df = spark.createDataFrame(new_record, schema=columns)
    df = df.union(new_df)
    
    print("The following record has been inserted:")
    df.filter(df.id == '123123124').show()
    return df

def read(df):
    print("Top 5 rows of the CancerDB DataFrame:")
    df.show(5)
    return df

def update(df):
    print("The following record will be updated to reflect the new radius_mean")
    df.filter(df.id == '123123124').show()
    df = df.withColumn("radius_mean", 
                       when(df.id == "123123124", lit(32.234234423))  
                       .otherwise(col("radius_mean")) 
                      )

    print("After updating the record with id='123123123':")
    df.filter(df.id == "123123124").show()

    return df

def delete(df):
    # Delete the record with id='123123123'
    df = df.filter(df.id != "123123124")
    
    print("After deleting the record with id='123123124': (No output is expected here)")
    df.filter(df.id == "123123124").show()
    return df


if __name__ == "__main__":
    url = "Cancer_Data.csv"
    df=extract(url)
    df=create(df)
    df=read(df)
    df=update(df)
    delete(df)
    spark.stop()

