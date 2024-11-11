from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Initialize Spark session
spark = SparkSession.builder.appName("CancerDB_CRUD").getOrCreate()

# Sample schema and data for CancerDB
data = [("123123123", "M", 12.3123312, 21.312312312, 12.3213123, 21.3123213, 23.1231312)]
columns = ["id", "diagnosis", "radius_mean", "texture_mean", "perimeter_mean", "area_mean", "smoothness_mean"]

# Create an initial DataFrame
df = spark.createDataFrame(data, schema=columns)

def create(df):
    # Insert a new record into the DataFrame
    new_record = [("123123124", "B", 15.123, 18.456, 10.789, 20.123, 19.987)]
    new_df = spark.createDataFrame(new_record, schema=columns)
    df = df.union(new_df)
    
    # Show the new state of the DataFrame
    print("After inserting a new record:")
    df.show()
    return df

def read(df):
    # Display the top 5 rows of the DataFrame
    print("Top 5 rows of the CancerDB DataFrame:")
    df.show(5)
    return df

def update(df):
    # Update the record by setting radius_mean to a new value where id='123123123'
    df = df.withColumn("radius_mean", 
                       lit(32.234234423).when(col("id") == "123123123", col("radius_mean"))
                      )
    print("After updating the record with id='123123123':")
    df.filter(col("id") == "123123123").show()
    return df

def delete(df):
    # Delete the record with id='123123123'
    df = df.filter(col("id") != "123123123")
    
    print("After deleting the record with id='123123123':")
    df.show()
    return df

# Perform CRUD operations
df = read(df)
df = create(df)
df = update(df)
df = delete(df)

# Stop Spark session
spark.stop()
