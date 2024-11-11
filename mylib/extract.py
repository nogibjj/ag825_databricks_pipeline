from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("ReadCSVExample").getOrCreate()

def extract(url):

    # Read the CSV file into a DataFrame
    df = spark.read.csv(url, header=True, inferSchema=True)

    # Show the first few rows of the DataFrame
    df.show()

    # Print the schema of the DataFrame
    df.printSchema()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    url = "https://raw.githubusercontent.com/nogibjj/ag825_sqlite_lab/refs/heads/main/Cancer_Data.csv"

    extract(url)