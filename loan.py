import requests
from pyspark.sql import SparkSession
import os

# Set the path to the MySQL JDBC driver JAR file
jar_location = 'mysql-connector-java-8.0.23.jar'

# Create a SparkSession with the MySQL JDBC driver
spark = SparkSession.builder \
    .appName("mysql_connect") \
    .config("spark.jars", jar_location) \
    .getOrCreate()

# Define db connect parameters
url = "jdbc:mysql://localhost:3306/creditcard_capstone"

# Read environment variables (set these values based on your database credentials)
user = "root"
password = "password"

# 4.1 Get data from API
response = requests.get(
    "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json")

# 4.2 Find status code
print("status code:", response.status_code)

# 4.3 Load data into SQL
rdd = spark.sparkContext.parallelize([response.json()])
loan_df = spark.read.json(rdd)
loan_df.show()

loan_df.write.format("jdbc") \
    .mode("append") \
    .option("url", url) \
    .option("dbtable", "CDW_SAPP_LOAN_APPLICATION") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .save()
