# Import necessary libraries
import requests
from pyspark.sql import SparkSession
import os
import configparser

# Read database configuration from 'config.ini' using configparser
config = configparser.ConfigParser()
config.read('config.ini')

# Set the path to the MySQL JDBC driver JAR file
jar_location = 'mysql-connector-java-8.0.23.jar'

# Create a SparkSession with the MySQL JDBC driver
spark = SparkSession.builder \
    .appName("mysql_connect") \
    .config("spark.jars", jar_location) \
    .getOrCreate()

# Define database connection parameters
url = "jdbc:mysql://localhost:3306/creditcard_capstone"

# Read database user and password from the configuration file
user = config['database']['user']
password = config['database']['password']

# 4.1 Get data from an API
response = requests.get(
    "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json")

# 4.2 Find the HTTP status code of the API response
print("status code:", response.status_code)

# 4.3 Load data into a Spark DataFrame
# Convert the JSON response from the API into a Spark DataFrame
rdd = spark.sparkContext.parallelize([response.json()])
loan_df = spark.read.json(rdd)

# Show the first few rows of the DataFrame
loan_df.show()

# Write the DataFrame to a MySQL table
loan_df.write.format("jdbc") \
    .mode("append") \
    .option("url", url) \
    .option("dbtable", "CDW_SAPP_LOAN_APPLICATION") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .save()
