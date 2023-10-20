# Import necessary libraries and functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, lit, lower, substring, concat, coalesce, lpad
from pyspark.sql.types import IntegerType, TimestampType, DoubleType
import configparser
import findspark
import mysql.connector
findspark.init()

# Initialize SparkSession and configure it with the MySQL connector JAR
config = configparser.ConfigParser()
config.read('config.ini')
jar_location = 'mysql-connector-java-8.0.23.jar'
spark = SparkSession.builder \
    .appName("mysql_connect") \
    .config("spark.jars", jar_location) \
    .getOrCreate()

# creating DB

# Host = 'localhost'
# User = config['database']['user']
# Password = config['database']['password']

# conn = pymysql.connect(host=Host, user=User, password=Password)
# cur = conn.cursor()
# cur.execute("CREATE DATABASE creditcard_capstone")


# Define the URL and properties for the MySQL connection
url = "jdbc:mysql://localhost:3306/creditcard_capstone"
properties = {
    "user": config.get('database', 'user'),
    "password": config.get('database', 'password'),
    "driver": config.get('database', 'driver')
}

# Read JSON files into Spark DataFrames
branch = spark.read.json('cdw_sapp_branch.json')
credit = spark.read.json('cdw_sapp_credit.json')
customer = spark.read.json('cdw_sapp_custmer.json')

# Transform and clean the customer DataFrame
customer_df = customer.withColumn("SSN", customer["SSN"].cast(IntegerType())) \
                      .withColumn("FIRST_NAME", initcap(customer["FIRST_NAME"])) \
                      .withColumn("MIDDLE_NAME", lower(customer["MIDDLE_NAME"])) \
                      .withColumn("LAST_NAME", initcap(customer["LAST_NAME"])) \
                      .withColumn("FULL_STREET_ADDRESS", concat(customer["APT_NO"], lit(','), customer["STREET_NAME"])) \
                      .withColumn("CUST_ZIP", customer['CUST_ZIP'].cast(IntegerType())) \
                      .withColumn("CUST_PHONE", concat(lit("(111)"), substring("CUST_PHONE", 1, 3), lit("-"), substring("CUST_PHONE", 4, 7))) \
                      .withColumn("LAST_UPDATED", customer["LAST_UPDATED"].cast(TimestampType()))

# Drop unnecessary columns
customer_df = customer_df.drop('APT_NO', 'STREET_NAME')

# Transform and clean the branch DataFrame
branch_df = branch.withColumn("BRANCH_CODE", branch["BRANCH_CODE"].cast(IntegerType())) \
                  .withColumn("BRANCH_ZIP", coalesce(branch["BRANCH_ZIP"].cast(IntegerType()), lit(99999))) \
                  .withColumn("BRANCH_PHONE", concat(lit("("), substring("BRANCH_PHONE", 1, 3), lit(")"), substring("BRANCH_PHONE", 4, 3), lit("-"), substring("BRANCH_PHONE", 7, 4))) \
                  .withColumn("LAST_UPDATED", branch["LAST_UPDATED"].cast(TimestampType()))

# Transform and clean the credit DataFrame
credit_df = credit.withColumn("CUST_CC_NO", credit["CREDIT_CARD_NO"]) \
                  .withColumn("TIMEID", concat(credit["YEAR"], lpad(credit["MONTH"], 2, "0"), lpad(credit["DAY"], 2, "0"))) \
                  .withColumn("CUST_SSN", credit["CUST_SSN"].cast(IntegerType())) \
                  .withColumn("BRANCH_CODE", credit["BRANCH_CODE"].cast(IntegerType())) \
                  .withColumn("TRANSACTION_VALUE", credit["TRANSACTION_VALUE"].cast(DoubleType())) \
                  .withColumn("TRANSACTION_ID", credit["TRANSACTION_ID"].cast(IntegerType()))

# Drop unnecessary columns
credit_df = credit_df.drop('DAY', 'MONTH', 'YEAR', 'CREDIT_CARD_NO')

# Write the DataFrames to MySQL
branch_df.write.jdbc(url=url, table="CDW_SAPP_BRANCH",
                     mode="overwrite", properties=properties)
credit_df.write.jdbc(url=url, table="CDW_SAPP_CREDIT_CARD",
                     mode="overwrite", properties=properties)
customer_df.write.jdbc(url=url, table="CDW_SAPP_CUSTOMER",
                       mode="overwrite", properties=properties)
