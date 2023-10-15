from pyspark.sql import SparkSession
import findspark
import configparser
findspark.init()

jar_location = 'mysql-connector-java-8.0.23.jar'
spark = SparkSession.builder \
    .appName("mysql_connect") \
    .config("spark.jars", jar_location) \
    .getOrCreate()

# Define db connect parameters
url = "jdbc:mysql://localhost:3306/creditcard_capstone"

config = configparser.ConfigParser()
config.read('config.ini')

db_properties = {
    "user": config.get('database', 'user'),
    "password": config.get('database', 'password'),
    "driver": config.get('database', 'driver')
}


# Load tables into DataFrames
branch_dataframe = spark.read.jdbc(
    url, "creditcard_capstone.CDW_SAPP_BRANCH", properties=db_properties)
credit_dataframe = spark.read.jdbc(
    url, "creditcard_capstone.CDW_SAPP_CREDIT_CARD", properties=db_properties)
customer_dataframe = spark.read.jdbc(
    url, "creditcard_capstone.CDW_SAPP_CUSTOMER", properties=db_properties)

# Create temporary views for the DataFrames
branch_dataframe.createOrReplaceTempView("BRANCH")
credit_dataframe.createOrReplaceTempView("CREDIT")
customer_dataframe.createOrReplaceTempView("CUSTOMER")

# Function to execute and show a SQL query


def execute_and_show_query(query):
    result_dataframe = spark.sql(query)
    result_dataframe.show(result_dataframe.count())
    print(f"{result_dataframe.count()} row(s) fetched")

# Transaction function to display transactions by zip code and date


def display_transactions_by_zip_date():
    while True:
        zip_code = input("Enter Zip Code: ")
        if not zip_code.isnumeric():
            print("Invalid Zip Code. Please try again!")
        else:
            break
    month = input("Enter Month (2 digits): ")
    year = input("Enter Year (4 digits): ")
    if len(month) == 1:
        month = "0" + month

    query = f"""SELECT cc.*
              FROM CREDIT cc
              LEFT JOIN CUSTOMER c
              ON cc.CUST_SSN = c.SSN
              WHERE c.cust_zip = {zip_code}
              AND SUBSTRING(cc.TIMEID, 1, 4) = '{year}'
              AND SUBSTRING(cc.TIMEID, 5, 2) = '{month}'
              ORDER BY TIMEID DESC"""

    execute_and_show_query(query)

# Transaction function to display number and total value of transactions by type


def display_transactions_by_type():
    credit_dataframe.select("TRANSACTION_TYPE").distinct().show()
    transaction_type = input("Select Transaction Type: ")

    query = f"""SELECT TRANSACTION_TYPE, COUNT(*) as NUMBER, ROUND(SUM(TRANSACTION_VALUE), 2) as TOTAL_VALUES
                FROM CREDIT AS cc
                WHERE TRANSACTION_TYPE = '{transaction_type}'
                GROUP BY TRANSACTION_TYPE;"""

    execute_and_show_query(query)

# Transaction function to display number and total value of transactions by branch state


def display_transactions_by_branch_state():
    state = input("Enter State: ")

    query = f"""SELECT b.BRANCH_CODE, b.BRANCH_STATE, COUNT(*) as NUMBER, ROUND(SUM(cc.TRANSACTION_VALUE), 2) as TOTAL_VALUES
                FROM CREDIT as cc, BRANCH as b
                WHERE cc.BRANCH_CODE = b.BRANCH_CODE and (b.BRANCH_STATE='{state}')
                GROUP BY b.BRANCH_CODE, b.BRANCH_STATE
                ORDER BY b.BRANCH_CODE;"""

    execute_and_show_query(query)

# Main program


def main():
    while True:
        print("1. Display all transactions by zip and date")
        print("2. Display number and total value by type")
        print("3. Display total number and total value by branch state")
        print("4. Exit")

        choice = input("Enter your choice (1-4): ")

        if choice == '1':
            display_transactions_by_zip_date()
        elif choice == '2':
            display_transactions_by_type()
        elif choice == '3':
            display_transactions_by_branch_state()
        elif choice == '4':
            print("Exiting...")
            break
        else:
            print("Please try again!")


if __name__ == "__main__":
    main()
