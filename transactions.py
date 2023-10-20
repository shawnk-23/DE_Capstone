# Import necessary libraries
# for connecting to the MySQL database
import mysql.connector
# for tabular display of data
from tabulate import tabulate
# for reading configuration settings
import configparser
import os

# Clear the screen (OS-specific)
os.system('cls' if os.name == 'nt' else 'clear')

# Function to display transactions by zip code and date


def display_transactions_by_zip_code_and_date(cursor):
    zip_code = input("Enter the customer's zip code: ")
    year = input("Enter the year (YYYY): ")
    month = input("Enter the month (MM): ")

    # SQL query to retrieve transactions
    query = """
    SELECT cust_zip, transaction_id, transaction_type, transaction_value
    FROM cdw_sapp_credit_card cc
    JOIN cdw_sapp_customer c ON cc.cust_ssn = c.ssn
    WHERE MONTH(timeid) = %s AND YEAR(timeid) = %s AND cust_zip = %s
    ORDER BY DAY(timeid) DESC
    """
    cursor.execute(query, (month, year, zip_code))
    results = cursor.fetchall()

    # Display query results in a tabular format
    if results:
        headers = ['Zip Code', 'Transaction ID',
                   'Transaction Type', 'Transaction Value']
        print(tabulate(results, headers, tablefmt='grid'))
    else:
        print("No results found.")

# Function to display transactions by type


def display_transactions_by_type(cursor):
    # List of available transaction types
    transaction_types = ['Bills', 'Education',
                         'Entertainment', 'Gas', 'Grocery', 'Healthcare', 'Test']

    # Display available transaction types
    print("Available Transaction Types:")
    for i, transaction_type in enumerate(transaction_types, start=1):
        print(f"{i}) {transaction_type}")

    choice_type = input(
        "Enter the number corresponding to the transaction type: ")

    # Validate and select a transaction type
    if choice_type.isnumeric():
        choice_type = int(choice_type)
        if 1 <= choice_type <= len(transaction_types):
            transaction_type = transaction_types[choice_type - 1]

            # SQL query to retrieve transaction count and total value
            query = """
            SELECT COUNT(TRANSACTION_ID), SUM(TRANSACTION_VALUE)
            FROM cdw_sapp_credit_card
            WHERE TRANSACTION_TYPE = %s
            """
            cursor.execute(query, (transaction_type,))
            results = cursor.fetchone()

            # Display query results
            if results[0] > 0:
                headers = ['Number of Transactions', 'Total Value']
                data = [[results[0], f"${results[1]:,.2f}"]]
                print(tabulate(data, headers, tablefmt='grid'))
            else:
                print(f"No {transaction_type} transactions found.\n")
        else:
            print("Invalid choice. Please enter a valid number.\n")
    else:
        print("Invalid input. Please enter a valid number.\n")


def display_transactions_by_branch_state(cursor):
    branch_state = input("Enter the branch state: ")

    # SQL query to retrieve transaction count and total value
    query = """
    SELECT COUNT(TRANSACTION_ID), SUM(TRANSACTION_VALUE)
    FROM cdw_sapp_credit_card c
    JOIN cdw_sapp_branch b ON c.BRANCH_CODE = b.BRANCH_CODE
    WHERE b.BRANCH_STATE = %s
    """
    cursor.execute(query, (branch_state,))
    results = cursor.fetchone()

    # Display query results
    if results[0] > 0:
        headers = ['Number of Transactions', 'Total Value']
        data = [[results[0], f"${results[1]:,.2f}"]]
        print(tabulate(data, headers, tablefmt='grid'))
    else:
        print(f"Transaction not found in {branch_state}.")

# Main program


def main():
    config = configparser.ConfigParser()
    config.read('config.ini')

    Host = 'localhost'
    User = config['database']['user']
    Password = config['database']['password']
    Database = 'creditcard_capstone'

    # Connect to the creditcard_capstone database
    db = mysql.connector.connect(
        host=Host,
        user=User,
        password=Password,
        database=Database
    )

    # Create a cursor to execute SQL queries
    cursor = db.cursor()

    # Display a menu and handle user's choice
    while True:
        print("| ============ Transaction Detail ============ | \n")
        print("1) Display Transactions by Customer Zip Code and Date")
        print("2) Display Number and Total Value of Transactions by Type")
        print("3) Display Number and Total Value of Transactions by Branch State")
        print("4) Quit")

        choice = input("Select your choice (1-4): ")

        if choice == "1":
            os.system('cls' if os.name == 'nt' else 'clear')
            display_transactions_by_zip_code_and_date(cursor)
        elif choice == "2":
            os.system('cls' if os.name == 'nt' else 'clear')
            display_transactions_by_type(cursor)
        elif choice == "3":
            os.system('cls' if os.name == 'nt' else 'clear')
            display_transactions_by_branch_state(cursor)
        elif choice == "4":
            print("Exiting the program...")
            break
        else:
            print("Invalid choice. Try again.")

    # Close the database connection
    db.close()


# Entry point to the app
if __name__ == "__main__":
    main()
