import pandas as pd
import mysql.connector
from mysql.connector import pooling
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

# Initialize a connection pool
connection_pool = pooling.MySQLConnectionPool(
    pool_name="my_pool",
    pool_size=5,
    pool_reset_session=True,
    host="localhost",
    user=config['database']['user'],
    password=config['database']['password'],
    database="creditcard_capstone"
)


def get_connection():
    # Get a connection from the pool
    return connection_pool.get_connection()


def display_customer_details(credit_card_no):
    try:
        connection = get_connection()
        cursor = connection.cursor(dictionary=True)
        query = f"SELECT * FROM CDW_SAPP_CUSTOMER WHERE CREDIT_CARD_NO = %s"
        cursor.execute(query, (credit_card_no,))
        result = cursor.fetchall()
        if result:
            # Convert the query result to a Pandas DataFrame
            df = pd.DataFrame(result)
            print(df)
        else:
            print("Customer not found.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        connection.close()


def modify_customer_details(credit_card_no):
    def update_name():
        first_name = input("Enter new first name: ")
        middle_name = input("Enter new middle name: ")
        last_name = input("Enter new last name: ")
        query = "UPDATE CDW_SAPP_CUSTOMER SET FIRST_NAME=%s, MIDDLE_NAME=%s, LAST_NAME=%s WHERE CREDIT_CARD_NO=%s"
        data = (first_name, middle_name, last_name, credit_card_no)
        return query, data

    def update_full_address():
        apt_no = input("Enter apartment number: ")
        street_name = input("Enter street name: ")
        full_address = f"{apt_no}, {street_name}"
        query = "UPDATE CDW_SAPP_CUSTOMER SET FULL_STREET_ADDRESS=%s WHERE CREDIT_CARD_NO=%s"
        data = (full_address, credit_card_no)
        return query, data

    def update_city():
        new_city = input("Enter new city: ")
        query = "UPDATE CDW_SAPP_CUSTOMER SET CUST_CITY=%s WHERE CREDIT_CARD_NO=%s"
        data = (new_city, credit_card_no)
        return query, data

    def update_state():
        new_state = input("Enter new state: ")
        query = "UPDATE CDW_SAPP_CUSTOMER SET CUST_STATE=%s WHERE CREDIT_CARD_NO=%s"
        data = (new_state, credit_card_no)
        return query, data

    def update_zipcode():
        new_zip = input("Enter new zipcode: ")
        query = "UPDATE CDW_SAPP_CUSTOMER SET CUST_ZIP=%s WHERE CREDIT_CARD_NO=%s"
        data = (new_zip, credit_card_no)
        return query, data

    def update_phone():
        phone = input("Enter phone number: ")
        phone = "".join(filter(str.isdigit, phone))
        phone = f"({phone[:3]}){phone[3:6]}-{phone[6:]}"
        query = "UPDATE CDW_SAPP_CUSTOMER SET CUST_PHONE=%s WHERE CREDIT_CARD_NO=%s"
        data = (phone, credit_card_no)
        return query, data

    def update_email():
        new_email = input("Enter new email address: ")
        query = "UPDATE CDW_SAPP_CUSTOMER SET CUST_EMAIL=%s WHERE CREDIT_CARD_NO=%s"
        data = (new_email, credit_card_no)
        return query, data

    print("Which account detail do you want to update?")
    print("1. Name")
    print("2. Full Street Address")
    print("3. City")
    print("4. State")
    print("5. Zipcode")
    print("6. Phone")
    print("7. Email")
    print("8. Exit to main menu")

    while True:
        choice = input("Enter your choice (1-8): ")
        if choice == "1":
            query, data = update_name()
        elif choice == "2":
            query, data = update_full_address()
        elif choice == "3":
            query, data = update_city()
        elif choice == "4":
            query, data = update_state()
        elif choice == "5":
            query, data = update_zipcode()
        elif choice == "6":
            query, data = update_phone()
        elif choice == "7":
            query, data = update_email()
        elif choice == "8":
            break
        else:
            print("Not a valid choice. Try again.")
            continue

        try:
            if query and data:
                connection = get_connection()
                cursor = connection.cursor(dictionary=True)
                cursor.execute(query, data)
                connection.commit()
                print("Update successful.")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            cursor.close()
            connection.close()


def generate_monthly_bill(credit_card_no):
    year = input("Enter year (YYYY): ")
    month = input("Enter month (MM): ")

    timeid = str(year) + str(month).zfill(2)

    try:
        connection = get_connection()
        cursor = connection.cursor(dictionary=True)
        query = "SELECT * FROM CDW_SAPP_CREDIT_CARD WHERE CUST_CC_NO = %s AND TIMEID LIKE %s "
        arg = (credit_card_no, f"{timeid}%")
        cursor.execute(query, arg)
        result = cursor.fetchall()

        if result:
            # Convert the query result to a Pandas DataFrame
            df = pd.DataFrame(result)
            print(df)

            # Calculate total amount due
            total = df["TRANSACTION_VALUE"].sum()
            print(
                f"\nTotal amount due for {month}/{year}: ${total:.2f}\n")
        else:
            print("No transactions found.\n")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        connection.close()


def display_transactions_between_dates(credit_card_no):
    start_date = input("Start date (YYYYMMDD): ")
    end_date = input("End date (YYYYMMDD): ")

    try:
        connection = get_connection()
        cursor = connection.cursor(dictionary=True)
        query = "SELECT * FROM CDW_SAPP_CREDIT_CARD WHERE CUST_CC_NO = %s AND TIMEID BETWEEN %s AND %s ORDER BY TIMEID DESC"
        cursor.execute(query, (credit_card_no, start_date, end_date))
        result = cursor.fetchall()

        if result:
            # Convert the query result to a Pandas DataFrame
            df = pd.DataFrame(result)
            print(df)
        else:
            print("No transactions found.\n")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    while True:
        print("| ============ Customer Detail ============ | \n")
        print("1. Check account details")
        print("2. Modify account details")
        print("3. Generate monthly bill")
        print("4. Display transactions between two dates")
        print("5. Exit")

        option = input("Enter your choice: ")

        if option == "1":
            cc = input("Please enter card number: ")
            display_customer_details(cc)
        elif option == "2":
            cc = input("Please enter card number: ")
            modify_customer_details(cc)
        elif option == "3":
            cc = input("Please enter card number: ")
            generate_monthly_bill(cc)
        elif option == "4":
            cc = input("Please enter card number: ")
            display_transactions_between_dates(cc)
        elif option == "5":
            print("Exiting program...")
            break
        else:
            print("Not a valid choice. Try again.")
