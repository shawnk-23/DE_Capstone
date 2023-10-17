import pandas as pd
import seaborn as sns
import mysql.connector
import matplotlib.pyplot as plt
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

# Connect to the creditcard_capstone database
db = mysql.connector.connect(
    host="localhost",
    user=config['database']['user'],
    password=config['database']['password'],
    database="creditcard_capstone"
)

# 3.1 Find and plot which transaction type has the highest transaction count.
query = """
SELECT TRANSACTION_TYPE, COUNT(*) AS TRANSACTION_COUNT
FROM CDW_SAPP_CREDIT_CARD
GROUP BY TRANSACTION_TYPE
ORDER BY TRANSACTION_COUNT DESC
"""

df = pd.read_sql(query, db)
plt.figure(figsize=(8, 6))
plt.title("Transaction Frequencies by Types")
sns.set(style="whitegrid")
sns.barplot(x="TRANSACTION_TYPE", y="TRANSACTION_COUNT",
            data=df, color='blue')
plt.xlabel("Transaction Type")
plt.xticks(rotation=45, ha='right')
plt.ylabel("Frequency")

# Add count labels on each bar
for index, row in df.iterrows():
    plt.text(index, row['TRANSACTION_COUNT'], str(
        row['TRANSACTION_COUNT']), ha='center', va='bottom')

plt.tight_layout()
plt.show()

# 3.2 Find and plot which state has a high number of customers.
cursor = db.cursor()
query2 = """
SELECT UPPER(CUST_STATE) AS STATE, COUNT(DISTINCT SSN) AS CUSTOMER_COUNT
FROM CDW_SAPP_CUSTOMER
GROUP BY STATE
ORDER BY CUSTOMER_COUNT DESC
"""
cursor.execute(query2)
result = cursor.fetchall()

cnt = [row[1] for row in result]
states = [row[0] for row in result]

fig, ax = plt.subplots(figsize=(10, 6))
plt.bar(states, cnt, width=0.6, color='green')
plt.title('High Number of Customers by States')
plt.xlabel('States')
plt.ylabel('Number of Customers')

# Add count labels on each bar
for index, value in enumerate(cnt):
    plt.text(index, value, str(value), ha='center', va='bottom')

plt.show()

# 3.3 Find and plot the sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
cursor = db.cursor()

# Query to retrieve the top 10 customers with the highest total transaction amounts
query3 = """
SELECT CONCAT(FIRST_NAME, ' ', LAST_NAME) AS NAME, SUM(TRANSACTION_VALUE) AS TOTAL
FROM CDW_SAPP_CREDIT_CARD AS C
JOIN CDW_SAPP_CUSTOMER AS U ON C.CUST_SSN = U.SSN
GROUP BY NAME
ORDER BY TOTAL DESC
LIMIT 10
"""

cursor.execute(query3)
result = cursor.fetchall()

# Create a bar chart showing the total transactions for the top 10 customers
plt.figure(figsize=(12, 8))
plt.bar([row[0] for row in result], [row[1] for row in result], color='red')
plt.title('Total Transactions for Top 10 Customers')
plt.xlabel('Customer Names')
plt.xticks(range(len(result)), [row[0]
           for row in result], rotation=45, ha='right')
plt.ylabel('Total Transactions')
plt.subplots_adjust(bottom=0.19)

# Add count labels on each bar
for index, value in enumerate([row[1] for row in result]):
    rounded_value = round(value, 2)  # Round the count
    plt.text(index, value, str(rounded_value), ha='center', va='bottom')

plt.show()

# Close the database connection
db.close()
