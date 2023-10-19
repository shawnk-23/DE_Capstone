import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

# Connect to the MySQL database

db = mysql.connector.connect(
    host="localhost",
    user=config['database']['user'],
    password=config['database']['password'],
    database="creditcard_capstone"
)

# Load data from the database table into a DataFrame
loan_df = pd.read_sql("SELECT * FROM CDW_SAPP_LOAN_APPLICATION", con=db)

# Figure 1: Self-Employed Applicants Approval Rate
self_employed = loan_df[loan_df['Self_Employed'] == 'Yes']
approved_self_employed = self_employed[self_employed['Application_Status'] == 'Y']
total_self_employed = self_employed.shape[0]
approval_percentage = round(
    (approved_self_employed.shape[0] / total_self_employed) * 100, 2)

labels = ['Approved', 'Not Approved']
colors = ['#5DBE98', '#FFBB51']
sizes = [approval_percentage, round(100 - approval_percentage, 2)]

plt.figure(1)
plt.pie(sizes, labels=labels, explode=(0.1, 0),
        colors=colors, autopct='%1.2f%%', startangle=90)
plt.title('Approval Rate for Self-Employed Applicants')
plt.axis('equal')
plt.show()

# Figure 2: Rejection Rate for Married Male Applicants
married_male_rejected = loan_df[(loan_df['Married'] == 'Yes') & (
    loan_df['Gender'] == 'Male') & (loan_df['Application_Status'] == 'N')]
total_married_male = loan_df[(loan_df['Married'] == 'Yes') & (
    loan_df['Gender'] == 'Male')].shape[0]
rejection_percentage = round(
    (married_male_rejected.shape[0] / total_married_male) * 100, 2)

labels = ['Rejected', 'Approved']
colors = ['#5F88EE', '#FF884D']
sizes = [rejection_percentage, round(100 - rejection_percentage, 2)]

plt.figure(2)
plt.pie(sizes, labels=labels, explode=(0.1, 0),
        colors=colors, autopct='%1.2f%%', startangle=90)
plt.title('Rejection Rate for Married Male Applicants')
plt.axis('equal')
plt.show()

# Figure 3: Top Three Months with Largest Transaction Data
cursor = db.cursor()
query = "SELECT DATE_FORMAT(timeid, '%Y-%M') as month, SUM(transaction_value) AS Total FROM CDW_SAPP_CREDIT_CARD GROUP BY month ORDER BY Total DESC LIMIT 3"
cursor.execute(query)
data = cursor.fetchall()
months = [row[0] for row in data]
transaction_values = [round(row[1], 2) for row in data]

plt.figure(3, figsize=(10, 6))  # Larger figure size
plt.bar(months, transaction_values, color='#5AB3A5')
plt.xlabel('Month')
plt.ylabel('Total Transaction Value')
plt.title('Top Three Months with Largest Transaction Data')
plt.ylim(bottom=200000)
plt.xticks(rotation=0)

# Add count labels on top of each bar
for i, v in enumerate(transaction_values):
    plt.text(i, v, str(v), ha='center', va='bottom')

plt.show()

# Figure 4: Branches with Highest Healthcare Transaction Value
query = "SELECT branch_code, SUM(transaction_value) AS Total FROM CDW_SAPP_CREDIT_CARD WHERE TRANSACTION_TYPE = 'HEALTHCARE' GROUP BY branch_code ORDER BY Total DESC LIMIT 5"
cursor.execute(query)
data = cursor.fetchall()
branch_codes = [str(row[0]) for row in data]
healthcare_transaction_values = [round(row[1], 2) for row in data]

plt.figure(4, figsize=(10, 6))  # Larger figure size
plt.bar(branch_codes, healthcare_transaction_values, color='#8A9EB5')
plt.xlabel('Branch Code')
plt.ylabel('Total Transaction Value')
plt.title('Top Branches with Highest Healthcare Transaction Value')
plt.xticks(rotation=45)

# Add count labels on top of each bar
for i, v in enumerate(healthcare_transaction_values):
    plt.text(i, v, str(v), ha='center', va='bottom')

plt.show()

cursor.close()
db.close()
