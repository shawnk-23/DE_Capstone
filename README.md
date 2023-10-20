
# Capstone Project Overview

This Capstone Project requires data engineers to work with the following technologies to manage an ETL process for a Loan Application dataset and a Credit Card dataset: Python (Pandas, advanced modules, e.g., Matplotlib), SQL, Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries. As a data engineer, you are expected to set up your development environments and perform installations on their local machines.

# Credit Card Dataset Overview

The Credit Card System database independent system developed for managing activities such as registering new customers and approving or canceling requests, etc., using the architecture. A credit card is issued to users to enact the payment system. It allows the cardholder to access financial services in exchange for the holder's promise to pay for them later. Below are three files that contain the customer’s transaction information and inventories in the credit card information.

# LOAN application Data API Overview

Banks deal in all home loans. They have a presence across all urban, semi-urban, and rural areas. Customers first apply for a home loan; after that, a company will validate the customer's eligibility for a loan.

Banks want to automate the loan eligibility process (in real-time) based on customer details provided while filling out the online application form. These details are Gender, Marital Status, Education, Number of Dependents, Income, Loan Amount, Credit History, and others. To automate this process, they have the task of identifying the customer segments to those who are eligible for loan amounts so that they can specifically target these customers. Here they have provided a partial dataset.

# Workflow Diagram of the Requirements.

![image](https://github.com/shawnk-23/DE_Capstone/assets/136545323/ee865e0f-8c89-412d-9b67-e31d9d899329)


# Project Files

- Etl.py: Used to extract and transform json files then load to DB.
- Transactions.py: Front-end to handle transactional data.
- Customer.py: Front-end to access/modify customer data.
- Graph.py: Used for visualizing transaction results.
- Loan.py: Used to fetch data from API and load to DB.
- Graph2.py: Used for visualizing loan/transaction results.

# References

- API: https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json
- Pyspark: https://spark.apache.org/docs/latest/api/python/index.html
