
# Capstone Project Overview

This Capstone Project requires data engineers to work with the following technologies to manage an ETL process for a Loan Application dataset and a Credit Card dataset: Python (Pandas, advanced modules, e.g., Matplotlib), SQL, Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries. As a data engineer, you are expected to set up your development environments and perform installations on their local machines.

## Credit Card Dataset Overview

The Credit Card System database independent system developed for managing activities such as registering new customers and approving or canceling requests, etc., using the architecture. A credit card is issued to users to enact the payment system. It allows the cardholder to access financial services in exchange for the holder's promise to pay for them later. Below are three files that contain the customer’s transaction information and inventories in the credit card information.

## LOAN application Data API Overview

Banks deal in all home loans. They have a presence across all urban, semi-urban, and rural areas. Customers first apply for a home loan; after that, a company will validate the customer's eligibility for a loan.

Banks want to automate the loan eligibility process (in real-time) based on customer details provided while filling out the online application form. These details are Gender, Marital Status, Education, Number of Dependents, Income, Loan Amount, Credit History, and others. To automate this process, they have the task of identifying the customer segments to those who are eligible for loan amounts so that they can specifically target these customers. Here they have provided a partial dataset.

## Workflow Diagram of the Requirements.

![image](https://github.com/shawnk-23/DE_Capstone/assets/136545323/2f969e5b-d911-490e-9676-52d62684fce1)


## Project Files

- Etl.py: Used to extract and transform json files then load to DB.
- Transactions.py: Front-end for user to interact with the data related to transactions.
- Customer.py: Front-end to access/modify customer data.
- Graph.py: Used for visualizing transaction results.
- Loan.py: Used to fetch data from API and load to DB.
- Graph2.py: Used for visualizing loan/transaction results.

## How code works

### 1)etl.py
It imports necessary libraries and functions for working with Spark, MySQL, and configuration.
Initializes a SparkSession, sets the application name, and configures it with the MySQL connector JAR.
Reads database configuration from a config.ini file and sets the URL and properties for the MySQL connection.
Reads JSON files into Spark DataFrames for tables cdw_sapp_branch, cdw_sapp_credit, and cdw_sapp_customer.
Transforms and cleans each DataFrame by modifying column data types, formatting values, and creating new columns.
Drops unnecessary columns from the DataFrames.
Writes the transformed DataFrames back to the MySQL database tables using the specified URL and properties.

### 2)transactions.py
This code defines a program that connects to a MySQL database, provides a menu to query and display transaction details, and allows the user to interact with the database.

### 3)customer.py
This code sets up a connection pool for MySQL, defines functions to interact with a database, and provides a menu-driven interface to check and modify customer account details, generate monthly bills, and display transactions. 

### 4)graph1.py
This code connects to a MySQL database, executes SQL queries to retrieve and analyze data, and creates various plots to visualize the results. It includes the following analyses:
Finding and plotting the transaction type with the highest transaction count.
Finding and plotting the state with the highest number of customers.
Finding and plotting the sum of all transactions for the top 10 customers and identifying the customer with the highest transaction amount.

### 5)loan.py
The code reads database configuration from a config.ini file using the configparser library.
It sets the path to the MySQL JDBC driver JAR file and creates a SparkSession configured with the MySQL JDBC driver.
Database connection parameters like the URL, user, and password are defined based on the configuration file.
The code sends an HTTP GET request to an API endpoint and stores the API response in the response variable.
It prints the HTTP status code of the API response to the console.
The code converts the JSON response from the API into a Spark DataFrame, which is then displayed with the show() method.
Finally, the DataFrame is written to a MySQL table named "CDW_SAPP_LOAN_APPLICATION" using the specified database connection parameters and JDBC driver.
( This code essentially retrieves data from an API, converts it to a DataFrame, and then stores it in a MySQL database table.)

### 6)graph2.py
Database configuration is read from a config.ini file using the configparser library.
A connection is established to the MySQL database specified in the configuration.
Data is loaded from the "CDW_SAPP_LOAN_APPLICATION" table in the database into a Pandas DataFrame named loan_df.
Four different figures are created and displayed using Matplotlib:
Figure 1: A pie chart that shows the approval rate for self-employed applicants.
Figure 2: A pie chart that shows the rejection rate for married male applicants.
Figure 3: A bar chart that displays the top three months with the largest transaction data.
Figure 4: A bar chart that displays the branches with the highest healthcare transaction values.
The code closes the database connection and releases the resources.
(The code is essentially performing data analysis and visualization based on the data from the MySQL database.)

## Resolution of Technical Challenges:

- During the implementation of the 'transactions.py' logic, I encountered some unexpected system-related warnings and errors when using Spark. To address this issue, I successfully resolved it by switching to 'mysql.connector' for handling the data.
- Another challenge emerged when implementing an update query in 'customer.py' using Spark. I came to realize that Spark doesn't inherently support update queries, so I opted to employ 'mysql.connector' to overcome this complexity. This approach provided a straightforward solution to the problem.
- Initially, I underestimated the complexity of working with Matplotlib. Upon realizing that Matplotlib could become somewhat complex, especially when it comes to customizing aspects like coloring bars differently, assigning distinct figure names, and more, I focused more intently on mastering these finer details.
- An issue arose when attempting to read a JSON file from a GET response. To address this problem, I learned that utilizing 'spark.sparkContext.parallelize()' was necessary to effectively convert the file, ultimately resolving the issue.
  
## References

- API: https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json
- Pyspark: https://spark.apache.org/docs/latest/api/python/index.html
