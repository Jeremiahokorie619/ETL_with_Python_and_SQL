# ETL_with_Python_and_SQL
This project demonstrates Extraction, Transformation and Loading of data using python and SQL connections

### INTRODUCTION
The main aim of this project is to demonstrate proficiency with Python and SQL in data preparation. The chosen dataset("orders.csv") will be downloaded from the Kaggle library and then read in using simple pandas commands.

### PROJECT SCOPE
1. Extracting data from the kaggle library
2. Performing Data Preparation and Transformation on the Extracted data
3. Loading the transformed data directly from python into Microsoft SQL Server(MSSQL) and running specific queries on the data

### TECHNOLOGIES USED
1. PYTHON(JUPYTER NOTEBOOKS)
2. MICROSOFT SQL SERVER

### WORK FLOW

#### STEP 1: Set up the workspace and import important libraries for data extraction

```python
# import libraries
!pip install kaggle
import kaggle
# Kaggle library contains many useful datasets for practice

!kaggle datasets download ankitbansal06/retail-orders -f orders.csv # Downloads the specified dataset from kaggle library

#extract file from zipfile
import zipfile
zip_ref = zipfile.ZipFile('orders.csv.zip')
zip_ref.extractall() # extracts the file from the zip file
zip_ref.close() # closes the file

# read in the orders.csv file
import pandas as pd
df = pd.read_csv('orders.csv')
df #displays the dataframe
```
![image](https://github.com/user-attachments/assets/095d73bb-65fd-4d89-ab3b-37a7738d1df2)
*9994 rows × 16 columns*

#### STEP 2: Exploratory Data analysis and Data Preparation

```python
df['Ship Mode'].unique() # returns a unique list of all ship modes in this dataset
# The ship modes "Not Available" and "unknown" should be treated as null
```
![image](https://github.com/user-attachments/assets/22f6450c-1a0e-45aa-8d99-d0a168ff2485)

```python
#now we need to replace the Not Available and unknown as null and read in the file again
df2 = pd.read_csv('orders.csv', na_values = ['Not Available','unknown'])
df2['Ship Mode'].unique() # Not Available and unknown values have been replaces with null
```
![image](https://github.com/user-attachments/assets/8ee59042-8642-4895-bae8-6a7f241e3884)

```python
# rename column names and make them lower case and have underscore instead of space between words(standard practice)
df2.columns = df2.columns.str.lower() # converts column names to lower case
df2.columns # Displays the new column names all in lower case
```
![image](https://github.com/user-attachments/assets/23ebb5b8-7b05-4e46-b426-323a87ef1887)

```python
df2.columns= df2.columns.str.replace(' ','_') # replaces the column names to have underscore as delimiter instead of space
df2.columns # Displays the new list of column names now having '_' instead of space as a delimiter
```
![image](https://github.com/user-attachments/assets/1c5fcc00-0086-48f4-b0a2-0b1660fbd7ab)

```python
# derive new columns for discount, sales_price and profit
df2['discount'] = df2['list_price']*df2['discount_percent']*0.01 # creates the column for discount
df2['sale_price'] = df2['list_price'] - df2['discount'] # creates the column for sale_price
df2['profit'] = df2['sale_price'] - df2['cost_price'] # creates the column for profit
df2 # Displays the dataframe with all the new columns
```
![image](https://github.com/user-attachments/assets/07faf9af-dd01-4396-ad1d-4c56337dccc7)

```python
# convert order date from object data type to datetime
# df2.dtypes # shows that order_date is in object data type
df2['order_date'] = pd.to_datetime(df2['order_date'], format = '%Y-%m-%d') #converts the order_date to datetime datatype
df2.dtypes # Displays the data types of all the columns
# We can now see that the data type for order_date has changed to datetime- this will allow us run time series analysis on the dataset later
```
![image](https://github.com/user-attachments/assets/5e63d556-4aa9-4f47-b264-569e89e77fe9)

```python
# drop irrelevant columns (cost_price, list_price and discount_percent columns)
df2.drop(columns = ['cost_price','list_price','discount_percent'], inplace = True) # Drops the specified columns from the dataframe
```

```python
# load the data into sql server using "append" option
import sqlalchemy as sal # this import allows us to load data from python seamlessly into sql server
engine = sal.create_engine('mssql://DESKTOP-EONGM8U\SQLEXPRESS01/SQLTutorial?driver=ODBC+DRIVER+17+FOR+SQL+SERVER') # server name in brackets
conn=engine.connect()
```
![image](https://github.com/user-attachments/assets/fd67c0e9-17c8-48a3-917a-28bc2c84c7ae)

```python
df2.to_sql('df_orders', con=conn, index=False, if_exists = 'append') # appends this data to a prepared table in MSSQL
```

#### STEP 3: Write MSSQL queries to solve the problems in the problem statement below

##### PROBLEM STATEMENT
The following information is to be extracted from the data:
1. Top 10 highest revenue generating products
2. Top 5 highest selling products in each region
3. Month over Month comparison in terms of sales. eg jan 2022 vs jan 2023
4. Month with the highest sales for each product category
5. Which subcategory had the highest growth by profit in 2023 compared to 2022?

These insights will help the management to understand how well their business is performing

##### CODES AND RESULTS
-- **Top 10 highest revenue generating products**

```SQL
select top 10 product_id, sum(sale_price) as total_sales
from df_orders
group by product_id
order by total_sales desc
```
![image](https://github.com/user-attachments/assets/b9aa99e6-177a-42ef-ad18-9cd1c9b7efb8)

-- **Top 5 highest selling products in each region**
```SQL
with cte as (
select  region,product_id,sum(sale_price) as total_sales
from df_orders
group by region, product_id)
select * from(
select *, 
ROW_NUMBER() over(partition by region order by total_sales desc) as row_num
from cte) A
where row_num <= 5
```
![image](https://github.com/user-attachments/assets/73163f35-e2d5-43b5-b2b8-2852a9b3ecbc)
![image](https://github.com/user-attachments/assets/c2c94496-2246-4683-8ffa-bc2c9f4b1e9d)

-- **Month over Month comparison in terms of sales. eg jan 2022 vs jan 2023**
```SQL
with cte as (
select substring(datename(month, order_date),1,3) AS order_month_name, month(order_date) as 
order_month_number , year(order_date) as order_year, sum(sale_price) as sales
from df_orders
group by substring(datename(month, order_date),1,3), month(order_date), year(order_date)
)
select order_month_name, order_month_number, 
sum(case when order_year = 2022 then sales else 0 end) as sales_2022,
sum(case when order_year = 2023 then sales else 0 end) as sales_2023
from cte
group by order_month_name, order_month_number
order by order_month_number
```
![image](https://github.com/user-attachments/assets/be2bfa61-3e82-4b26-84bf-81e17cdcb518)
![image](https://github.com/user-attachments/assets/000bb132-33e2-47b4-aa0e-e8b63266dcaf)

-- **Month with the highest sales for each product category**
```SQL
select format(order_date, 'yyyy/MM')
from df_orders -- this returns the date in the specified format

with cte2 as (
select category, format(order_date, 'yyyy/MM') as order_date, sum(sale_price) as total_sales
from df_orders
group by category, format(order_date, 'yyyy/MM')
--order by category, format(order_date, 'yyyy/MM')
)
select* from 
(select *,
row_number()over(partition by category order by total_sales desc) as row_num
from cte2) as [table]
where row_num = 1
```
![image](https://github.com/user-attachments/assets/e4c6c897-022f-4a3c-a5b9-a7a9e4f60223)


-- **Which subcategory had the highest growth by profit in 2023 compared to 2022?**
```SQL
select distinct(sub_category) # returns the distinct sub_categories in the data
from df_orders

with cte as(
select sub_category, year(order_date) as order_year, sum(profit) as total_profit
from df_orders
group by sub_category,year(order_date)
), 
cte2 as(
select sub_category,
sum(case when order_year = 2022 then total_profit else 0 end) as profit_2022,
sum(case when order_year = 2023 then total_profit else 0 end) as profit_2023
from cte
group by sub_category
),
cte3 as(
select sub_category,profit_2022,profit_2023,
(((profit_2023-profit_2022)/profit_2022)*100) as growth_percentage
from cte2
)
select top 1 *
from cte3
order by growth_percentage desc
```
![image](https://github.com/user-attachments/assets/911df3c9-3839-4999-a333-942dce5113b5)

#### KEY INSIGHTS
1. The highest revenue generating product is TEC-CO-10004722 and it generated a sum of $59,514.00
2. The best performing sub_category in terms of growth percentage is "Machines" with a value of 50.189142%

### CONCLUSION
The project has successfully demonstrated how to carry out ETL and data analysis using Python and SQL combination
Cheers!!
