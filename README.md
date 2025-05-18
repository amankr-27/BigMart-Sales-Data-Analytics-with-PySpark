# 🧪 PySpark Practice on BigMart Sales Data
This project is a comprehensive hands-on practice notebook using PySpark on the BigMart Sales dataset, focused on learning and demonstrating essential data manipulation, transformation, and analysis techniques in a distributed environment.

# 📁 File: Pyspark_Practise.py
The script (exported from a Databricks notebook) includes step-by-step exercises and practical code examples covering:

# 🔹 Data Ingestion
Reading CSV data using Spark's read API

Multiple schema definitions:

Inferred Schema

DDL-style Schema

StructType schema

# 🔹 Data Exploration
show(), display(), and printSchema() for schema and data introspection

# 🔹 Transformations
Column Selection & Aliasing

Filtering Data

Renaming Columns

Creating New Columns using withColumn() and lit()

Casting Columns to different types

Sorting & Ordering

Limiting Results

Dropping Columns

Dropping Duplicates and Distinct Rows

# 🔹 Unions
Demonstrates union() and unionByName() on compatible DataFrames

# 🔹 Handling Nulls
Using dropna() with different strategies

Subset-based null filtering

# 🔹 GroupBy and Aggregations
Grouping by single and multiple columns

Aggregation functions: sum(), avg(), etc.

# 🔹 Joins
Inner Join

Left Join

Right Join

Using sample employee-department datasets for join demonstrations.

# 🔹 Window Functions
row_number()

rank()

dense_rank()

Applied using Window.orderBy() to demonstrate row-level analytical operations.

# 🔹 Spark SQL
Creating temporary views

Running SQL queries using spark.sql()

# Flow Diagram outline for your BigMart Sales Data PySpark
+------------------+
|  Data Ingestion   | 
|  - Read CSV      |
|  - Define Schema |
+--------+---------+
         |
         v
+------------------+
|  Data Exploration |
|  - show()        |
|  - printSchema() |
+--------+---------+
         |
         v
+-------------------------+
|  Data Transformations    |
|  - Filter, Rename        |
|  - New Columns, Cast     |
|  - Sort, Drop Duplicates |
+--------+----------------+
         |
         v
+-------------------+
|  Data Combination |
|  - Union, UnionByName |
+--------+----------+
         |
         v
+----------------------+
| Handling Missing Data |
|  - dropna()          |
|  - Null Filtering    |
+--------+-------------+
         |
         v
+------------------------+
| Grouping & Aggregations |
|  - groupBy(), sum(), avg() |
+--------+----------------+
         |
         v
+-------------------+
|       Joins       |
|  - Inner, Left, Right |
+--------+----------+
         |
         v
+------------------+
|  Window Functions |
|  - row_number()   |
|  - rank(), dense_rank() |
+--------+---------+
         |
         v
+-------------------+
|    Spark SQL      |
|  - Temp Views     |
|  - SQL Queries    |
+-------------------+

