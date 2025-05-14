# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df=spark.read.format('csv').option('interSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL Schema

# COMMAND ----------


my_ddl_schema = '''
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING, 
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING, 
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE 

                ''' 

# COMMAND ----------

df = spark.read.format('csv')\
            .schema(my_ddl_schema)\
            .option('header',True)\
            .load('/FileStore/tables/BigMart_Sales.csv') 

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StruckType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_strct_schema = StructType([
                                StructField('Item_Identifier',StringType(),True), 
                                StructField('Item_Weight',StringType(),True), 
                                StructField('Item_Fat_Content',StringType(),True),
                                StructField('Item_Visibility',StringType(),True), 
                                StructField('Item_Type',StringType(),True),
                                StructField('Item_MRP',StringType(),True), 
                                StructField('Outlet_Identifier',StringType(),True), 
                                StructField('Outlet_Establishment_Year',StringType(),True), 
                                StructField('Outlet_Size',StringType(),True), 
                                StructField('Outlet_Location_Type',StringType(),True), 
                                StructField('Outlet_Type',StringType(),True), 
                                StructField('Item_Outlet_Sales',StringType(),True)

])

df = spark.read.format('csv').schema(my_strct_schema).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select

# COMMAND ----------

df.select(['Item_Weight','Item_Fat_Content']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Another Way

# COMMAND ----------

df.select([col('Item_Weight'),col('Item_Fat_Content')]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alias

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 1

# COMMAND ----------

df.filter(df['Item_Fat_Content']=='Regular').select(['Item_Identifier','Item_Weight','Item_Fat_Content','Item_MRP']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario -2

# COMMAND ----------

df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Weight') < 10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming a Columns

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding New Column , lit() Create a Column Of Constant Values

# COMMAND ----------

df.withColumn('Flag',lit('New')).display()

# COMMAND ----------

df.withColumn('Multiple',round(col('Item_Weight')*col('Item_MRP'),2)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cast  -- .cast(StringType())  & For other IntegerType(),DoubleType(),FloatType(),BooleanType()

# COMMAND ----------

df.withColumn('Item_Weight',col('Item_Weight').cast(StringType())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### OrderBy()

# COMMAND ----------

df.orderBy(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending=[0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

df.drop('Item_Visibility','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union And Union By Name

# COMMAND ----------

data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Nulls

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### GroupBy

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOINS

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner Join

# COMMAND ----------

df1.join(df2,'dept_id','inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Join

# COMMAND ----------

df1.join(df2,'dept_id','left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right Join

# COMMAND ----------

df1.join(df2,'dept_id','right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti

# COMMAND ----------

df1.join(df2,'dept_id','anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WINDOW FUNCTIONS

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Number

# COMMAND ----------

df.withColumn('Row_Rank',row_number().over(Window.orderBy('Item_Identifier'))).select(['Item_Identifier','Row_Rank']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dense Rank

# COMMAND ----------

df.withColumn('Dense_Rank',dense_rank().over(Window.orderBy('Item_Identifier'))).select(['Item_Identifier','Dense_Rank']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rank

# COMMAND ----------

df.withColumn('Rank',rank().over(Window.orderBy('Item_Identifier'))).select(['Item_Identifier','Rank']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPARK SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### createTempView

# COMMAND ----------

df.createTempView('mart')

# COMMAND ----------

df_sql = spark.sql("select * from mart where Item_Fat_Content = 'Low Fat'")

# COMMAND ----------

df_sql.display()
