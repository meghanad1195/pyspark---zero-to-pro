# Databricks notebook source
# MAGIC %md
# MAGIC ### DATA READING

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/default/my_files/BigMart Sales.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA READING IN CSV FORMAT
# MAGIC

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("inferschema", True)
    .option("header", True)
    .load("/Volumes/workspace/default/my_files/BigMart Sales.csv")
)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA READING IN JSON FORMAT

# COMMAND ----------

df_json = (
    spark.read.format("json")
    .option("inferschema", True)
    .option("header", True)
    .option("multiline", False)
    .load("/Volumes/workspace/default/my_files/drivers.json")
)
df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA READING IN PARQUET FORMAT

# COMMAND ----------

df_parquet = spark.read.format("parquet").load(
    "/Volumes/workspace/default/my_files/mtcars.parquet"
)

df_parquet.display()

# COMMAND ----------

df_parquet1 = spark.read.format("parquet").load(
    "/Volumes/workspace/default/my_files/titanic.parquet"
)

df_parquet1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA READING IN AVRO FORMAT

# COMMAND ----------

df_avro = spark.read.format("avro").load(
    "/Volumes/workspace/default/my_files/userdata5.avro"
)

df_avro.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading in delta format

# COMMAND ----------

df_parquet.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/my_files/titanic_delta")

df_delta = spark.read.format("delta").load("/Volumes/workspace/default/my_files/titanic_delta")
display(df_delta)



# COMMAND ----------

# MAGIC %md
# MAGIC ###SCHEMA DEFINATION

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DDL SCHEMA

# COMMAND ----------

my_ddl_Schema = """
Item_Identifier string,
Item_Weight String,
Item_Fat_Content string,
Item_Visibility double,
Item_Type string,
Item_MRP double,
Outlet_Identifier string,
Outlet_Establishment_Year integer,
Outlet_Size string,
Outlet_Location_Type string,
Outlet_Type string,
Item_Outlet_Sales double
"""

# COMMAND ----------

df = (
    spark.read.format("csv")
    .schema(my_ddl_Schema)
    .option("header", True)
    .load("/Volumes/workspace/default/my_files/BigMart Sales.csv")
)
df.printSchema()
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###STRUCT_TYPE SCHEMA

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema = StructType(
    [
        StructField("Item_Identifier", StringType(), True),
        StructField("Item_Weight", StringType(), True),
        StructField("Item_Fat_Content", StringType(), True),
        StructField("Item_Visibility", StringType(), True),
        StructField("Item_Type", StringType(), True),
        StructField("Item_MRP", StringType(), True),
        StructField("Outlet_Identifier", StringType(), True),
        StructField("Outlet_Establishment_Year", StringType(), True),
        StructField("Outlet_Size", StringType(), True),
        StructField("Outlet_Location_Type", StringType(), True),
        StructField("Outlet_Type", StringType(), True),
        StructField("Item_Outlet_Sales", StringType(), True),
    ]
)

# COMMAND ----------

df = (
    spark.read.format("csv")
    .schema(my_struct_schema)
    .option("header", True)
    .load("/Volumes/workspace/default/my_files/BigMart Sales.csv")
)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SELECT TRANSFORMATION

# COMMAND ----------

df.display()

# COMMAND ----------

df_cel = df.select("Item_Identifier", "Item_Weight", "Item_Fat_Content")

df_cel.display()

# COMMAND ----------

df_cel = df.select(
    col("Item_Identifier"), col("Item_Weight"), col("Item_Fat_Content")
)
display(df_cel)

# COMMAND ----------

# MAGIC %md
# MAGIC ###ALIAS TRANSFORMATION

# COMMAND ----------

df.select(col("Item_Identifier").alias("Item_ID")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###FILTER/WHERE TRANSFORMATION
# MAGIC #####1. Filter the data with fat content = regular

# COMMAND ----------

df.filter(col("Item_Fat_Content") == "Regular").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Slice the data with item type = soft drinks and weight < 10

# COMMAND ----------

df.filter(
    (col("Item_Type") == "Soft Drinks") & (col("Item_Weight").cast("double") < 10)
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Fetch the data with tier in (Tier1 or Tier2) and outlet size is Null

# COMMAND ----------

df.filter(
    (col("Outlet_Size").isNull())
    & (col("Outlet_Location_Type").isin("Tier 1", "Tier 2"))
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###WithColumnRenamed Transformation

# COMMAND ----------

df.withColumnRenamed("Item_Weight", "item_wt").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###withColumn
# MAGIC #####1. creating a new column

# COMMAND ----------

df = df.withColumn("flag", lit("new"))
display(df)

# COMMAND ----------

df.withColumn("multiply", col("Item_Weight") * col("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. modify the existing column

# COMMAND ----------

df.withColumn(
    "Item_Fat_Content", regexp_replace(col("Item_Fat_Content"), "Low Fat", "LF")
).withColumn(
    "Item_Fat_Content", regexp_replace(col("Item_Fat_Content"), "Regular", "reg")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Type Casting

# COMMAND ----------

df.withColumn("Item_Weight", col("Item_Weight").cast("string")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SORT/ORDER BY

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. sorting in Descending order where the first weight will be the heighest

# COMMAND ----------

df.sort(col("Item_Weight").desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. sorting in ascending order based on particular column

# COMMAND ----------

df.sort(col("Item_Visibility").asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. sorting based on multiple columns

# COMMAND ----------

df.sort(["Item_Weight", "Item_Visibility"], ascending=[0, 0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. perform sorting in descending order in one column and ascending order in another column

# COMMAND ----------

df.sort(["Item_Weight", "Item_Visibility"], ascending=[0, 1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###LIMIT

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####DROP

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario - 1

# COMMAND ----------

df.drop("Item_Visibility").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario - 2

# COMMAND ----------

df.drop("Item_Visibility", "Item_Type").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DROP_DUPLICATES

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario - 1

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####scenario - 2 

# COMMAND ----------

df.drop_duplicates(subset=["Item_Type"]).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###UNION and UNION BY NAME

# COMMAND ----------

# MAGIC %md
# MAGIC #####Preparing Dataframes

# COMMAND ----------

data1 = [("1", "kad"), ("2", "sid")]
schema1 = "id STRING, name STRING"

df1 = spark.createDataFrame(data1, schema1)

data2 = [("3", "rahul"), ("4", "jas")]
schema2 = "id STRING, name STRING"

df2 = spark.createDataFrame(data2, schema2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###UNION

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1 = [
    (
        "kad",
        "1",
    ),
    (
        "sid",
        "2",
    ),
]
schema1 = "name STRING, id STRING"

df1 = spark.createDataFrame(data1, schema1)

df1.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###UNION BY NAME

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###String Functions

# COMMAND ----------

# MAGIC %md
# MAGIC #####Initcap()

# COMMAND ----------

df.select(initcap("Item_Type")).display()

# COMMAND ----------

df.select(lower("Item_Type")).display()

# COMMAND ----------

df.select(upper("Item_Type")).display()

# COMMAND ----------

df.select(upper("Item_Type").alias("upper_item_type")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Date Functions

# COMMAND ----------

# MAGIC %md
# MAGIC #####current_date

# COMMAND ----------

df = df.withColumn(
    "curr_date", current_date()
)  #####is used to create a column in the dataframe
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####date_add()

# COMMAND ----------

df = df.withColumn("week_after", date_add("curr_date", 7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####date_sub()

# COMMAND ----------

df = df.withColumn("week_before", date_sub("curr_date", 7))
df.display()

# COMMAND ----------

df = df.withColumn("week_before", date_add("curr_date", -7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####DateDIFF

# COMMAND ----------

df = df.withColumn("date_diff", datediff("curr_date", "week_before"))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Date_Format()

# COMMAND ----------

df = df.withColumn("week_before", date_format("week_before", "dd-MM-yyyy"))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handling Nulls

# COMMAND ----------

# MAGIC %md
# MAGIC #####Dropping Nulls

# COMMAND ----------

df.dropna("all").display()

# COMMAND ----------

df.dropna("any").display()

# COMMAND ----------

df.dropna(subset=["Outlet_Size"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Filling Nulls

# COMMAND ----------

df.fillna("NotAvailable").display()

# COMMAND ----------

df.fillna("NotAvailable", subset=["Outlet_Size"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SPLIT and Indexing

# COMMAND ----------

# MAGIC %md
# MAGIC #####SPLIT

# COMMAND ----------

df.withColumn("Outlet_Type", split("Outlet_Type", " ")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####indexing

# COMMAND ----------

df.withColumn("Outlet_Type", split("Outlet_Type", " ")[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####explode

# COMMAND ----------

df_exp = df.withColumn("Outlet_Type", split("Outlet_Type", " "))
df_exp.display()

# COMMAND ----------

df_exp.withColumn("Outlet_Type", explode("Outlet_Type")).display()

# COMMAND ----------

df_exp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###ARRAY_CONTAINS

# COMMAND ----------

df_exp.withColumn("Type1_flag", array_contains("Outlet_Type", "Type1")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###GROUP BY

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario - 1

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy("Item_Type").agg(sum("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario - 2

# COMMAND ----------

df.groupBy("Item_Type").agg(avg("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario - 3

# COMMAND ----------

df.groupBy("Item_Type", "Outlet_Size").agg(sum("Item_MRP").alias("Total_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario - 4

# COMMAND ----------

df.groupBy("Item_Type", "Outlet_Size").agg(sum("Item_MRP"), avg("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Collect_list

# COMMAND ----------

data = [
    ("user1", "book1"),
    ("user1", "book2"),
    ("user2", "book2"),
    ("user2", "book4"),
    ("user3", "book1"),
]

schema = "user string, book string"

df_book = spark.createDataFrame(data, schema)

df_book.display()

# COMMAND ----------

df_book.groupBy("user").agg(collect_list("book")).display()

# COMMAND ----------

df.select("Item_Type", "Outlet_Size", "Item_MRP").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###PIVOT

# COMMAND ----------

df.groupBy("Item_Type").pivot("Outlet_Size").agg(avg("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###When_otherwise

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario - 1

# COMMAND ----------

df = df.withColumn(
    "veg_exp_flag", when(col("Item_Type") == "Meat", "Non_veg").otherwise("veg")
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario - 2

# COMMAND ----------

df = df.withColumn(
    "veg_flag", when(col("Item_Type") == "Meat", "Non-Veg").otherwise("Veg")
)

# COMMAND ----------

df.withColumn(
    "veg_exp_flag",
    when(((col("veg_flag") == "Veg") & (col("Item_MRP") < 100)), "Veg_Inexpensive")
    .when((col("veg_flag") == "Veg") & (col("Item_MRP") > 100), "Veg_Expensive")
    .otherwise("Non_Veg"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###JOINS

# COMMAND ----------

dataj1 = [
    ("1", "gaur", "d01"),
    ("2", "kit", "d02"),
    ("3", "sam", "d03"),
    ("4", "tim", "d03"),
    ("5", "aman", "d05"),
    ("6", "nad", "d06"),
]

schemaj1 = "emp_id STRING, emp_name STRING, dept_id STRING"

df1 = spark.createDataFrame(dataj1, schemaj1)

dataj2 = [
    ("d01", "HR"),
    ("d02", "Marketing"),
    ("d03", "Accounts"),
    ("d04", "IT"),
    ("d05", "Finance"),
]

schemaj2 = "dept_id STRING, department STRING"

df2 = spark.createDataFrame(dataj2, schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Inner Join

# COMMAND ----------

df1.join(df2, df1["dept_id"] == df2["dept_id"], "inner").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Left Join

# COMMAND ----------

df1.join(df2, df1["dept_id"] == df2["dept_id"], "left").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Right Join

# COMMAND ----------

df1.join(df2, df1["dept_id"] == df2["dept_id"], "right").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Anti Join

# COMMAND ----------

df1.join(df2, df1["dept_id"] == df2["dept_id"], "anti").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Window Functions

# COMMAND ----------

# MAGIC %md
# MAGIC #####ROW_NUMBER()

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, col

# COMMAND ----------

df.withColumn("rowcol", row_number().over(Window.orderBy("Item_Identifier"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####RANK VS DENSE_RANK

# COMMAND ----------

df.withColumn(
    "rank", rank().over(Window.orderBy(col("Item_Identifier").desc()))
).withColumn(
    "denseRank", dense_rank().over(Window.orderBy(col("Item_Identifier").desc()))
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Cumilative Sum

# COMMAND ----------

df.withColumn('cum_sum', sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

df.withColumn('cum_sum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.currentRow))).display()


# COMMAND ----------

df.withColumn('total_sum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###User Defined Functions

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step - 1

# COMMAND ----------

def my_func(x):
    return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step - 2

# COMMAND ----------

my_UDF = udf(my_func)

# COMMAND ----------


df.withColumn('mynewcol',my_UDF('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATA WRITING

# COMMAND ----------

# MAGIC %md
# MAGIC #####CSV

# COMMAND ----------

df.write.format('csv').save('/Volumes/workspace/default/my_files/data.csv').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### JSON

# COMMAND ----------

df.write.format("json") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/my_files/data2_json")
df_json_new = (
    spark.read.format("json")
    .load("/Volumes/workspace/default/my_files/data2_json")
)

df_json_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Delta

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/my_files/data1_delta")
df_delta = (
    spark.read.format("delta")
    .load("/Volumes/workspace/default/my_files/data1_delta")
)

df_delta.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####APPEND

# COMMAND ----------

df.write.format('csv')\
    .mode('append')\
        .save('/Volumes/workspace/default/my_files/data.csv')

# COMMAND ----------

df.write.format('csv')\
    .mode('append')\
    .option('path', '/Volumes/workspace/default/my_files/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #####OVERWRITE

# COMMAND ----------

df.write.format('csv')\
    .mode('overwrite')\
    .option('path', '/Volumes/workspace/default/my_files/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #####ERROR

# COMMAND ----------

df.write.format('csv')\
    .mode('error')\
    .option('path', '/Volumes/workspace/default/my_files/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #####IGNORE

# COMMAND ----------

df.write.format('csv')\
    .mode('ignore')\
    .option('path', '/Volumes/workspace/default/my_files/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #####PARQUET

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
    .option('path', '/Volumes/workspace/default/my_files/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #####TABLE

# COMMAND ----------

df.write.format('parquet')\
.mode('overwrite')\
.saveAsTable('my_table')

# COMMAND ----------

# MAGIC %md
# MAGIC ###SPARK SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #####CreateTempView

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_Fat_Content = 'Lf'

# COMMAND ----------

df_sql = spark.sql("select * from my_view where Item_Fat_Content = 'Lf'")

# COMMAND ----------

df_sql.display()