#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
import re
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType


# Install findspark to make available spark from jupyter-python

# In[2]:


#!conda insstall openjdk
#!conda install findspark
#install hadoop:
#https://github.com/ruslanmv/How-to-install-Hadoop-on-Windows?tab=readme-ov-file


# In[3]:


import findspark
findspark.init()
findspark.find()


# In[6]:


catalog_nm = "flights_catalog"
layer_nm = "silver" #options: "silver", "gold"
warehouse_path = f"file:/C:/ws/github/iceberg-lakehouse/warehouse_flights/{layer_nm}"
sales_table_nm = "sales"

conf = (
    pyspark.SparkConf()
    .setAppName("iceberg_flights_app")
    #packages
    .set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.2") #spark: 3.4, scala:2.12 iceberg:1.5.2
    #SQL extensions
    .set("spark.sql.extensions","org.apache.iceberg.spark.extensions:IcebergSparkSessionExtensions")
    #Eager evaluation
    .set('spark.sql.repl.eagerEval.enabled', True)
    #Configuring catalog
    .set(f"spark.sql.catalog.{catalog_nm}","org.apache.iceberg.spark.SparkCatalog")
    .set(f"spark.sql.catalog.{catalog_nm}.type","hadoop")
    .set(f"spark.sql.catalog.{catalog_nm}.warehouse",warehouse_path)
)

#Start SparkSession
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")


# In[7]:


spark


# Schema declaration

# In[8]:


staging_in_schema = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("transaction_timestamp", StringType(), nullable=False),
    StructField("price", StringType(), nullable=False),
    StructField("currency", StringType(), nullable=False),
    StructField("flight_id", StringType(), nullable=False),
    StructField("booking_reference", StringType(), nullable=True)
])


# In[9]:


staging_path = "file:/C:/ws/github/iceberg-lakehouse/staging/"
date_str = "20130701"
file_nm = f"sellings_{date_str}_MADJHT.csv"
staging_file_path = staging_path + file_nm
#staging_df = spark.read.format('csv').option("header","true").load(staging_file_path)
#staging_df = spark.read.csv(staging_file_path, header=True, nanValue="N/A",mode="FAILFAST",schema=staging_in_schema)
staging_df = (
    spark.read.format('csv')
    .option("header","true")
    .schema(staging_in_schema)
    .load(staging_file_path)
)


# In[10]:


staging_df.printSchema()


# In[11]:


staging_df.show()


# In[12]:


staging_df.count()


# Calculate columns for partitioning:year_ptt, month_ptt, year_ptt

# In[13]:


#Extract transaction year, month and day from the csv filename
print(file_nm)
#file_nm2= "selkjdkjfijelskjlkjledkjfe_201307901_eldkfkendnknd.csv"
matched = re.search("[^_]*_(\\d{4})(\\d{2})(\\d{2})_[^\.]*\.csv",file_nm)
if matched:
    year_ptt = matched.group(1)
    month_ptt = matched.group(2)
    day_ptt = matched.group(3)
    print(f"{year_ptt}-{month_ptt}-{day_ptt}")
else:
    raise ValueError("Incorrect file name")


# In[14]:


staging_df = (
    staging_df.withColumn('year_ptt',F.lit(year_ptt).cast(IntegerType()))
    .withColumn('month_ptt',F.lit(month_ptt).cast(IntegerType()))
    .withColumn('day_ptt',F.lit(day_ptt).cast(IntegerType()))
)


# In[15]:


staging_df.printSchema()


# In[16]:


staging_df.show()


# In[17]:


#staging_df = staging_df.withColumn('operation_date',F.to_date(staging_df.operation_date_str,'yyyyMMdd').alias('operation_date'))
#staging_df.show()


# Dataquality

# In[18]:


[c for c in staging_df.columns]


# In[19]:


#Search for null values
staging_df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in staging_df.columns]).show()


# In[20]:


#Search for duplicates
df_count = staging_df.count()


# In[21]:


df_distinct_count = staging_df.distinct().count()


# In[22]:


duplicates = df_count-df_distinct_count
assert duplicates == 0, f"There are {duplicates} records duplicate"


# In[23]:


#Format validation


# In[24]:


transaction_id_format_error_df = staging_df.select(F.count(F.when(~F.col('transaction_id').rlike(r'TRANS\d{6}'), True)).alias('count'))


# In[25]:


transaction_id_format_error_count = transaction_id_format_error_df.collect()[0][0]
assert transaction_id_format_error_count == 0,f"transaction_id invalid format count={transaction_id_format_error_count}"


# Creation of the final DataFrame with the correct schema

# In[26]:


staging_df_schema = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("transaction_timestamp", StringType(), nullable=False),
    StructField("price", StringType(), nullable=False),
    StructField("currency", StringType(), nullable=False),
    StructField("flight_id", StringType(), nullable=False),
    StructField("booking_reference", StringType(), nullable=True),
    StructField("year_ptt", IntegerType(), nullable=False),
    StructField("month_ptt", IntegerType(), nullable=False),
    StructField("day_ptt", IntegerType(), nullable=False)
])


# In[27]:


final_staging_df = spark.createDataFrame(staging_df.collect(),schema=staging_df_schema)


# In[28]:


final_staging_df.printSchema()


# In[29]:


final_staging_df.show()


# Write option 1: Spark DataFrame method

# In[30]:


sales_tb = f"{catalog_nm}.{sales_table_nm}"
final_staging_df.writeTo(sales_tb).overwritePartitions()
#final_staging_df.writeTo(sales_tb).append()


# In[31]:


spark.sql(
    f"""
    SELECT * 
    FROM {sales_tb}
    LIMIT 10;
    """
).show()


# In[32]:


spark.sql(
    f"""
    SELECT COUNT(*) 
    FROM {catalog_nm}.{sales_table_nm};
    """
).show()


# Write option 2: Spark SQL (UPSERT)

# In[34]:


#Create a temporary view for the dataframe
staging_df_view = "staging_df"
final_staging_df.createOrReplaceTempView(staging_df_view)


# In[37]:


#show dataframe contents using Spark SQL
spark.sql(
    f"""
    SELECT * 
    FROM {staging_df_view};
    """
).show()


# In[44]:


#Upsert
spark.sql(
    f"""
    MERGE INTO {catalog_nm}.{sales_table_nm} AS t
    USING (SELECT * FROM {staging_df_view}) AS s
    ON s.transaction_id = t.transaction_id
    WHEN MATCHED THEN
        UPDATE SET
           t.customer_id = s.customer_id,
           t.transaction_timestamp = s.transaction_timestamp,
           t.price = s.price,
           t.currency = s.currency,
           t.flight_id = s.flight_id,
           t.booking_reference = s.booking_reference,
           t.year_ptt = s.year_ptt,
           t.month_ptt = s.month_ptt,
           t.day_ptt = s.day_ptt
    WHEN NOT MATCHED BY TARGET THEN
        INSERT(transaction_id,customer_id,transaction_timestamp,
            price,currency,flight_id,booking_reference,year_ptt,month_ptt,day_ptt)
        VALUES(s.transaction_id,s.customer_id,s.transaction_timestamp,
            s.price,s.currency,s.flight_id,s.booking_reference,s.year_ptt,s.month_ptt,s.day_ptt)
    ;
    """
)


# In[ ]:




