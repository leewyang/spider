import os
import pandas as pd
import sqlite3
from datasets import load_dataset
from datetime import datetime
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("spider-datagen-jdbc")
    .getOrCreate()
)

database = '/home/leey/devpub/spider/spider/database/college_1/college_1.sqlite'

con = sqlite3.connect(database)
cur = con.cursor()
res = cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = res.fetchall()
tables = [x[0] for x in tables]
print(tables)

# Load each table into spark
for table in tables:
    print(table)
    df = spark.read.format('jdbc') \
        .options(driver='org.sqlite.JDBC', dbtable=table, \
                 url='jdbc:sqlite:/home/leey/devpub/spider/spider/database/college_1/college_1.sqlite') \
        .option("customSchema", "STU_DOB STRING, EMP_HIREDATE STRING, EMP_DOB STRING") \
        .load()
    df.show()
    df.createOrReplaceTempView(table)
# con.close()

# Load queries
dataset_name = "xlangai/spider"
dataset = load_dataset(dataset_name, split="train")
df = pd.DataFrame(dataset)
queries = list(df['query'].loc[df['db_id'] == 'college_1'])

# Merge synthetic w/ real
for table in tables:
    df = spark.table(table)
    synthetic_df = spark.read.parquet(f"{table}.parquet")
    merged_df = synthetic_df.union(df)
    # merged_df.cache()
    merged_df.createOrReplaceTempView(table)
    # merged_df.write.mode("overwrite").parquet(f"syn_{table}.parquet")
    print(merged_df.count())

#slow_queries = [34, 35, 78, 79, 120, 121, 122, 127, 140, 141, 142, 143, 144, 145, 148, 149, 150, 151]
slow_queries = [34, 78, 120, 140]
for i, q in enumerate(queries):
#    if i not in slow_queries:
#        continue
    try:
        print(f"{datetime.utcnow()}: {i}: {q}")
        res = spark.sql(q)
        res.show()
    except Exception as e:
        print(e)
