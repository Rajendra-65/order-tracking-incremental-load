# Databricks notebook source
files = dbutils.fs.ls('/Data Engineering/input_data')
for file in files:
    print(file.path)

# COMMAND ----------

df = spark.read.csv('dbfs:/Data Engineering/input_data/sample.csv',header=True,inferSchema=True)
display(df)

# COMMAND ----------

df.write.format("delta").save('dbfs:/Data Engineering/output_data/sample.csv')

# COMMAND ----------

df.write.format("delta").saveAsTable('databricks_practice1.default.sample_data')

# COMMAND ----------

tbl_df = spark.read.format("delta").table("databricks_practice1.default.sample_data")
display(tbl_df)
