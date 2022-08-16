# Databricks notebook source

with open("/dbfs/tmp/log/CD4MT/oldcluster_test_Churn_Schedule/Prepro/0725-152201-znzv0plw/init_scripts/0725-152201-znzv0plw_172_17_19_29/20220725_152212_00_init_jobcluster.sh.stderr.log", "r") as f_read:
    for line in f_read:
        print(line)

# COMMAND ----------


with open("/dbfs/tmp/log/CD4MT/oldcluster_test_Churn_Schedule/Prepro/0725-141818-wb5nsmou/driver/stdout", "r") as f_read:
    for line in f_read:
        print(line)

# COMMAND ----------

import sparknlp
from sparknlp.base import *
from pyspark.ml import Pipeline
data = spark.createDataFrame([["Spark NLP is an open-source text processing library."]]).toDF("text")
documentAssembler = DocumentAssembler().setInputCol("text").setOutputCol("document")
result = documentAssembler.transform(data)

# COMMAND ----------


