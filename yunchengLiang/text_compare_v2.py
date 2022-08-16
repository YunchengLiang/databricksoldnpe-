# Databricks notebook source
import pandas as pd
import numpy as np
spark = spark
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
import re
import string

import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from datetime import datetime
from pyspark.ml import Pipeline

# COMMAND ----------

sparktext=spark.sql('select * from default.spark_text_v5')
spacytext=spark.sql('select * from default.spacy_text_v2')
sparktext=sparktext.withColumnRenamed('clean_text','clean_text_spark').withColumnRenamed('session_id','session_id_spark')
spacytext=spacytext.withColumnRenamed('clean_text','clean_text_spacy').withColumnRenamed('session_id','session_id_spacy')

# COMMAND ----------

text_compare=sparktext.join(spacytext, sparktext.session_id_spark==spacytext.session_id_spacy, 'outer')

# COMMAND ----------

display(text_compare)

# COMMAND ----------

def tokenize(inputcol,df):
  inputcol_token=inputcol+'_token'
  documentAssembler = DocumentAssembler() \
      .setInputCol(inputcol) \
      .setOutputCol("document")

  tokenizer = Tokenizer() \
      .setInputCols(["document"]) \
      .setOutputCol("token")

  finisher = Finisher() \
      .setInputCols("token") \
      .setOutputCols(inputcol_token) \
      .setOutputAsArray(True)\
      .setCleanAnnotations(True)

  nlp_pipeline = Pipeline(
      stages=
      [documentAssembler, tokenizer, finisher]
  )
  text_compare_col.append(inputcol_token)
  return nlp_pipeline.fit(df).transform(df).select(text_compare_col)
text_compare_col=list(text_compare.columns)
middf=tokenize('clean_text_spark',text_compare)
finaldf=tokenize('clean_text_spacy',middf)

# COMMAND ----------

display(finaldf)

# COMMAND ----------

text_compare_col

# COMMAND ----------

def get_non_overlap(df,first_col,second_col):
    non_overlap_udf = udf(lambda a, b: list(set(a) ^ set(b)), ArrayType(StringType()))
    left_udf = udf(lambda a, b: list(set(a) - set(b)), ArrayType(StringType()))
    right_udf = udf(lambda a, b: list(set(b) - set(a)), ArrayType(StringType()))
    return df.select('session_id_spark',
     'clean_text_spark',
     'session_id_spacy',
     'clean_text_spacy',
     'clean_text_spark_token',
     'clean_text_spacy_token', 
      non_overlap_udf(first_col,second_col).alias("non_overlap"),
      left_udf(first_col,second_col).alias("spark_extra"),
      right_udf(first_col,second_col).alias("spacy_extra"))
outputdf=get_non_overlap(finaldf, 'clean_text_spark_token','clean_text_spacy_token')

# COMMAND ----------

display(outputdf)

# COMMAND ----------

outputdf.select("spark_extra").show()

# COMMAND ----------

def lenudf_filter(df,inputcol):
  df_col=df.columns
  length=udf(lambda x: len(x))
  return df.select(df_col).filter(length(col(inputcol))>0)
midoutput=lenudf_filter(outputdf,"spark_extra")
finaloutput=lenudf_filter(midoutput,"spacy_extra")
display(finaloutput)

# COMMAND ----------

outlistspark=finaloutput.select(explode(col("spark_extra")).alias("spark_extra_explode")).select(collect_list("spark_extra_explode").alias("spark_extra_all"))
display(outlistspark)

# COMMAND ----------

print(set(outlistspark.take(1)[0].spark_extra_all))

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install wordcloud

# COMMAND ----------

sparktext=outlistspark.select(array_join("spark_extra_all", ' ').alias('text'))
sparktext.take(1)[0].text

# COMMAND ----------

from wordcloud import WordCloud
import matplotlib.pyplot as plt

# COMMAND ----------

# Generate a word cloud image
wordcloud = WordCloud(
                      scale=10,
                     background_color="white",
                     random_state=1 # Make sure the output is always the same for the same input
             ).generate(sparktext.take(1)[0].text)

# Display the generated image the matplotlib way:
plt.figure(figsize=(20,20))
plt.axis("off")
plt.imshow(wordcloud, interpolation='bilinear')

# COMMAND ----------

outlistspacy=finaloutput.select(explode(col("spacy_extra")).alias("spacy_extra_explode")).select(collect_list("spacy_extra_explode").alias("spacy_extra_all"))
spacytext=outlistspacy.select(array_join("spacy_extra_all", ' ').alias('text'))
spacytext.take(1)[0].text

# COMMAND ----------

# Generate a word cloud image
wordcloud = WordCloud(
                      scale=10,
                     background_color="white",
                     random_state=1 # Make sure the output is always the same for the same input
             ).generate(spacytext.take(1)[0].text)

# Display the generated image the matplotlib way:
plt.figure(figsize=(20,20))
plt.axis("off")
plt.imshow(wordcloud, interpolation='bilinear')

# COMMAND ----------


