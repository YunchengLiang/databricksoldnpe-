# Databricks notebook source
import pyspark.sql.functions as f
df_sessions_booked=spark.sql("SELECT p7_value,CONCAT(unit_num, '0', RIGHT(CONCAT('00000000', channel_num),8)) as speech_id FROM verint.sessions_booked where p7_value is not null")
df_sessions_booked = df_sessions_booked.groupBy('p7_value', 'speech_id').count().select('p7_value', 'speech_id',f.col('count').alias('counts')).filter(f.col('counts')>1)
display(df_sessions_booked)# if same speech id, then same p7_value

# COMMAND ----------

import pyspark.sql.functions as f
sess = [('id1',1),
      ('id2',2),
      ('id3',2),
       ('id3',2)] # if same speech id, then same account_number
con = [('id1','A'),  
      ('id2','B'), 
      ('id3', 'C')]
hh=[(1, '10'), 
   (2, '20'),
   (2, '30')]
hh1=[(1, '10'), 
   (2, '30')]

cols1 = ["speech_id","account_number"]
cols2 = ["speech_id", "text"]
cols3=["account_number", "report_date"]
cols4=["account_number", "latest_report_date"]
dft1 = spark.createDataFrame(data=sess, schema = cols1)
dft2 = spark.createDataFrame(data=con, schema = cols2)
dft3 = spark.createDataFrame(data=hh, schema = cols3)
dft4 = spark.createDataFrame(data=hh1, schema = cols4)
# +---------+--------------+
# |speech_id|account_number|
# +---------+--------------+
# |      id1|             1|
# |      id2|             2|  ->dft1: sesion_booked, if same speech id, then must have same account_number
# |      id3|             2|
# |      id3|             2|
# +---------+--------------+

# +---------+----+
# |speech_id|text|
# +---------+----+
# |      id1|   A|  -> dft2: conversation
# |      id2|   B|
# |      id3|   C|
# +---------+----+

# +--------------+-----------+
# |account_number|report_date|
# +--------------+-----------+
# |             1|         10|
# |             2|         20|   -> dft3: household
# |             2|         30|
# +--------------+-----------+

# +--------------+------------------+
# |account_number|latest_report_date|
# +--------------+------------------+
# |             1|                10|  -> dft4: household removed duplicates account number by latest report date
# |             2|                30|
# +--------------+------------------+



merge1 = dft1.join(dft2, dft1.speech_id == dft2.speech_id, 'inner').drop(dft1.speech_id).distinct()
# merge1.show()
# +--------------+---------+----+
# |account_number|speech_id|text|
# +--------------+---------+----+
# |             1|      id1|   A|  -> merged session_booked and conversation
# |             2|      id2|   B|
# |             2|      id3|   C|
# +--------------+---------+----+

hhmerge = merge1.join(dft3,merge1.account_number == dft3.account_number, 'inner').drop(merge1.account_number).drop(merge1.speech_id)
# hhmerge.show()
hhmerge1 = merge1.join(dft4,merge1.account_number == dft4.account_number, 'inner').drop(merge1.account_number).drop(merge1.speech_id)
hhmerge1.show()

# +----+--------------+-----------+
# |text|account_number|report_date|
# +----+--------------+-----------+
# |   A|             1|         10|  -> merged household with (session_booked & conversation)
# |   C|             2|         20|  -> filter date at later step
# |   B|             2|         20|  ->after filter using latest date: A 1 10, C 2 30, B 2 30
# |   C|             2|         30|
# |   B|             2|         30|
# +----+--------------+-----------+


# +----+--------------+------------------+
# |text|account_number|latest_report_date|    
# +----+--------------+------------------+   -> filter date at hh stage
# |   A|             1|                10|   -> merged household with (session_booked & conversation)
# |   C|             2|                30|   -> date is useless, need to be dropped for further actions
# |   B|             2|                30|
# +----+--------------+------------------+


unique = hhmerge.groupby('text').agg(f.max('report_date').alias("latest_report_date"))
# unique.show()
# +----+------------------+
# |text|latest_report_date|
# +----+------------------+
# |   A|                10|  -> filtered out duplicates text by latest report date
# |   B|                30|
# |   C|                30|  -> date is useless, need to be dropped for further actions
# +----+------------------+




"""
household -> churned customers (a, b, c, d, e, ...)
join conversation : all calls from the a, b, c, d, e... (calls text: include all activities they did in rogers from beginning to the end)
"""

# COMMAND ----------

hhmerge.createOrReplaceTempView("hhmerge")
dft3.createOrReplaceTempView("dft3")
test = spark.sql("select dft3.num, dft3.date from hhmerge, dft3 where hhmerge.date == dft3.date")
dft3.show()
hhmerge.show()

# COMMAND ----------

import pyspark.sql.functions as f
df = spark.read.parquet("dbfs:/mnt/ml-etl-output-data/churn_sample_data/ibro_verint_churn_200/part-00000-tid-7891822072604037770-b36a4ca7-cd38-4740-a825-f55c5727392d-22041-1-c000.snappy.parquet")
display(df)

# COMMAND ----------

import pyspark.sql.functions as f
df = spark.read.parquet("dbfs:/mnt/ml-etl-output-data/churn_sample_data/ibro_verint_churn_200/part-00000-tid-7891822072604037770-b36a4ca7-cd38-4740-a825-f55c5727392d-22041-1-c000.snappy.parquet")
df = df.where(f.length('CLEAN_TEXT') == 0)
display(df)

# COMMAND ----------


verint_df= spark.sql("SELECT sumfct.*, booked.* FROM\
(SELECT CONCAT(unit_num, '0', channel_num) as speech_id, sid_key, (sess_duration/60) as session_duration, number_of_holds, (total_hold_time/60) as hold_time_minutes, p7_value as account_number, CAST(local_start_time as DATE) as local_start_date, local_start_time FROM VERINT.SESSIONS_BOOKED) as booked \
INNER JOIN \
(SELECT *, YEAR(conversation_date) as Year, MONTH(conversation_date) as Month FROM VERINT.CBU_ROG_CONVERSATION_SUMFCT WHERE conversation_date >= '2021-11-01' and conversation_date <= '2021-11-10') \
as sumfct ON sumfct.speech_id_verint = booked.speech_id")

display(verint_df)
##merged  session_booked & conversation table
##speech_id -> join key



# COMMAND ----------

import pyspark.sql.functions as f
sumfct_keep_cols = ['text_customer_full', 'text_agent_full', 'text_overlap', 'text_unknown', 'ctn',
                        'speech_id_verint', 'interaction_id', 'connection_id', 'receiving_skill', 
                        'conversation_date','customer_id', 'sid_key', 'account_number']

verint_df = verint_df.select(sumfct_keep_cols)
# verint_df=verint_df.groupBy('account_number').count().select('account_number',f.col('count').alias('counts')).filter(f.col('counts')>1)
#verint_df1 = verint_df.where(f.length('text_customer_full') == 0)
display(verint_df1)

## clean up df



# COMMAND ----------

household_df = spark.sql("select if(length(customer_id) = 12, customer_id, customer_account) as account_number,\
            report_date,\
            arpa_out,\
            arpa_out*-1 as deac_count,\
            arpa_out*1 as winback_count,  \
            customer_relation_key as cr_key,\
            enterprise_id,\
            customer_location_id,\
            multi_segment,\
            case \
            when activity_voluntary in ('i') then 'out_invol' else 'out_vol'end \
            as vol_invol_ind,\
            (case\
                when (contract_group_code in ('2','3') and multi_segment in ('cbu')) then 'ch bulk'\
                when (multi_system in ('source-compton-kincardine-mci')) then 'ch cks'\
                when (multi_segment in ('cbu')) then 'ch non-bulk'\
                when (multi_segment in ('ebu')) then 'r4b retail cable'\
                else 'ch bulk'\
            end)\
            as classification,\
            (case\
                when multi_brand = 'fido' then 'fido' else 'rogers' end)\
                as brand,\
            case\
                when multi_system = 'ss' then 'legacy' else 'maestro' end\
            as platform,\
            postal_code,\
            activity_grade_code\
            from APP_IBRO.IBRO_HOUSEHOLD_ACTIVITY where activity_grade_code in ('HH_CUSTOMER_DEFECTION','HH_WINBACK')\
            order by\
            report_date,\
            activity_grade_code,\
            customer_id,\
            customer_location_id")

display(household_df)

#customer id & customer account
#(churned table : only contain churned customer (no winback customer))

# COMMAND ----------

ibro_verint_df = verint_df.join(household_df, verint_df.account_number == household_df.account_number, 'inner')\
    .drop(household_df.account_number)\
    .drop(verint_df.customer_id)

display(ibro_verint_df)

# join verint & churned table  using account number (verint p7 value)

# COMMAND ----------

hh_df1 = spark.sql("select distinct(*) from default.temp_churned_df where ACTIVITY_GRADE_CODE='HH_CUSTOMER_DEFECTION' and length(CLEAN_TEXT) > 0")
display(hh_df1)

# COMMAND ----------

import pyspark.sql.functions as f
df = spark.sql("select * from default.temp_churned_df")
hh_df1 = df.where(df.ACTIVITY_GRADE_CODE=='HH_CUSTOMER_DEFECTION')
hh_df1 = hh_df1.where(f.length('CLEAN_TEXT') > 0)
display(hh_df1)

# COMMAND ----------

import pyspark.sql.functions as f
# churned_df_counts=churned_df.groupBy('clean_text').count().select('clean_text',f.col('count').alias('counts')).filter(f.col('counts')>1)
df_remove1=hh_df1.groupBy("CLEAN_TEXT").agg(f.max('REPORT_DATE').alias("LATEST_REPORT_DATE"))
display(df_remove1)

# COMMAND ----------

counts=df_remove1.groupBy('clean_text').count().select('clean_text',f.col('count').alias('counts')).filter(f.col('counts')>1)
display(counts)

# COMMAND ----------

cond = [hh_df1.REPORT_DATE == df_remove1.LATEST_REPORT_DATE, hh_df1.CLEAN_TEXT == df_remove1.CLEAN_TEXT]
result = hh_df1.join(df_remove1, cond).drop(df_remove1.CLEAN_TEXT)
display(result)

# COMMAND ----------

hh_df = spark.sql("select * from default.temp_churned_df where ACTIVITY_GRADE_CODE='HH_CUSTOMER_DEFECTION' and length(CLEAN_TEXT) > 0")
hh_df.createOrReplaceTempView("hh_df")

# COMMAND ----------

df_dup = spark.sql("select CLEAN_TEXT, count(*) as ct, max(REPORT_DATE) as m from hh_df group by CLEAN_TEXT")
display(df_dup)
df_dup.createOrReplaceTempView("df_dup")

# COMMAND ----------



# COMMAND ----------

df_remove = spark.sql("select * from hh_df inner join df_dup on hh_df.REPORT_DATE = df_dup.m and hh_df.CLEAN_TEXT = df_dup.CLEAN_TEXT").drop(df_dup.CLEAN_TEXT)
display(df_remove)
df_remove.createOrReplaceTempView("df_remove")
#df_remove.drop(hh_df.CLEAN_TEXT)
#display(df_remove)

# COMMAND ----------

# %sql
# select clean_text,count(*),report_date from 

result = spark.sql("select CLEAN_TEXT, count(*) as ct, max(REPORT_DATE) as m from df_remove group by CLEAN_TEXT having ct > 1")
display(result)
