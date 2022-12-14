# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC cancellations: 101000264, 101001648, 109000006
# MAGIC move requests: 109000011 
# MAGIC over all verint conversation

# COMMAND ----------

# MAGIC %md
# MAGIC when removing duplicates entries, if a customer has been transfered to different agent, the connection id will be the same, use (connection id, employee id) to remove duplicates, sometimes has same conversation context. If a customer is overall talking about one main topic, regardless if they are transferred, the category will be the same.    
# MAGIC     
# MAGIC 
# MAGIC Verint table:  (sumfct_conversation, session_booked)   
# MAGIC Speech_id_verint: is an internal ID that should be unique to each call     
# MAGIC for the entire call, but it's generated by the Azure system, connection and interaction are generated by verint     
# MAGIC it's used to connect verint tables within Azure     
# MAGIC interaction_id: is each segment of a call so each customer/agent interaction      
# MAGIC A segment is defined by each unique customer/agent interaction, so it gets a new id when they transfer      
# MAGIC connection_id: is for the entire call, so all the customer/agent interactions between when someone calls and hangs up     
# MAGIC so a connection id can contain many interaction ids, if the customer gets transferred a lot. but it could also have only one, if they call into a single agent and then hang up     
# MAGIC ctn: customer telephone number     
# MAGIC Customer_id: same as BAN, CAN     
# MAGIC Receiving_skill: specific plan code      
# MAGIC Record_insert_dt: refers to when it entered the verint system, different from the conversation_date     
# MAGIC  
# MAGIC 
# MAGIC Session_categories table:     
# MAGIC category_id is created by verint     
# MAGIC instance ID is a broader definition, so every call gets a instance, and then category, there can be many categories within an instance     
# MAGIC every entire call will be treated as an instance, and each call text will contain one or more categories depends on each customer     

# COMMAND ----------

# MAGIC %md
# MAGIC Verint table:?? (sumfct_conversation, session_booked)    
# MAGIC Session_categories table:

# COMMAND ----------

conv_sumfct=spark.sql("select * from verint.cbu_rog_conversation_sumfct")
session_booked=spark.sql("select * from verint.sessions_booked")
session_category=spark.sql("select * from verint.sessions_categories")

# COMMAND ----------

conv_sumfct.count()#115,622,019 too large

# COMMAND ----------

for i in ["CONNECTION_ID","CTN"]:
  print(i,' has count of ', conv_sumfct.select(i).distinct().count(),"\n")
#71,058,603 ??????????????????1.62???interaction(transfer)
#14,862,749 ??????????????????4.8?????????

# COMMAND ----------

test_col=["SPEECH_ID_VERINT","TEXT_CUSTOMER_FULL","TEXT_ALL","CUSTOMER_ID","CTN","CONNECTION_ID","INTERACTION_ID","RECEIVING_SKILL","CATEGORY_NAMES",   "CONVERSATION_DATE"]
conv_sumfct_test=conv_sumfct.select(test_col).sample(0.01)

# COMMAND ----------

conv_sumfct_test.groupby("RECEIVING_SKILL").count().show()

# COMMAND ----------

#???feature ??????????????????transfer?????????
transfer=conv_sumfct_test.groupby("CONNECTION_ID").count()
transfer=transfer.withColumnRenamed("CONNECTION_ID","CONNECTION_ID_2")
conv_sumfct_test_joined=conv_sumfct_test.join(transfer,conv_sumfct_test.CONNECTION_ID == transfer.CONNECTION_ID_2, 'inner')
conv_sumfct_test_joined=conv_sumfct_test_joined.withColumnRenamed("count","transfers_per_call").drop("CONNECTION_ID_2")
#??????CUSTOMER_ID???CTN??????TEXT_ALL??????drop?????? 
conv_sumfct_test_joined=conv_sumfct_test_joined.dropna(subset=["TEXT_ALL","CUSTOMER_ID","CTN"])

# COMMAND ----------

conv_sumfct_test_joined.show()

# COMMAND ----------

conv_sumfct_test_joined.columns

# COMMAND ----------

#???feature ??????????????????conversation recency???rank 1???2???3...
conv_sumfct_test_mid=conv_sumfct_test_joined.select(["CTN","CONNECTION_ID","CONVERSATION_DATE"]).dropDuplicates()#??????????????????????????????call,
windowSpec = Window.partitionBy("CTN").orderBy(desc("CONVERSATION_DATE"))#???????????????call???recency
conv_sumfct_test_mid=conv_sumfct_test_mid.withColumn("call_recency",row_number().over(windowSpec))\
                      .select(["CTN","CONNECTION_ID","call_recency"])
conv_sumfct_test_mid=conv_sumfct_test_mid.withColumnRenamed("CTN","CTN2")
conv_sumfct_test_mid=conv_sumfct_test_mid.withColumnRenamed("CONNECTION_ID","CONNECTION_ID_2")

# COMMAND ----------

conv_sumfct_test_mid.show()

# COMMAND ----------

conv_sumfct_test_featured=conv_sumfct_test_joined.join(conv_sumfct_test_mid,(conv_sumfct_test_joined.CTN==conv_sumfct_test_mid.CTN2)&(conv_sumfct_test_joined.CONNECTION_ID==conv_sumfct_test_mid.CONNECTION_ID_2), "inner").drop("CTN2").drop("CONNECTION_ID_2")

# COMMAND ----------

conv_sumfct_test_featured.groupby("call_recency").count().show()

# COMMAND ----------

display(conv_sumfct_test_featured)

# COMMAND ----------

display(conv_sumfct_test_joined)

# COMMAND ----------

#session catergory data only keep cancel & move, which column could be used to join this data to coversation data
session_category_test_col=["SID_KEY","CATEGORY_ID","INSTANCE_ID"]
session_category_test=session_category.select(session_category_test_col).where(col("CATEGORY_ID").isin([101000264, 101001648, 109000006,109000011])).sample(0.01)

# COMMAND ----------

display(session_category_test)

# COMMAND ----------

display(conv_sumfct)

# COMMAND ----------

display(session_category)

# COMMAND ----------

display(session_booked)

# COMMAND ----------


