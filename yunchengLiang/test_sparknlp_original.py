# Databricks notebook source
import sparknlp
spark = sparknlp.start(spark24=True)

# COMMAND ----------

# from rogers.cd.preprocessor import BoldChatETL
# from rogers.cd.SparkOPs import DataBricks

import logging


from rogers.cd.assets import ALL_BOLDCHAT_STOPWORDS
from rogers.cd.dataOPs import BoldChat

import logging
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from sparknlp.base import *
from sparknlp.annotator import *
from datetime import datetime
import re
import string

import spacy
import en_core_web_md

class BoldChatETL:
    """
    This class covers the required functionalities to perform
    preliminary ETL jobs on raw bold_chat and live_sas_summary
    to prepare an integrated dateset for the Normalizer Pipeline.
    """

    keep_cols = ['session_id', 'message_technician', 'message_customer',
                 'start_timestamp', 'end_timestamp', 'channel_name', 'chat_url', 'inbound_flag']

    def __init__(self, min_date, max_date):
        """
        :param boldchat_session_df: Spark DataFrame containing raw boldchat_session data for a given time period
        :param livechat_ss_df: Spark DataFrame containing raw livechat_sas_summary data for a given time period
        """
        self.min_date = min_date
        self.max_date = max_date
        self.logger = logging.getLogger(__name__)

        self.logger.info("reading boldchat data from {} forward...".format(self.min_date))
        self.boldchat_session_df , self.livechat_ss_df = BoldChat.load_data(min_date=self.min_date, max_date=self.max_date)
        self.logger.info("Data read successfully:\n boldchat_session: {} records, livechat_sas_summary: {} records" \
                    .format(self.boldchat_session_df.count(), self.livechat_ss_df.count()))

        " ******Merging boldchat_seesion and livechat_sas_summary****** "
        self.boldchat_df = self.__merge_input_data(self.boldchat_session_df, self.livechat_ss_df)
        # Keeping the required columns only
        self.boldchat_df = self.boldchat_df.select(BoldChatETL.keep_cols).dropDuplicates()
        self.logger.info("Number of merged boldchat records: {}".format(self.boldchat_df.count()))
        try:
            assert (self.boldchat_df.count() > 0)
        except AssertionError:
            self.logger.error("The joined boldchat dataframe is empty.")

        ######for quick testing the entire pipeline
        #####here we just take 200 records and try to
        #####run the whole pipeline on this data
        self.boldchat_df=self.boldchat_df.limit(200)

    def __merge_input_data(self, boldchat_session_df, livechat_ss_df):
        """
        Integrating the input bold_chat and livechat_sas_summary via join operation
        :return: Joined Spark DF
        """
        #The Join Condition
        join_cond = [livechat_ss_df.session_id == boldchat_session_df.session_id,
                     livechat_ss_df.session_start_timestamp == boldchat_session_df.start_timestamp,
                     livechat_ss_df.received_date == boldchat_session_df.received_date]

        return livechat_ss_df.join(boldchat_session_df, join_cond, 'inner') \
            .drop(boldchat_session_df.session_id) \
            .drop(boldchat_session_df.received_date)


    ############################
    #### Spark Transformers ####
    ############################

    def __filter_by_channel(self, channel_name):
        """
            Transformer function for filtering the bold-chat data based on channel (cable, etc.)
        """
        def transform(boldchat_df):
            return boldchat_df.where("channel_name ='{}'".format(channel_name))
        return transform


    def __inbound_type(self, boldchat_df):
        """
            adding friendly inbound type columns
        """
        flags = {'0':'Organic','8':'VA','4':'Proactive'}
        flags_udf = udf(lambda x: flags[x] if x in flags else 'N/A', StringType())
        return boldchat_df.withColumn("inbound_type", flags_udf("inbound_flag"))

    def __chat_date(self, boldchat_df):
        " Simply adding a date column, cutting the date part from a chat_start timestamp "
        get_date_part_udf = udf(lambda ts: str(ts)[:10], StringType())
        return boldchat_df.withColumn("chat_start_date", get_date_part_udf("start_timestamp"))

    def __chat_sess_length(self, boldchat_df):
        " Adding a new col showing the length of the session in minutes."
        chat_sess_udf = udf(BoldChatETL.time_diff, DoubleType())
        return boldchat_df.withColumn("chat_session_length",
                                     chat_sess_udf(boldchat_df.start_timestamp, boldchat_df.end_timestamp))

    def __normalize_timestamps(self, boldchat_df):
        "Normalizing start and end time stamps"
        return boldchat_df\
            .withColumn("start_timestamp", substring(boldchat_df.start_timestamp, 1, 19))\
            .withColumn("end_timestamp", substring(boldchat_df.end_timestamp, 1, 19))

    def __msg_not_empty(self, msg_col):
        "Removing all the records with no value for the customer message (empty messages)"
        def curry(boldchat_df):
            msg_not_empty_udf = udf(lambda msg: len(msg.strip()) > 0 , BooleanType())
            return boldchat_df.where(msg_not_empty_udf(boldchat_df[msg_col]))
        return curry

    def __cust_wait_time(self, boldchat_df):
        " Adding customer wait time column"
        wait_time_udf = udf(BoldChatETL.add_wait_time, DoubleType())
        return boldchat_df.withColumn("customer_wait_time", wait_time_udf(boldchat_df.message_customer, \
                                                                          boldchat_df.message_technician, \
                                                                          boldchat_df.chat_start_date))
    def __truncate_message(self, boldchat_df):
        ""
        truncate_msg_udf = udf(BoldChatETL.truncate_msg, StringType())
        return boldchat_df.withColumn("extract_msg", truncate_msg_udf(boldchat_df.message_customer))


    def __extract_cus_msg_spacy(self, boldchat_df):
        extract_cus_msg_udf = udf(BoldChatETL.extract_cus_msg_spacy, StringType())
        return boldchat_df.withColumn("clean_text", extract_cus_msg_udf(boldchat_df['extract_msg']))

    def __extract_cus_msg(self, boldchat_df):
        documentAssembler = DocumentAssembler() \
            .setInputCol("extract_msg") \
            .setOutputCol("document")

        tokenizer = Tokenizer() \
            .setInputCols(["document"]) \
            .setOutputCol("token")

        normalizer = Normalizer() \
            .setInputCols(["token"]) \
            .setOutputCol("normalized") \
            .setLowercase(True)

        lemmatizer = LemmatizerModel().pretrained()\
            .setInputCols(["normalized"]) \
            .setOutputCol("lemma")

        stopwords_cleaner = StopWordsCleaner() \
            .setInputCols(["lemma"]) \
            .setOutputCol("cleanTokens")\
            .setStopWords(list(ALL_BOLDCHAT_STOPWORDS))

        finisher = Finisher() \
            .setInputCols(["normalized"]) \
            .setOutputCols("clean_text") \
            .setOutputAsArray(False) \
            .setCleanAnnotations(True)\
            .setAnnotationSplitSymbol(" ")

        nlp_pipeline = Pipeline(
            stages=
            [documentAssembler, tokenizer, normalizer, lemmatizer, stopwords_cleaner, finisher]
        )

        return nlp_pipeline\
            .fit(boldchat_df)\
            .transform(boldchat_df)

    def __damaged_phone_mention(self, boldchat_df):
        phrase_list = ['screen is cracked', 'broken phone', 'device is cracked','phone is cracked',\
                 'cracked my cell phone screen', 'cracked my phone screen',\
                 'cracked my screen', 'cracked my phone', 'screen cracked', 'phone cracked',\
                 'cracked screen', 'phone is broken', 'cracked phone', 'cracked the screen',\
                 'phone is damaged', 'repair', 'brightstar', 'wefix', 'broken phone',\
                 'damaged phone', 'damaged device', 'device is broken',\
                 'iphone is cracked', 'it’s cracked', 'it is cracked', \
                 'water damage',\
                 'phone is badly cracked', 'screen is completely cracked']

        damaged_phone_udf = udf(BoldChatETL.phrase_mention,  StringType())
        return boldchat_df.withColumn("damaged_phone_phrase", damaged_phone_udf(boldchat_df['extract_msg'], f.array([f.lit(x) for x in phrase_list])))

    def __new_upgrade_mention(self, boldchat_df):
        phrase_list = ['new phone', 'new iphone', 'new samsung', 'new pixel', 'upgrade offer',
                       'upgrade promotion', 'upgrade special offer', 'upgrade gift with purchase',
                       'upgrade pre-order', 'device launch', 'device release', 'new contract',
                       'device financing', 'upfront edge', 'upgrade phone']

        new_upgrade_udf = udf(BoldChatETL.phrase_mention,  StringType())
        return boldchat_df.withColumn("new_upgrade_mention", new_upgrade_udf(boldchat_df['extract_msg'], f.array([f.lit(x) for x in phrase_list])))

    def __trade_mention(self, boldchat_df):
        phrase_list = ['trade', 'trade-in', 'trade in', 'trade value', 'trade price', 'trade quote',
                       'online trade', ]

        trade_udf = udf(BoldChatETL.phrase_mention,  StringType())
        return boldchat_df.withColumn("trade_mention", trade_udf(boldchat_df['extract_msg'], f.array([f.lit(x) for x in phrase_list])))


    def __phone_model_mention(self, boldchat_df):
        phrase_list = ['samsung', 'galaxy', 'samsung galaxy', 'galaxy note', 'galaxy s 20',
                       'galaxy s20', 'galaxy s 21', 'galaxy s21', 'galaxy s21 ultra', 'galaxy s10',
                       'galaxy s21 plus',
                       's20 fe', 's 20 fe', 's20fe', 's 20fe', 'note 20', 'note20', 'galaxy note 20 ultra',
                       'galaxy a51', 'galaxy a71',
                       'galaxy a21', 'galaxy a31', 'galaxy a52', 'galaxy a32',
                       'galaxy fold', 'galaxy z fold',  'galaxy flip', 'galaxy z flip',
                       'pixel','google pixel', 'pixel4a', 'pixel 4a', 'pixel 4a 5g', 'google pixel 6',
                       'google pixel 6 pro', 'google pixel 5',
                       'iphone', 'apple iphone', 'iphone11', 'iphone12', 'iphone 11', 'iphone 12', 'iphone13',
                       'iphone12 pro', 'iphone 13', 'iphone 12 pro', 'iphone 12 pro max', 'iphone se',
                       'iphone 13 pro']

        phone_model_udf = udf(BoldChatETL.phrase_mention,  StringType())
        return boldchat_df.withColumn("phone_model_mention", phone_model_udf(boldchat_df['extract_msg'], f.array([f.lit(x) for x in phrase_list])))




    def __competitor_mention(self, boldchat_df):
        comp_udf = udf(BoldChatETL.competitor_mention, StringType())
        return boldchat_df.withColumn("competitor_mention", comp_udf(boldchat_df['extract_msg']))

    def __product_mention(self, boldchat_df):
        prod_udf = udf(BoldChatETL.product_mention, StringType())
        return boldchat_df.withColumn("product_mention", prod_udf(boldchat_df['extract_msg']))

    ###########################
    #### Cable Service ETL ####
    ###########################
    def cbl_ser_etl(self):
        return self.boldchat_df\
            .transform(self.__filter_by_channel('Cable Service EN'))\
            .transform(self.__inbound_type)\
            .transform(self.__chat_date)\
            .transform(self.__chat_sess_length)\
            .transform(self.__normalize_timestamps)\
            .transform(self.__msg_not_empty("message_customer")) \
            .transform(self.__cust_wait_time)\
            .transform(self.__truncate_message)\
            .transform(self.__msg_not_empty("extract_msg"))\
            .transform(self.__extract_cus_msg_spacy)\
            .transform(self.__msg_not_empty("clean_text"))\
            .transform(self.__competitor_mention)\
            .transform(self.__product_mention)

    ##############################
    #### Wireless Service ETL ####
    ##############################
    def wir_ser_etl(self):
        return self.boldchat_df\
            .transform(self.__filter_by_channel('Wireless Service EN'))\
            .transform(self.__inbound_type)\
            .transform(self.__chat_date)\
            .transform(self.__chat_sess_length)\
            .transform(self.__normalize_timestamps)\
            .transform(self.__msg_not_empty("message_customer")) \
            .transform(self.__cust_wait_time)\
            .transform(self.__truncate_message)\
            .transform(self.__msg_not_empty("extract_msg"))\
            .transform(self.__extract_cus_msg)\
            .transform(self.__msg_not_empty("clean_text"))\
            .transform(self.__competitor_mention)\
            .transform(self.__damaged_phone_mention) \
            .transform(self.__phone_model_mention) \
            .transform(self.__trade_mention) \
            .transform(self.__new_upgrade_mention)

    ##############################
    #### Fido Care ETL ####
    ##############################
    def fido_care_etl(self):
        return self.boldchat_df\
            .transform(self.__filter_by_channel('Fido Care EN'))\
            .transform(self.__inbound_type)\
            .transform(self.__chat_date)\
            .transform(self.__chat_sess_length)\
            .transform(self.__normalize_timestamps)\
            .transform(self.__msg_not_empty("message_customer")) \
            .transform(self.__cust_wait_time)\
            .transform(self.__truncate_message)\
            .transform(self.__msg_not_empty("extract_msg"))\
            .transform(self.__extract_cus_msg_spacy)\
            .transform(self.__msg_not_empty("clean_text"))\
            .transform(self.__competitor_mention)\
            .transform(self.__damaged_phone_mention) \
            .transform(self.__phone_model_mention) \
            .transform(self.__trade_mention) \
            .transform(self.__new_upgrade_mention)


    ############################################
    ######### Static Helper Functions ##########
    ############################################

    @staticmethod
    def time_diff(start_ts, end_ts):
        """
            Calculating the time difference between a give
            start time stamp (start_ts) and end time stamp
            (end_ts) in minutes
        """
        fmt = '%Y-%m-%d %H:%M:%S'
        datetime_start = datetime.strptime(str(start_ts)[:19], fmt)
        datetime_end = datetime.strptime(str(end_ts)[:19], fmt)
        minutes_diff = (datetime_end - datetime_start).total_seconds() / 60.0
        return minutes_diff

    @staticmethod
    def add_wait_time(cus_msg, agt_msg, start_date):
        """
            Calculating the customer wait time before the agent joins the chat
        """

        def extract_message_time(text):
            if len(text.strip()) == 0:
                return '', ''
            msg_list = re.split(r'\|(?=\d)', text)
            msg_time_list = [s[:8] for s in msg_list]
            start_time = msg_time_list[0]
            end_time = msg_time_list[-1]
            return extract_hr_min(start_time), extract_hr_min(end_time)

        def extract_hr_min(text):
            if 'AM' not in text and 'PM' not in text:
                return ''
            time_parts = text.split()
            hr_mt = time_parts[0].split(':')
            a_p = time_parts[1]
            hr = hr_mt[0]
            mt = hr_mt[1]
            if a_p == 'PM':
                if hr != '12':
                    hr = str(int(hr) + 12)
            else:
                if hr == '12':
                    hr = '00'
            if len(hr) == 1:
                hr = '0' + hr
            if len(mt) == 1:
                mt = '0' + mt
            return f'{hr}:{mt}:00'

        cus_start_time, cus_end_time = extract_message_time(cus_msg)
        agt_start_time, agt_end_time = extract_message_time(agt_msg)
        cus_start_dt = start_date + " " + cus_start_time
        agt_start_dt = start_date + " " + agt_start_time

        fmt = '%Y-%m-%d %H:%M:%S'
        cus_start_ts = datetime.strptime(cus_start_dt, fmt)
        agt_start_ts = datetime.strptime(agt_start_dt, fmt)

        start_time_diff = (agt_start_ts - cus_start_ts).total_seconds() / 60.0
        wait_time = start_time_diff if start_time_diff > 0.0 else 0.0
        return wait_time

    @staticmethod
    def truncate_msg(text):
        if len(text.strip()) == 0:
            return ''
        rm_str_1 = "Transferred from Virtual Assistant"
        rm_str_2 = "vaid="
        msg_list = text.split("|")
        extracted_messages = []
        for message in msg_list:
            message = message.replace("::", ":")
            if rm_str_1 in message or rm_str_2 in message:
                continue
            if len(message.split(":")) > 3:
                cus_msg = message.split(":")[2].strip()
            else:
                cus_msg = message.split(":")[-1].strip()
            if len(cus_msg.strip()) == 0:
                continue
            if cus_msg[-1] not in string.punctuation:
                cus_msg += '.'
            extracted_messages.append(cus_msg)
        if len(extracted_messages) == 0:
            return ''
        join_string = " ".join(extracted_messages)
        return join_string.lower()

    @staticmethod
    def phrase_mention(text, phrase_list):
        text_lower = text.lower()
        punc_set = set(string.punctuation)
        punc_free_text = ''.join([ch for ch in text_lower if ch not in punc_set])
        output = []
        for phrase in phrase_list:
            if phrase.lower() in punc_free_text:
                output.append(phrase)
        return " | ".join(output)

    @staticmethod
    def competitor_mention(msg):
        comp_list = ['Bell','Telus']
        men_list = []
        msg_list = msg.split()
        for comp in comp_list:
            if comp.lower() in msg_list:
                men_list.append(comp)
        return " | ".join(men_list)

    @staticmethod
    def product_mention(msg):
        men_list = []
        prod_dict = {"ignite smartstream": "Ignite SmartStream", "ignite smart stream": "Ignite SmartStream",
                     "smartstream": "Ignite SmartStream", "smart stream": "Ignite SmartStream",
                     "ignite tv": "Ignite TV", "tv": "Ignite TV",
                     "ignite internet": "Ignite Internet", "internet": "Ignite Internet",
                     "ignite bundles": "Ignite Bundles", "bundles": "Ignite Bundles", }
        for key, val in prod_dict.items():
            if key.lower() in msg:
                if val not in men_list:
                    men_list.append(val)
        return " | ".join(men_list)

    @staticmethod
    def extract_cus_msg_spacy(msg_text, stop_w=ALL_BOLDCHAT_STOPWORDS):

        def get_spacy_model():
            nlp_model = en_core_web_md.load()
            nlp_model.remove_pipe('ner')
            SPACY_MODEL = nlp_model
            return SPACY_MODEL

        def tok_filter(tok):
            cond_1 = tok.is_alpha
            cond_2 = not tok.is_stop
            cond_3 = tok.lemma_ != '-PRON-'
            return (cond_1 and cond_2 and cond_3)

        def lemma_msg(doc, stop_w):
            lemma_list = [tok.lemma_.replace("datum", "data") for tok in doc if tok_filter(tok)]
            no_stop_list = [word for word in lemma_list if word not in stop_w]
            if len(no_stop_list) < 2:
                return ''
            return " ".join(no_stop_list).replace("e mail", "email").replace("caller d", "caller id")

        spacy_model = get_spacy_model()
        chat_doc = spacy_model(msg_text)
        clean_msg = lemma_msg(chat_doc, stop_w)
        return clean_msg



from pyspark.sql import SparkSession
import confuse
from dotenv import load_dotenv
import os

class DataBricks:

    """
        This Static class provides support for connecting the project to the Azure Databricks
        environment.
    """

    ###  Spark Session  ###
    spark = SparkSession.builder \
        .config("spark.driver.memory", "16G") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.4") \
        .getOrCreate()


 # .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        # .config("spark.kryoserializer.buffer.max", "2000M") \
        #

    @staticmethod
    def read_from_storage(parquet_file_path):
        """
            Reading parquet file from a blob storage to a Spark dataframe
        """
        read_config = confuse.Configuration("databricks_read", __name__)
        read_config.set_file("conf/storage/postpro_input_blob_storage.yaml")
        container_name = read_config['container_name'].get(str)
        storage_name = read_config['storage_name'].get(str)

        """ Loading the destination storage sas key from .env"""
        load_dotenv()
        sas_key = os.environ.get("storage_sas_key")

        DataBricks.spark.conf.set(
            "fs.azure.account.key.%s.dfs.core.windows.net" % storage_name,
            sas_key)

        output_container_path = "abfss://%s@%s.dfs.core.windows.net" % (container_name, storage_name)
        parquet_file_path = "%s/%s" % (output_container_path, parquet_file_path)

        return DataBricks.spark.read.parquet(parquet_file_path)


    @staticmethod
    def read_from_mounted_storage(parquet_file_folder, container_name):
        """
            Reading parquet file from a mounted blob storage to a Spark dataframe
        """
        parquet_file_path="dbfs:/mnt/"+container_name+'/'+parquet_file_folder
        return DataBricks.spark.read.parquet(parquet_file_path)



    @staticmethod
    def write_to_storage(df, folder, mode="overwrite", header=True):
        """
        Writing spark dataframe to Azure Blob Storage in Parquet Format
        :param df: Spark Dataframe to be written to storage
        :param folder: custom folder in blob storage containing the dataOPs
        :param mode: overwrite, append, etc.
        :param header: True/False
        """

        write_config = confuse.Configuration("databricks_write", __name__)
        write_config.set_file("conf/storage/prepro_output_blob_storage.yaml")

        """ Reading container name and storage name from config """
        container_name = write_config['container_name'].get(str)
        storage_name = write_config['storage_name'].get(str)

        """ Loading the destination storage sas key from .env"""
        load_dotenv()
        sas_key = os.environ.get("storage_sas_key")

        DataBricks.spark.conf.set(
            "fs.azure.account.key.%s.dfs.core.windows.net" % storage_name,
            sas_key)

        output_container_path = "abfss://%s@%s.dfs.core.windows.net" % (container_name, storage_name)
        output_blob_folder = "%s/%s" % (output_container_path, folder)

        df \
            .coalesce(1) \
            .write \
            .mode(mode) \
            .option("header", header) \
            .parquet(output_blob_folder)


    @staticmethod
    def write_to_mounted_storage(df,folder, container_name, mode="overwrite", header=True):
        """
        Writing spark dataframe to mounted container in DBFS from Azure Blob Storage in Parquet Format
        :param df: Spark Dataframe to be written to mounted container
        :param folder: custom folder in mounted container that includes outputs
        :param mode: overwrite, append, etc.
        :param header: True/False
        """

        output_folder = "dbfs:/mnt/"+container_name+'/'+folder

        df \
            .coalesce(1) \
            .write \
            .mode(mode) \
            .option("header", header) \
            .parquet(output_folder)

    @staticmethod
    def write_to_storage_input_key(df, folder, sas_key, container_name, storage_name, mode="overwrite", header=True):
        """
        Writing spark dataframe to Azure Blob Storage in Parquet Format
        :param df: Spark Dataframe to be written to storage
        :param folder: custom folder in blob storage containing the dataOPs
        :param mode: overwrite, append, etc.
        :param header: True/False
        """

        DataBricks.spark.conf.set(
            "fs.azure.account.key.%s.dfs.core.windows.net" % storage_name,
            sas_key)

        output_container_path = "abfss://%s@%s.dfs.core.windows.net" % (container_name, storage_name)
        output_blob_folder = "%s/%s" % (output_container_path, folder)

        df \
            .coalesce(1) \
            .write \
            .mode(mode) \
            .option("header", header) \
            .parquet(output_blob_folder)






# COMMAND ----------

logger = logging.getLogger("BoldChat_Wireless_ETL")

" ***** min_date should be read from a parameter storage on Azure, assigning manually for now **** "
min_date = '2021-07-19'
max_date = '2021-07-20'
###################################
## Creating a BoldChatETL Object ##
## to perform ETL jobs on the    ##
## input data.                   ##
###################################
boldchat_etl = BoldChatETL(min_date=min_date, max_date=max_date)

#### Wireless Service ETL ###
wir_ser = boldchat_etl.wir_ser_etl()

### Writing the ETL output to Blob Storage ###
#DataBricks.write_to_storage(wir_ser,"test/wir_bold_prepro_output_20210719_20210720_debugging")

# COMMAND ----------

display(wir_ser)

# COMMAND ----------


