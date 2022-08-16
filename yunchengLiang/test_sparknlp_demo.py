# Databricks notebook source
FINAL_INTERNAL_STOPWORDS_ALIGN = ['actually', 'better', 'was', 'how', 'for', 'could', 'please', 'then',
  'whereupon', 'so', 'consider', 'as', 'any', 'used', 'brief', 'why', 'several', 'hi', 'six', 'by', 
  'hence', 'yourself', 'k', 'looking', 'right', 'saying', 'fifth', 'wish', 'did', "there's", 'whose',
  'uses', 'inward', 'appear', 'doing', 'n’t', 'doesnt', 'her', 'yourselves', 'nothing', 'not', 'believe',
  '‘ve', 'thereby', 'got', 'took', "c's", 'forty', 'each', "we've", 'whether', 'gets', 'obviously',
  'serious', 'inner', 'perhaps', 'me', 'necessary', 'gotten', 'nevertheless', 'furthermore', 'looks',
  'somebody', 'rather', 'elsewhere', 'former', 'seemed', 'away', 'up', 'than', 'except', 'via', 'can',
  'everything', "you've", 'already', 'along', 'currently', 'while', 'selves', 'anyone', 'our', 
  'thus', 'Shes', 'must', 'either', 'c', "c'mon", 'try', 'wants', 'welcome', '’ve', 'between', 
  'ever', 'z', 'does', 'whereafter', 'twelve', 'apart', 'ca', 'gone', 'awfully', 'came', 'let', 
  'like', 'in', 'u', 'getting', 'again', 'taken', 'itself', 'themselves', 'thank', 'need', 'until',
  'whence', 'she', 'no', 'us', "they'll", 'normally', 'amongst', 'greetings', 'nearly', 'despite',
  'hither', "i'll", 'consequently', 'were', 'whole', 'couldnt', 'knows', 'nine', 'everywhere', 
  'under', 'mainly', 'thanx', 'an', 'corresponding', 'therein', 'would', 'containing', 'causes', 
  'beyond', 'near', 'become', "you're", 'etc', 'liked', "doesn't", 'seriously', 'sure', 's', 
  'asking', 'uucp', 'sixty', 'gives', 'merely', 'myself', 'they', 'about', "hasn't", 'ie', 'st', 
  'indicates', 'far', "i'd", 'since', 'put', 'amount', 'whatever', 'whereby', 'though', 'lately',
  'nd', 'thru', "isn't", "that's", 'a', "haven't", "hadn't", "'ll", 'never', 'cant', 'saw', 'viz',
  'theres', 'their', 'unlikely', 'even', 'ours', 'twenty', "can't", 'am', 'mo', 'jus', 'someone',
  'that', "'s", 'last', 'to', 'd', "he's", "couldn't", 'becoming', 'placed', 'upon', 'one', 
  'this', 'meanwhile', 'more', 'else', 'usually', 'definitely', 'hello', "who's", 'himself', 
  'moreover', 'bottom', 'tends', 'possible', 'well', 'regardless', "shouldn't", 'became',
  'reasonably', 'same', 'b', 'everybody', 'gonna', 'j', 'alone', 'self', '‘d', 'theyd', 
  'these', "wasn't", 'before', 'needs', 'goes', 'with', 'but', 'because', 'went', 'following',
  'hers', 'really', 'thereupon', 'thorough', 'third', 'always', 'described', "we'll", 'some',
  "'ve", 'associated', 'ivent', 'thanks', "i've", 'seen', 'think', 'hereby', 'his', "what's",
  'above', 'going', 'part', 'twice', 'wont', 'th', 'few', 'hows', 'formerly', "'d", 'insofar',
  'sensible', 'happens', 'maybe', 'however', '’d', 'seeming', 'having', 'co', 'somewhere', 
  'him', 'neither', 'okay', 'do', 'whereas', 'according', 'particularly', 'ask', 'howbeit',
  'o', 'besides', 'wherein', "they've", 'contains', 'next', 'throughout', 'against', 'edu',
  'et', 'is', 'unless', 'yet', 'therefore', 'many', 'eight', 'still', 'name', 'certainly', 
  'nobody', 'mine', 'top', 'sup', 'somehow', 'you', 'anybody', 'eg', 'overall', 'been', 'fifty', 
  'sometimes', 'where', 'hereafter', 'three', 'g', 'quite', 'towards', 'made', 'may', 'gotta', 'side',
  'thereafter', 'beside', 'noone', 'none', 'be', 'anywhere', 'further', 'help', '‘ll', 'accordingly',
  'specifying', 'toward', 'due', "you'll", 'hopefully', 'my', 'what', 'p', 'youre', 'pl', 'beforehand',
  'there', 'secondly', 'shes', "we're", 'thence', 'ignored', 'forth', 'indicated', 'together', 'four',
  'hardly', 'useful', 'kept', 'sub', 'yes', 'among', 'at', 'q', "n't", 'ex', 'such', 'considering', 
  't', 'using', 'something', 'ourselves', 'r', 'keep', "wouldn't", 'y', 'another', 'clearly', 'onto', 
  'im', 'given', 'latterly', '‘s', 'dont', 'especially', 'seeing', 'appropriate', 'later', 'had', 'or', 
  'only', 'provides', 'ididnt', 'hundred', 'whenever', 'anyways', 'also', 'itd', 'see', '‘re', 'havent', 
  'best', 'whither', "aren't", 'less', 'allows', 'lest', 'much', 'others', 'n', 'should', 'immediate', 
  'although', 'around', 'sometime', 'example', 'seem', 'entirely', "they're", 'que', 'Im', 'arent', 
  'of', 'l', 'oh', 'down', 'five', 'them', 'allow', 'on', "you'd", 'followed', '‘m', 'yours', 'regard', 
  'somewhat', 'tried', 'very', 'thoroughly', 'various', 'empty', 'shall', "weren't", 'e', "it'll", 
  'through', 'wherever', 'most', 'cannot', "it's", 'here', 'thatll', 'namely', "don't", 'mostly', 
  'during', 'per', 'v', 'whoever', 'out', 'we', "let's", 'course', 'qv', 'alls', 'appreciate', '’re', 
  'want', 'both', 're', 'ten', 'h', 'm', 'anyhow', 'after', 'back', 'trying', 'wasnt', 'everyone', 
  'done', 'often', 'regards', 'concerning', 'w', 'presumably', 'oops', 'hasnt', "they'd", "ain't", 
  'latter', 'might', 'seems', 'now', 'take', 'unto', 'says', 'certain', 'fifteen', "won't", 'every', 
  'own', '’m', 'truly', 'becomes', 'sent', 'likely', 'all', 'ok', 'make', 'sorry', 'which', 'full', 
  'indeed', 'thats', 'nor', 'give', 'other', 'almost', 'unfortunately', 'hereupon', 'who', 'wonder', 
  'nowhere', "here's", "i'm", 'willing', 'youll', '’s', "'re", 'rd', 'com', 'changes', 'said', 'too', 
  'exactly', 'tries', 'x', "didn't", 'ive', 'he', 'i', 'vs', "it'd", 'particular', 'its', 'inasmuch', 
  'those', 'probably', 'the', 'contain', 'it', 'first', 'herein', 'hes', '’ll', 'instead', 'ones', 
  'indicate', 'just', 'specify', 'over', "a's", 'have', 'and', "we'd", "'m", 'specified', 'when', 
  'relatively', 'eleven', 'below', 'if', 'f', 'theirs', 'behind', 'whom', 'didnt', 'are', 'will', 
  "where's", 'from', 'into', 'enough', 'least', 'your', 'anyway', 'n‘t', 'un', "t's", 'cause', 
  'herself', 'keeps', 'ought', 'has', 'within', 'comes', 'once', 'novel', 'front', 'anything', 'known', 
  'afterwards', 'aside', 'look', 'being', 'off', 'across', 'mean', 'respectively', 'value', 'two', 
  'otherwise', 'Dont', 'isnt', 'regarding', 'without', 'follows', 'ltd', 'pci','#pci#','#PCI#']

# COMMAND ----------

# from rogers.cd.preprocessor import BoldChatETL
# from rogers.cd.SparkOPs import DataBricks

from rogers.cd.assets import ALL_BOLDCHAT_STOPWORDS
from rogers.cd.dataOPs import BoldChat

import sparknlp
import logging
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from sparknlp.base import *
from sparknlp.annotator import *
from datetime import datetime
from pyspark.ml import Pipeline
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
                 'start_timestamp', 'end_timestamp', 'channel_name', 'chat_url', 'inbound_flag']#41 inboundflag是什么，chaturl和sessionid是否唯一
    #41 botchat和livechat是怎么融合的 只留一个 还是怎么着
    def __init__(self, min_date, max_date):
        """
        :param boldchat_session_df: Spark DataFrame containing raw boldchat_session data for a given time period
        :param livechat_ss_df: Spark DataFrame containing raw livechat_sas_summary data for a given time period
        """
        self.min_date = min_date
        self.max_date = max_date
        self.logger = logging.getLogger(__name__)#41

        self.logger.info("reading boldchat data from {} forward...".format(self.min_date))
        self.boldchat_session_df , self.livechat_ss_df = BoldChat.load_data(min_date=self.min_date, max_date=self.max_date)#41
        self.logger.info("Data read successfully:\n boldchat_session: {} records, livechat_sas_summary: {} records" \
                    .format(self.boldchat_session_df.count(), self.livechat_ss_df.count()))

        " ******Merging boldchat_seesion and livechat_sas_summary****** "
        self.boldchat_df = self.__merge_input_data(self.boldchat_session_df, self.livechat_ss_df)
        # Keeping the required columns only
        self.boldchat_df = self.boldchat_df.select(BoldChatETL.keep_cols).dropDuplicates()#41 为什么要drop duplicates
        self.logger.info("Number of merged boldchat records: {}".format(self.boldchat_df.count()))
        try:
            assert (self.boldchat_df.count() > 0)
        except AssertionError:
            self.logger.error("The joined boldchat dataframe is empty.")

        ######for quick testing the entire pipeline
        #####here we just take 200 records and try to
        #####run the whole pipeline on this data
        self.boldchat_df=self.boldchat_df.limit(3000)

    def __merge_input_data(self, boldchat_session_df, livechat_ss_df): #41 function之前加__是为什么
        """
        Integrating the input bold_chat and livechat_sas_summary via join operation
        :return: Joined Spark DF
        """
        #The Join Condition
        join_cond = [livechat_ss_df.session_id == boldchat_session_df.session_id,
                     livechat_ss_df.session_start_timestamp == boldchat_session_df.start_timestamp,
                     livechat_ss_df.received_date == boldchat_session_df.received_date]
        #41 recievedate? 为什么starttime，sessionid要一样，意思是一个用户会同时与botchat和livechat在同一个session交互？
        #41 innerjoin意思是我们只考虑同时在一个session开启botchat和livechat的？
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
        flags = {'0':'Organic','8':'VA','4':'Proactive'} #41 四种分别代表着什么
        flags_udf = udf(lambda x: flags[x] if x in flags else 'N/A', StringType()) #41 pandasudf速度是否更快，还是要考虑scala
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
         #41 先normailize再diff，1-19是到哪里，分钟？
        return boldchat_df\
            .withColumn("start_timestamp", substring(boldchat_df.start_timestamp, 1, 19))\
            .withColumn("end_timestamp", substring(boldchat_df.end_timestamp, 1, 19))

    def __msg_not_empty_spark(self, token_col):
        "Removing all the records with no value for the customer message (empty messages)" #91
        def curry(boldchat_df):
            msg_not_empty_udf = udf(lambda msg: len(msg) > 1, BooleanType())
            return boldchat_df.where(msg_not_empty_udf(boldchat_df[token_col])).drop(token_col)
        return curry
      
    def __msg_not_empty(self, msg_col):
        "Removing all the records with no value for the customer message (empty messages)" #41 意思是botchat livechat都没有？那是怎么进去livechat的
        def curry(boldchat_df):
            msg_not_empty_udf = udf(lambda msg: len(msg.strip()) > 0 , BooleanType())
            return boldchat_df.where(msg_not_empty_udf(boldchat_df[msg_col]))
        return curry

    def __cust_wait_time(self, boldchat_df):
        " Adding customer wait time column"
        wait_time_udf = udf(BoldChatETL.add_wait_time, DoubleType())#41
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

      
    def __extract_cus_msg_spark(self, boldchat_df): 
        #比spacy少"datum，email,caller id"处理(ok),只有一个词时的处理(变空)(ok),去掉pronoun(ok)和每个词必须要有字母,符号如'+'要去掉(ok)以及基础停词的消除(ok)
        #spacy的pronoun不算在stopwords里，spark的pronoun被stopwords包含
        documentAssembler = DocumentAssembler() \
            .setInputCol("extract_msg") \
            .setOutputCol("document")

        #必须在tokenize之前做datum->data，e mail->email, caller d->caller id的处理，因为sparknlp去停词会去掉单个字母，且e mail为两个token
        #91 x4
        documentNormalizer1 = DocumentNormalizer() \
            .setInputCols("document") \
            .setOutputCol("normalizedDocument1") \
            .setAction("clean") \
            .setPatterns(["e mail"]) \
            .setReplacement("email") \
            .setPolicy("pretty_all") \
            .setLowercase(True)
        
        documentNormalizer2 = DocumentNormalizer() \
            .setInputCols("normalizedDocument1") \
            .setOutputCol("normalizedDocument2") \
            .setAction("clean") \
            .setPatterns(["caller d"]) \
            .setReplacement("caller id") \
            .setPolicy("pretty_all") \
            .setLowercase(True)

        documentNormalizer3 = DocumentNormalizer() \
            .setInputCols("normalizedDocument2") \
            .setOutputCol("normalizedDocument3") \
            .setAction("clean") \
            .setPatterns(["datum"]) \
            .setReplacement("data") \
            .setPolicy("pretty_all") \
            .setLowercase(True)
        #proved works, we have two types of 1.'  2.’ 
        documentNormalizer4 = DocumentNormalizer() \
            .setInputCols("normalizedDocument3") \
            .setOutputCol("normalizedDocument4") \
            .setAction("clean") \
            .setPatterns(["’"]) \
            .setReplacement("'") \
            .setPolicy("pretty_all")
          
        tokenizer = Tokenizer() \
            .setInputCols(["normalizedDocument4"]) \
            .setOutputCol("token")
      
        #91 get rid of number first, so s21 -> s and could be eliminated by pretrained stopwords
        normalizer1 = Normalizer() \
            .setInputCols(["token"]) \
            .setOutputCol("nonDigitTokens") \
            .setLowercase(True)\
            .setCleanupPatterns(["""[0-9]"""])
 
        lemmatizer = LemmatizerModel.pretrained() \
            .setInputCols(["nonDigitTokens"]) \
            .setOutputCol("lemma")
  
        stopwords_cleaner = StopWordsCleaner() \
            .setInputCols(["lemma"]) \
            .setOutputCol("cleanTokens1")\
            .setStopWords(list(ALL_BOLDCHAT_STOPWORDS))
        #91
        stopWords = StopWordsCleaner()\
            .setInputCols(["cleanTokens1"])\
            .setOutputCol("cleanTokens2")\
            .setStopWords(FINAL_INTERNAL_STOPWORDS_ALIGN)
        
        #sparknlp 比起spacy 多出来去掉脏词这一步
        normalizer2 = Normalizer() \
            .setInputCols(["cleanTokens2"]) \
            .setOutputCol("onlyAlphaTokens") \
            .setLowercase(True)\
            .setCleanupPatterns(["""[^A-Za-z]"""]) #91 only keep alphabet letters, could remove "+"       
        
        #91 outputasarray -> true
        finisher = Finisher() \
            .setInputCols(["onlyAlphaTokens"]) \
            .setOutputCols("clean_text") \
            .setOutputAsArray(False)\
            .setCleanAnnotations(False)\
            .setAnnotationSplitSymbol(" ")

        nlp_pipeline = Pipeline(
            stages=
            [documentAssembler, documentNormalizer1, documentNormalizer2,documentNormalizer3, documentNormalizer4,  tokenizer, normalizer1,  lemmatizer, stopwords_cleaner, stopWords, normalizer2, finisher]
        )
        boldchat_df_col=list(boldchat_df.columns)
        boldchat_df_col.append("clean_text")
        boldchat_df_col.append("onlyAlphaTokens")
        print(boldchat_df_col)
        return nlp_pipeline\
            .fit(boldchat_df)\
            .transform(boldchat_df).select(boldchat_df_col)
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

        damaged_phone_udf = udf(BoldChatETL.phrase_mention,  StringType()) #41 为什么要instantiate不同名字for same udf function？？
        return boldchat_df.withColumn("damaged_phone_phrase", damaged_phone_udf(boldchat_df['extract_msg'], f.array([f.lit(x) for x in phrase_list])))

    def __new_upgrade_mention(self, boldchat_df):
        phrase_list = ['new phone', 'new iphone', 'new samsung', 'new pixel', 'upgrade offer',
                       'upgrade promotion', 'upgrade special offer', 'upgrade gift with purchase',
                       'upgrade pre-order', 'device launch', 'device release', 'new contract',
                       'device financing', 'upfront edge', 'upgrade phone']

        new_upgrade_udf = udf(BoldChatETL.phrase_mention,  StringType()) #41 为什么要instantiate不同名字for same udf function？？
        return boldchat_df.withColumn("new_upgrade_mention", new_upgrade_udf(boldchat_df['extract_msg'], f.array([f.lit(x) for x in phrase_list])))

    def __trade_mention(self, boldchat_df):
        phrase_list = ['trade', 'trade-in', 'trade in', 'trade value', 'trade price', 'trade quote',
                       'online trade', ]

        trade_udf = udf(BoldChatETL.phrase_mention,  StringType()) #41 为什么要instantiate不同名字for same udf function？？
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

        phone_model_udf = udf(BoldChatETL.phrase_mention,  StringType()) #41 为什么要instantiate不同名字for same udf function？？
        return boldchat_df.withColumn("phone_model_mention", phone_model_udf(boldchat_df['extract_msg'], f.array([f.lit(x) for x in phrase_list])))




    def __competitor_mention(self, boldchat_df):
        comp_udf = udf(BoldChatETL.competitor_mention, StringType()) #竞争对少mention 只看俩个 bell telus
        return boldchat_df.withColumn("competitor_mention", comp_udf(boldchat_df['extract_msg']))

    def __product_mention(self, boldchat_df):
        prod_udf = udf(BoldChatETL.product_mention, StringType())
        return boldchat_df.withColumn("product_mention", prod_udf(boldchat_df['extract_msg']))

    ###########################
    #### Cable Service ETL ####
    ###########################
    def cbl_ser_etl_spacy(self):
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
      
    def cbl_ser_etl_spark(self):
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
            .transform(self.__extract_cus_msg_spark)\
            .transform(self.__msg_not_empty_spark("onlyAlphaTokens"))\
            .transform(self.__competitor_mention)\
            .transform(self.__product_mention)    

    ##############################
    #### Wireless Service ETL ####
    ##############################
    def wir_ser_etl_spacy(self):
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
            .transform(self.__extract_cus_msg_spacy)\
            .transform(self.__msg_not_empty("clean_text"))\
            .transform(self.__competitor_mention)\
            .transform(self.__damaged_phone_mention) \
            .transform(self.__phone_model_mention) \
            .transform(self.__trade_mention) \
            .transform(self.__new_upgrade_mention)

    def wir_ser_etl_spark(self):
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
            .transform(self.__extract_cus_msg_spark)\
            .transform(self.__msg_not_empty_spark("onlyAlphaTokens"))\
            .transform(self.__competitor_mention)\
            .transform(self.__damaged_phone_mention) \
            .transform(self.__phone_model_mention) \
            .transform(self.__trade_mention) \
            .transform(self.__new_upgrade_mention)
      
    ##############################
    #### Fido Care ETL ####
    ##############################
    def fido_care_etl_spacy(self):
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

    def fido_care_etl_spark(self):
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
            .transform(self.__extract_cus_msg_spark)\
            .transform(self.__msg_not_empty_spark("onlyAlphaTokens"))\
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
            msg_list = re.split(r'\|(?=\d)', text)#41 需要更仔细地解释 为什么是这些符号 我需要一个sample, match | only when it's followed by number
            msg_time_list = [s[:8] for s in msg_list]
            start_time = msg_time_list[0]
            end_time = msg_time_list[-1]
            return extract_hr_min(start_time), extract_hr_min(end_time)

        def extract_hr_min(text):
            if 'AM' not in text and 'PM' not in text: #41 时间必定有am pm  6：45 AM
                return ''
            time_parts = text.split()#默认splitby空格
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

        cus_start_time, cus_end_time = extract_message_time(cus_msg)#41 要end time有何用
        agt_start_time, agt_end_time = extract_message_time(agt_msg)
        cus_start_dt = start_date + " " + cus_start_time
        agt_start_dt = start_date + " " + agt_start_time

        fmt = '%Y-%m-%d %H:%M:%S'
        cus_start_ts = datetime.strptime(cus_start_dt, fmt)
        agt_start_ts = datetime.strptime(agt_start_dt, fmt)

        start_time_diff = (agt_start_ts - cus_start_ts).total_seconds() / 60.0
        wait_time = start_time_diff if start_time_diff > 0.0 else 0.0 
        return wait_time#41 waittime其实就是session开启对话后过了多久agent才respond

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
                continue #41 也就是说如果message包含有两句话就不算message不append？？？
            if len(message.split(":")) > 3:#41 什么情况
                cus_msg = message.split(":")[2].strip()
            else:
                cus_msg = message.split(":")[-1].strip() #41 应该一个是小时分钟分隔符 一个是说话的引号
            if len(cus_msg.strip()) == 0:
                continue
            if cus_msg[-1] not in string.punctuation:
                cus_msg += '.' #加上标点符号
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
        return " | ".join(men_list) #41 次序有所谓吗

    @staticmethod
    def extract_cus_msg_spacy(msg_text, stop_w=ALL_BOLDCHAT_STOPWORDS):

        def get_spacy_model():
            nlp_model = en_core_web_md.load() #Components: tok2vec, tagger, parser, senter, ner, attribute_ruler, lemmatizer.
            nlp_model.remove_pipe('ner')#41 为什么仅仅不要ner
            SPACY_MODEL = nlp_model
            return SPACY_MODEL

        def tok_filter(tok):
            cond_1 = tok.is_alpha #41有字母
            cond_2 = not tok.is_stop#非停词 此处为默认停词
            cond_3 = tok.lemma_ != '-PRON-' #不是人称词 像是你我他
            return (cond_1 and cond_2 and cond_3)#要同时满足

        def lemma_msg(doc, stop_w):
            lemma_list = [tok.lemma_.replace("datum", "data") for tok in doc if tok_filter(tok)]
            #只要满足三个条件 就先lemmitizize 然后把datum换成data
            no_stop_list = [word for word in lemma_list if word not in stop_w]#在筛选掉stopwords
            if len(no_stop_list) < 2: #41 为什么？？ 只有一个词会是哪一个？
                return ''
            return " ".join(no_stop_list).replace("e mail", "email").replace("caller d", "caller id")#最后整改

        spacy_model = get_spacy_model()
        chat_doc = spacy_model(msg_text)#具体需要了解spacy_model得到token的过程和sparknlp的比较
        clean_msg = lemma_msg(chat_doc, stop_w) 
        return clean_msg


# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE boldchat.livechat_sas_summary

# COMMAND ----------

from pyspark.sql import SparkSession
import confuse
from dotenv import load_dotenv
import os

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

#### Wireless Service ETL ### only choose one at a time, otherwise boldchat get cleaned twice
#wir_ser_spacy = boldchat_etl.wir_ser_etl_spacy()
wir_ser_spark = boldchat_etl.wir_ser_etl_spark()
### Writing the ETL output to Blob Storage ###
#DataBricks.write_to_storage(wir_ser,"test/wir_bold_prepro_output_20210719_20210720_debugging") 

# COMMAND ----------

display(wir_ser_spark)

# COMMAND ----------

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import sparknlp
from sparknlp.annotator import *
from sparknlp.base import *
spark = sparknlp.start()
document_assembler = DocumentAssembler() \
    .setInputCol('text')\
    .setOutputCol('document')

sentence_detector = SentenceDetector() \
    .setInputCols(['document'])\
    .setOutputCol('sentence')

tokenizer = Tokenizer()\
    .setInputCols(['sentence']) \
    .setOutputCol('token')

word_embeddings = WordEmbeddingsModel.pretrained("glove_6B_300", "xx")\
    .setInputCols(["document", "token"])\
    .setOutputCol("embeddings")
    
ner_model = NerDLModel.pretrained("ner_aspect_based_sentiment")\
    .setInputCols(["document", "token", "embeddings"])\
    .setOutputCol("ner")

ner_converter = NerConverter()\
    .setInputCols(['sentence', 'token', 'ner']) \
    .setOutputCol('ner_chunk')

nlp_pipeline = Pipeline(stages=[
    document_assembler, 
    sentence_detector,
    tokenizer,
    word_embeddings,
    ner_model,
    ner_converter])

empty_df = spark.createDataFrame([['']]).toDF('text')
pipeline_model = nlp_pipeline.fit(empty_df)
light_pipeline = LightPipeline(pipeline_model)

# COMMAND ----------

input_list = [
    """From the beginning, we were met by friendly staff members, and the convienent parking at Chelsea Piers made it easy for us to get to the boat."""]
df = spark.createDataFrame(pd.DataFrame({"text": input_list}))
result = pipeline_model.transform(df)
display(result)

# COMMAND ----------

! pip install spark-nlp-display

# COMMAND ----------

from sparknlp_display import NerVisualizer

NerVisualizer().display(result.collect()[0], 'ner_chunk', 'document')

# COMMAND ----------

# Process manually
input_list = [
    """what. then where is the one for my mobile? okay. then. one more question. i'm currently with bell, i mean until the simcard for my phone arrives right. but then the number i'm using is the same, and will be the same. how do i then cancel my plan with bell before activating with you guys? i don want to be double-charged. oh okay so basically have the bell simcard inserted still, and call you guys to switch over. am i right? okay so at the same time i start activating with rogers, bell automatical."""]
df = spark.createDataFrame(pd.DataFrame({"text": input_list}))
lresult = light_pipeline.fullAnnotate(input_list)
for example in lresult:
  for res in example['ner_chunk']:
    print ('Token/Phrase:', res.result, 'Sentiment: ', res.metadata['entity'])

# COMMAND ----------

# Process manually
input_list = [
    """open up?? as in take the card out? cant i see it in system settings? oh. is the sim another name? i don't see sim listed in "about" from system settings. *** is the last digit of iccid. .... and how long does this fraud team take? i need this resolved before the pay date. follows up how? telephone? email? andrew.munden@me.com. (should be the same on my account) yes *** *** *** how much is international text. i "could" add it for a month to reduce this bill? and remove it next month yes??? ok go ahead with the claim for fraud. in writing. email it to me as well? thank you for your time rob. thank you very much. have a good evening. thanks you as well. ciao."""]
df = spark.createDataFrame(pd.DataFrame({"text": input_list}))
lresult = light_pipeline.fullAnnotate(input_list)
for example in lresult:
  for res in example['ner_chunk']:
    print ('Token/Phrase:', res.result, 'Sentiment: ', res.metadata['entity'])

# COMMAND ----------


# Process manually
input_list = [
    """great just want to know why my bill is higher than normal and what the charges if any to calls to the usa last month. my bill is about $16.00 higher than normal. hello? i got some smap calls last month and i kept blocking their numbers. i made calls two months ago and this month was spam calls from us. i called them to tell them to take my number out of their data base adn stop calling me. calls to scottsdale i made fyi. the rest were spam. yes i called them to stop hairdressing me. harrassing. sorry auto correct. i have blocked about 15 numbers of theres calling me. can you set up the no call list list for me. great i will take that number, and are you adjusting my bill. thanks, so my bill will be adjusted and paid on auto pay? can i hit that link now and reg my number? thanks for you help."""]
df = spark.createDataFrame(pd.DataFrame({"text": input_list}))
lresult = light_pipeline.fullAnnotate(input_list)
for example in lresult:
  for res in example['ner_chunk']:
    print ('Token/Phrase:', res.result, 'Sentiment: ', res.metadata['entity'])

# COMMAND ----------

# Process manually
exploded = F.explode(F.arrays_zip('ner_chunk.result', 'ner_chunk.metadata'))
select_expression_0 = F.expr("cols['0']").alias("chunk")
select_expression_1 = F.expr("cols['1']['entity']").alias("ner_label")
result.select(exploded.alias("cols")).show(truncate=False)

# COMMAND ----------

# Process manually
exploded = explode(arrays_zip('ner_chunk.result', 'ner_chunk.metadata'))
select_expression_0 = expr("cols['0']").alias("chunk")
select_expression_1 = expr("cols['1']['entity']").alias("ner_label")
result.select(exploded.alias("cols")) \
    .select(select_expression_0, select_expression_1).show(truncate=False)

# COMMAND ----------


results = pipeline_model.transform(spark.createDataFrame([["Came for lunch my sister. We loved our Thai-style main which amazing with lots of flavours very impressive for vegetarian. But the service was below average and the chips were too terrible to finish."]]).toDF("text"))

# COMMAND ----------

import sparknlp
from sparknlp.base import *
from pyspark.ml import Pipeline

spark = sparknlp.start()

print("Spark NLP version", sparknlp.version())
print("Apache Spark version:", spark.version)

def extract_cus_msg_spark(boldchat_df): 
    documentAssembler = DocumentAssembler() \
        .setInputCol("text") \
        .setOutputCol("document")
    documentNormalizer1 = DocumentNormalizer() \
        .setInputCols("document") \
        .setOutputCol("normalizedDocument1") \
        .setAction("clean") \
        .setPatterns(["e mail"]) \
        .setReplacement("email") \
        .setPolicy("pretty_all") \
        .setLowercase(True)
    documentNormalizer2 = DocumentNormalizer() \
        .setInputCols("normalizedDocument1") \
        .setOutputCol("normalizedDocument2") \
        .setAction("clean") \
        .setPatterns(["caller d"]) \
        .setReplacement("caller id") \
        .setPolicy("pretty_all") \
        .setLowercase(True)
    documentNormalizer3 = DocumentNormalizer() \
        .setInputCols("normalizedDocument2") \
        .setOutputCol("normalizedDocument3") \
        .setAction("clean") \
        .setPatterns(["datum"]) \
        .setReplacement("data") \
        .setPolicy("pretty_all") \
        .setLowercase(True)
    #proved works, we have two types of 1.'  2.’ 
    documentNormalizer4 = DocumentNormalizer() \
        .setInputCols("normalizedDocument3") \
        .setOutputCol("normalizedDocument4") \
        .setAction("clean") \
        .setPatterns(["’"]) \
        .setReplacement("'") \
        .setPolicy("pretty_all")
    tokenizer = Tokenizer() \
        .setInputCols(["normalizedDocument4"]) \
        .setOutputCol("token")
    #91 get rid of number first, so s21 -> s and could be eliminated by pretrained stopwords
    normalizer1 = Normalizer() \
        .setInputCols(["token"]) \
        .setOutputCol("nonDigitTokens") \
        .setLowercase(True)\
        .setCleanupPatterns(["""[0-9]"""])
    lemmatizer = LemmatizerModel.pretrained() \
        .setInputCols(["nonDigitTokens"]) \
        .setOutputCol("lemma")
    stopwords_cleaner = StopWordsCleaner() \
        .setInputCols(["lemma"]) \
        .setOutputCol("cleanTokens1")\
        .setStopWords(list(ALL_BOLDCHAT_STOPWORDS))
    stopWords = StopWordsCleaner()\
        .setInputCols(["cleanTokens1"])\
        .setOutputCol("cleanTokens2")\
        .setStopWords(FINAL_INTERNAL_STOPWORDS_ALIGN)
    normalizer2 = Normalizer() \
        .setInputCols(["cleanTokens2"]) \
        .setOutputCol("onlyAlphaTokens") \
        .setLowercase(True)\
        .setCleanupPatterns(["""[^A-Za-z]"""]) #91 only keep alphabet letters, could remove "+"       
    #91 outputasarray -> true
    finisher = Finisher() \
        .setInputCols(["onlyAlphaTokens"]) \
        .setOutputCols("clean_text") \
        .setOutputAsArray(False)\
        .setCleanAnnotations(False)\
        .setAnnotationSplitSymbol(" ")
    nlp_pipeline = Pipeline(
        stages=
        [documentAssembler, documentNormalizer1, documentNormalizer2,documentNormalizer3, documentNormalizer4,  tokenizer, normalizer1,  lemmatizer, stopwords_cleaner, stopWords, normalizer2, finisher]
    )
    boldchat_df_col=list(boldchat_df.columns)
    boldchat_df_col.append("clean_text")
    boldchat_df_col.append("onlyAlphaTokens")
    print(boldchat_df_col)
    return nlp_pipeline\
        .fit(boldchat_df)\
        .transform(boldchat_df)#.select(boldchat_df_col)
  

data = spark.createDataFrame([["hello yes i have received an e mail stating that i owe ***$ something when my bill was only 51$ and im on a low income internet plan. okay. okay. no. i wasn't aware of that. the only thing i have is the other internet receiver. which i wasn't told to return. cause i was speaking to someone a while ago like i believe last week and they didn't say anything about returning. for the modem? ***c*** okay. yes. so once i return that ***$ be taken off correct. so once i return will my account be back to 5..., caller d is a datum , i want buy an iphone+ and samsung s21, don't don’t , financing removed, actually probably"]]).toDF("text")

display(extract_cus_msg_spark(data))

# COMMAND ----------

display(extract_cus_msg_spark(data).select("text","clean_text"))

# COMMAND ----------


