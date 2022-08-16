# Databricks notebook source
import spacy
import en_core_web_md
#loading the english language small model of spacy
en = spacy.load('en_core_web_md')
spacy_stopwords = list(en.Defaults.stop_words)
spacy_stopwords.remove('move')
spacy_stopwords.remove('call')
print(len(spacy_stopwords))
print(spacy_stopwords)

# COMMAND ----------

spark_stopwords=['a', "a's", 'able', 'about', 'above', 'according', 'accordingly', 'across', 'actually', 'after', 'afterwards', 'again', 'against', "ain't", 'all', 'allow', 'allows', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'an', 'and', 'another', 'any', 'anybody', 'anyhow', 'anyone', 'anything', 'anyway', 'anyways', 'anywhere', 'apart', 'appear', 'appreciate', 'appropriate', 'are', "aren't", 'around', 'as', 'aside', 'ask', 'asking', 'associated', 'at', 'available', 'away', 'awfully', 'b', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'behind', 'being', 'believe', 'below', 'beside', 'besides', 'best', 'better', 'between', 'beyond', 'both', 'brief', 'but', 'by', 'c', "c'mon", "c's", 'came', 'can', "can't", 'cannot', 'cant', 'cause', 'causes', 'certain', 'certainly', 'changes', 'clearly', 'co', 'com', 'come', 'comes', 'concerning', 'consequently', 'consider', 'considering', 'contain', 'containing', 'contains', 'corresponding', 'could', "couldn't", 'course', 'currently', 'd', 'definitely', 'described', 'despite', 'did', "didn't", 'different', 'do', 'does', "doesn't", 'doing', "don't", 'done', 'down', 'downwards', 'during', 'e', 'each', 'edu', 'eg', 'eight', 'either', 'else', 'elsewhere', 'enough', 'entirely', 'especially', 'et', 'etc', 'even', 'ever', 'every', 'everybody', 'everyone', 'everything', 'everywhere', 'ex', 'exactly', 'example', 'except', 'f', 'far', 'few', 'fifth', 'first', 'five', 'followed', 'following', 'follows', 'for', 'former', 'formerly', 'forth', 'four', 'from', 'further', 'furthermore', 'g', 'get', 'gets', 'getting', 'given', 'gives', 'go', 'goes', 'going', 'gone', 'got', 'gotten', 'greetings', 'h', 'had', "hadn't", 'happens', 'hardly', 'has', "hasn't", 'have', "haven't", 'having', 'he', "he's", 'hello', 'help', 'hence', 'her', 'here', "here's", 'hereafter', 'hereby', 'herein', 'hereupon', 'hers', 'herself', 'hi', 'him', 'himself', 'his', 'hither', 'hopefully', 'how', 'howbeit', 'however', 'i', "i'd", "i'll", "i'm", "i've", 'ie', 'if', 'ignored', 'immediate', 'in', 'inasmuch', 'inc', 'indeed', 'indicate', 'indicated', 'indicates', 'inner', 'insofar', 'instead', 'into', 'inward', 'is', "isn't", 'it', "it'd", "it'll", "it's", 'its', 'itself', 'j', 'just', 'k', 'keep', 'keeps', 'kept', 'know', 'knows', 'known', 'l', 'last', 'lately', 'later', 'latter', 'latterly', 'least', 'less', 'lest', 'let', "let's", 'like', 'liked', 'likely', 'little', 'look', 'looking', 'looks', 'ltd', 'm', 'mainly', 'many', 'may', 'maybe', 'me', 'mean', 'meanwhile', 'merely', 'might', 'more', 'moreover', 'most', 'mostly', 'much', 'must', 'my', 'myself', 'n', 'name', 'namely', 'nd', 'near', 'nearly', 'necessary', 'need', 'needs', 'neither', 'never', 'nevertheless', 'new', 'next', 'nine', 'no', 'nobody', 'non', 'none', 'noone', 'nor', 'normally', 'not', 'nothing', 'novel', 'now', 'nowhere', 'o', 'obviously', 'of', 'off', 'often', 'oh', 'ok', 'okay', 'old', 'on', 'once', 'one', 'ones', 'only', 'onto', 'or', 'other', 'others', 'otherwise', 'ought', 'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'overall', 'own', 'p', 'particular', 'particularly', 'per', 'perhaps', 'placed', 'please', 'plus', 'possible', 'presumably', 'probably', 'provides', 'q', 'que', 'quite', 'qv', 'r', 'rather', 'rd', 're', 'really', 'reasonably', 'regarding', 'regardless', 'regards', 'relatively', 'respectively', 'right', 's', 'said', 'same', 'saw', 'say', 'saying', 'says', 'second', 'secondly', 'see', 'seeing', 'seem', 'seemed', 'seeming', 'seems', 'seen', 'self', 'selves', 'sensible', 'sent', 'serious', 'seriously', 'seven', 'several', 'shall', 'she', 'should', "shouldn't", 'since', 'six', 'so', 'some', 'somebody', 'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhat', 'somewhere', 'soon', 'sorry', 'specified', 'specify', 'specifying', 'still', 'sub', 'such', 'sup', 'sure', 't', "t's", 'take', 'taken', 'tell', 'tends', 'th', 'than', 'thank', 'thanks', 'thanx', 'that', "that's", 'thats', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'thence', 'there', "there's", 'thereafter', 'thereby', 'therefore', 'therein', 'theres', 'thereupon', 'these', 'they', "they'd", "they'll", "they're", "they've", 'think', 'third', 'this', 'thorough', 'thoroughly', 'those', 'though', 'three', 'through', 'throughout', 'thru', 'thus', 'to', 'together', 'too', 'took', 'toward', 'towards', 'tried', 'tries', 'truly', 'try', 'trying', 'twice', 'two', 'u', 'un', 'under', 'unfortunately', 'unless', 'unlikely', 'until', 'unto', 'up', 'upon', 'us', 'use', 'used', 'useful', 'uses', 'using', 'usually', 'uucp', 'v', 'value', 'various', 'very', 'via', 'viz', 'vs', 'w', 'want', 'wants', 'was', "wasn't", 'way', 'we', "we'd", "we'll", "we're", "we've", 'welcome', 'well', 'went', 'were', "weren't", 'what', "what's", 'whatever', 'when', 'whence', 'whenever', 'where', "where's", 'whereafter', 'whereas', 'whereby', 'wherein', 'whereupon', 'wherever', 'whether', 'which', 'while', 'whither', 'who', "who's", 'whoever', 'whole', 'whom', 'whose', 'why', 'will', 'willing', 'wish', 'with', 'within', 'without', "won't", 'wonder', 'would', 'would', "wouldn't", 'x', 'y', 'yes', 'yet', 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves', 'z', 'zero']

# COMMAND ----------

spark_extra= list(set(spark_stopwords)-set(spacy_stopwords))
spacy_extra= list(set(spacy_stopwords)-set(spark_stopwords))
print("spark_extra: ", spark_extra)
print("***********")
print("spacy_extra: ", spacy_extra)

# COMMAND ----------

INTERNAL_STOPWORDS_ALIGN=spacy_stopwords.copy()
INTERNAL_STOPWORDS_ALIGN.extend(["let's", 'better', 'actually', 'gives', 'merely', 'course', 'certainly', 'qv', 'appreciate', "hasn't", 'ie', 'want', 'inc', 'indicates', 'far', "i'd", 'consider', 'h', 'sup', 'm', 'brief', 'trying', 'anybody', 'eg', 'lately', 'overall', 'hi', 'nd', "isn't", 'k', "that's", 'looking', 'right', 'saying', "haven't", 'g', 'fifth', 'wish', 'able', "there's", "hadn't", 'know', 'regards', 'concerning', 'w', 'presumably', 'cant', 'saw', 'uses', 'viz', 'inward', 'theres', 'appear', 'unlikely', "they'd", "ain't", 'help', 'accordingly', 'specifying', "can't", 'unto', "you'll", 'hopefully', 'says', 'certain', 'p', 'd', "won't", 'believe', "he's", "couldn't", 'truly', 'got', 'placed', 'took', 'non', "c's", 'secondly', 'sent', "we've", "we're", 'likely', 'ok', 'ignored', 'gets', 'obviously', 'forth', 'inner', 'usually', 'definitely', 'way', 'hello', 'indicated', "who's", 'sorry', 'necessary', 'gotten', 'thats', 'hardly', 'tends', 'possible', 'regardless', 'use', 'furthermore', 'looks', 'somebody', "shouldn't", 'useful', 'kept', 'sub', 'yes', 'reasonably', 'wonder', "here's", "i'm", 'willing', 'q', 'b', 'everybody', 'ex', 'considering', 'j', 't', 'away', 'rd', 'self', 'com', 'changes', 'said', "you've", 'currently', 'exactly', 'tries', "wasn't", 'selves', 'x', "didn't", 'needs', 'r', 'vs', "it'd", 'goes', 'particular', "wouldn't", 'unfortunately', 'inasmuch', 'c', 'tell', 'probably', "c'mon", 'y', 'contain', 'try', 'went', 'following', 'clearly', 'thorough', 'instead', 'given', 'ones', 'indicate', 'specify', 'wants', 'especially', 'welcome', 'seeing', 'described', 'appropriate', 'later', "a's", 'z', "we'll", "we'd", 'specified', 'provides', 'apart', 'associated', 'gone', 'relatively', 'awfully', 'thanks', 'came', 'let', 'like', "i've", 'anyways', 'soon', 'u', 'getting', 'f', 'theirs', 'best', 'seen', "aren't", 'allows', 'lest', 'n', 'think', "what's", 'taken', 'thank', 'immediate', 'need', 'going', 'example', "they'll", "where's", 'entirely', 'normally', 'twice', "they're", 'que', 'th', 'un', "t's", 'cause', 'greetings', 'nearly', 'despite', 'hither', "i'll", 'insofar', 'sensible', 'happens', 'maybe', 'consequently', 'l', 'oh', 'keeps', 'ought', 'comes', 'having', 'co', 'novel', 'knows', 'mainly', 'thanx', 'okay', 'allow', "you'd", 'according', 'followed', 'corresponding', 'known', 'particularly', 'aside', 'ask', 'look', 'howbeit', 'o', 'come', 'containing', 'causes', 'somewhat', 'tried', 'thoroughly', "they've", 'shall', "weren't", 'e', 'contains', 'mean', 'near', "it'll", 'respectively', "you're", 'value', 'etc', 'liked', "doesn't", 'edu', "it's", 'seriously', 'et', 'sure',  's', 'asking', 'follows', "don't", 'uucp', 'v', 'ltd'])
INTERNAL_STOPWORDS_ALIGN.extend(['didnt','doesnt','dont','wont','havent','hasnt','shes','hes','im','gonna','Im','couldnt','regard','mo','st','pl','ive','Shes','alls','arent','as','Dont','gotta','hows','ididnt','isnt','itd','its','jus','ivent','not','ok','oops','sure','thatll','thats','theyd','wasnt','youre','youll'])

# COMMAND ----------

print(INTERNAL_STOPWORDS_ALIGN)

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
    INTERNAL_STOPWORDS_ALIGN = INTERNAL_STOPWORDS_ALIGN# 91 save this in the asset.py
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
        self.boldchat_df=self.boldchat_df.limit(200)

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
            .setStopWords(INTERNAL_STOPWORDS_ALIGN)
        
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
            [documentAssembler, documentNormalizer1, documentNormalizer2,documentNormalizer3, documentNormalizer4,  tokenizer, normalizer1, lemmatizer,  stopwords_cleaner, stopWords, normalizer2, finisher]
        )
        boldchat_df_col=list(boldchat_df.columns)
        print(boldchat_df_col)
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

from pyspark.sql import SparkSession
import confuse
from dotenv import load_dotenv
import os

'''class DataBricks:

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
            .parquet(output_blob_folder)'''

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
wir_ser_spacy = boldchat_etl.wir_ser_etl_spacy()
wir_ser_spark = boldchat_etl.wir_ser_etl_spark()
### Writing the ETL output to Blob Storage ###
#DataBricks.write_to_storage(wir_ser,"test/wir_bold_prepro_output_20210719_20210720_debugging") 

# COMMAND ----------

display(wir_ser_spark.select("session_id","clean_text").orderBy(desc("session_id")))

# COMMAND ----------

display(wir_ser_spacy.select("session_id","clean_text").orderBy(desc("session_id"))) 

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

wir_ser_spark.select("session_id").count()

# COMMAND ----------

wir_ser_spacy.select("session_id").count()

# COMMAND ----------


