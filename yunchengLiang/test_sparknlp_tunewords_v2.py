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

spark_stopwords=['#PCI#','#pci#','a', "a's", 'able', 'about', 'above', 'according', 'accordingly', 'across', 'actually', 'after', 'afterwards', 'again', 'against', "ain't", 'all', 'allow', 'allows', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'an', 'and', 'another', 'any', 'anybody', 'anyhow', 'anyone', 'anything', 'anyway', 'anyways', 'anywhere', 'apart', 'appear', 'appreciate', 'appropriate', 'are', "aren't", 'around', 'as', 'aside', 'ask', 'asking', 'associated', 'at', 'available', 'away', 'awfully', 'b', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'behind', 'being', 'believe', 'below', 'beside', 'besides', 'best', 'better', 'between', 'beyond', 'both', 'brief', 'but', 'by', 'c', "c'mon", "c's", 'came', 'can', "can't", 'cannot', 'cant', 'cause', 'causes', 'certain', 'certainly', 'changes', 'clearly', 'co', 'com', 'come', 'comes', 'concerning', 'consequently', 'consider', 'considering', 'contain', 'containing', 'contains', 'corresponding', 'could', "couldn't", 'course', 'currently', 'd', 'definitely', 'described', 'despite', 'did', "didn't", 'different', 'do', 'does', "doesn't", 'doing', "don't", 'done', 'down', 'downwards', 'during', 'e', 'each', 'edu', 'eg', 'eight', 'either', 'else', 'elsewhere', 'enough', 'entirely', 'especially', 'et', 'etc', 'even', 'ever', 'every', 'everybody', 'everyone', 'everything', 'everywhere', 'ex', 'exactly', 'example', 'except', 'f', 'far', 'few', 'fifth', 'first', 'five', 'followed', 'following', 'follows', 'for', 'former', 'formerly', 'forth', 'four', 'from', 'further', 'furthermore', 'g', 'get', 'gets', 'getting', 'given', 'gives', 'go', 'goes', 'going', 'gone', 'got', 'gotten', 'greetings', 'h', 'had', "hadn't", 'happens', 'hardly', 'has', "hasn't", 'have', "haven't", 'having', 'he', "he's", 'hello', 'help', 'hence', 'her', 'here', "here's", 'hereafter', 'hereby', 'herein', 'hereupon', 'hers', 'herself', 'hi', 'him', 'himself', 'his', 'hither', 'hopefully', 'how', 'howbeit', 'however', 'i', "i'd", "i'll", "i'm", "i've", 'ie', 'if', 'ignored', 'immediate', 'in', 'inasmuch', 'inc', 'indeed', 'indicate', 'indicated', 'indicates', 'inner', 'insofar', 'instead', 'into', 'inward', 'is', "isn't", 'it', "it'd", "it'll", "it's", 'its', 'itself', 'j', 'just', 'k', 'keep', 'keeps', 'kept', 'know', 'knows', 'known', 'l', 'last', 'lately', 'later', 'latter', 'latterly', 'least', 'less', 'lest', 'let', "let's", 'like', 'liked', 'likely', 'little', 'look', 'looking', 'looks', 'ltd', 'm', 'mainly', 'many', 'may', 'maybe', 'me', 'mean', 'meanwhile', 'merely', 'might', 'more', 'moreover', 'most', 'mostly', 'much', 'must', 'my', 'myself', 'n', 'name', 'namely', 'nd', 'near', 'nearly', 'necessary', 'need', 'needs', 'neither', 'never', 'nevertheless', 'new', 'next', 'nine', 'no', 'nobody', 'non', 'none', 'noone', 'nor', 'normally', 'not', 'nothing', 'novel', 'now', 'nowhere', 'o', 'obviously', 'of', 'off', 'often', 'oh', 'ok', 'okay', 'old', 'on', 'once', 'one', 'ones', 'only', 'onto', 'or', 'other', 'others', 'otherwise', 'ought', 'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'overall', 'own', 'p', 'particular', 'particularly', 'per', 'perhaps', 'placed', 'please', 'plus', 'possible', 'presumably', 'probably', 'provides', 'q', 'que', 'quite', 'qv', 'r', 'rather', 'rd', 're', 'really', 'reasonably', 'regarding', 'regardless', 'regards', 'relatively', 'respectively', 'right', 's', 'said', 'same', 'saw', 'say', 'saying', 'says', 'second', 'secondly', 'see', 'seeing', 'seem', 'seemed', 'seeming', 'seems', 'seen', 'self', 'selves', 'sensible', 'sent', 'serious', 'seriously', 'seven', 'several', 'shall', 'she', 'should', "shouldn't", 'since', 'six', 'so', 'some', 'somebody', 'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhat', 'somewhere', 'soon', 'sorry', 'specified', 'specify', 'specifying', 'still', 'sub', 'such', 'sup', 'sure', 't', "t's", 'take', 'taken', 'tell', 'tends', 'th', 'than', 'thank', 'thanks', 'thanx', 'that', "that's", 'thats', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'thence', 'there', "there's", 'thereafter', 'thereby', 'therefore', 'therein', 'theres', 'thereupon', 'these', 'they', "they'd", "they'll", "they're", "they've", 'think', 'third', 'this', 'thorough', 'thoroughly', 'those', 'though', 'three', 'through', 'throughout', 'thru', 'thus', 'to', 'together', 'too', 'took', 'toward', 'towards', 'tried', 'tries', 'truly', 'try', 'trying', 'twice', 'two', 'u', 'un', 'under', 'unfortunately', 'unless', 'unlikely', 'until', 'unto', 'up', 'upon', 'us', 'use', 'used', 'useful', 'uses', 'using', 'usually', 'uucp', 'v', 'value', 'various', 'very', 'via', 'viz', 'vs', 'w', 'want', 'wants', 'was', "wasn't", 'way', 'we', "we'd", "we'll", "we're", "we've", 'welcome', 'well', 'went', 'were', "weren't", 'what', "what's", 'whatever', 'when', 'whence', 'whenever', 'where', "where's", 'whereafter', 'whereas', 'whereby', 'wherein', 'whereupon', 'wherever', 'whether', 'which', 'while', 'whither', 'who', "who's", 'whoever', 'whole', 'whom', 'whose', 'why', 'will', 'willing', 'wish', 'with', 'within', 'without', "won't", 'wonder', 'would', 'would', "wouldn't", 'x', 'y', 'yes', 'yet', 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves', 'z', 'zero']

# COMMAND ----------

spark_extra= list(set(spark_stopwords)-set(spacy_stopwords))
spacy_extra= list(set(spacy_stopwords)-set(spark_stopwords))
print("spark_extra: ", spark_extra)
print("***********")
print("spacy_extra: ", spacy_extra)

# COMMAND ----------

INTERNAL_STOPWORDS_ALIGN=spacy_stopwords.copy()
INTERNAL_STOPWORDS_ALIGN.extend(["let's", 'better', 'actually', 'gives', 'merely', 'course', 'certainly', 'qv', 'appreciate', "hasn't", 'ie', 'want', 'inc', 'indicates', 'far', "i'd", 'consider', 'h', 'sup', 'm', 'brief', 'trying', 'anybody', 'eg', 'lately', 'overall', 'hi', 'nd', "isn't", 'k', "that's", 'looking', 'right', 'saying', "haven't", 'g', 'fifth', 'wish', 'able', "there's", "hadn't", 'know', 'regards', 'concerning', 'w', 'presumably', 'cant', 'saw', 'uses', 'viz', 'inward', 'theres', 'appear', 'unlikely', "they'd", "ain't", 'help', 'accordingly', 'specifying', "can't", 'unto', "you'll", 'hopefully', 'says', 'certain', 'p', 'd', "won't", 'believe', "he's", "couldn't", 'truly', 'got', 'placed', 'took', 'non', "c's", 'secondly', 'sent', "we've", "we're", 'likely', 'ok', 'ignored', 'gets', 'obviously', 'forth', 'inner', 'usually', 'definitely', 'way', 'hello', 'indicated', "who's", 'sorry', 'necessary', 'gotten', 'thats', 'hardly', 'tends', 'possible', 'regardless', 'use', 'furthermore', 'looks', 'somebody', "shouldn't", 'useful', 'kept', 'sub', 'yes', 'reasonably', 'wonder', "here's", "i'm", 'willing', 'q', 'b', 'everybody', 'ex', 'considering', 'j', 't', 'away', 'rd', 'self', 'com', 'changes', 'said', "you've", 'currently', 'exactly', 'tries', "wasn't", 'selves', 'x', "didn't", 'needs', 'r', 'vs', "it'd", 'goes', 'particular', "wouldn't", 'unfortunately', 'inasmuch', 'c', 'tell', 'probably', "c'mon", 'y', 'contain', 'try', 'went', 'following', 'clearly', 'thorough', 'instead', 'given', 'ones', 'indicate', 'specify', 'wants', 'especially', 'welcome', 'seeing', 'described', 'appropriate', 'later', "a's", 'z', "we'll", "we'd", 'specified', 'provides', 'apart', 'associated', 'gone', 'relatively', 'awfully', 'thanks', 'came', 'let', 'like', "i've", 'anyways', 'soon', 'u', 'getting', 'f', 'theirs', 'best', 'seen', "aren't", 'allows', 'lest', 'n', 'think', "what's", 'taken', 'thank', 'immediate', 'need', 'going', 'example', "they'll", "where's", 'entirely', 'normally', 'twice', "they're", 'que', 'th', 'un', "t's", 'cause', 'greetings', 'nearly', 'despite', 'hither', "i'll", 'insofar', 'sensible', 'happens', 'maybe', 'consequently', 'l', 'oh', 'keeps', 'ought', 'comes', 'having', 'co', 'novel', 'knows', 'mainly', 'thanx', 'okay', 'allow', "you'd", 'according', 'followed', 'corresponding', 'known', 'particularly', 'aside', 'ask', 'look', 'howbeit', 'o', 'come', 'containing', 'causes', 'somewhat', 'tried', 'thoroughly', "they've", 'shall', "weren't", 'e', 'contains', 'mean', 'near', "it'll", 'respectively', "you're", 'value', 'etc', 'liked', "doesn't", 'edu', "it's", 'seriously', 'et', 'sure',  's', 'asking', 'follows', "don't", 'uucp', 'v', 'ltd'])
INTERNAL_STOPWORDS_ALIGN.extend(['didnt','doesnt','dont','wont','havent','hasnt','shes','hes','im','gonna','Im','couldnt','regard','mo','st','pl','ive','Shes','alls','arent','as','Dont','gotta','hows','ididnt','isnt','itd','its','jus','ivent','not','ok','oops','sure','thatll','thats','theyd','wasnt','youre','youll','#pci#','#PCI#'])

# COMMAND ----------


FIDO_BOLD_SEED_1 = [
['device upgrade', 'samsung ultra', 'update device upgrade', 'upgrade update', 'upgrade phone upgrade', 'iphone gb', 'upgrade update device', 'order iphone', 'order reference', 'iphone pro max', 'interested upgrade', 'upgrade iphone pro', 'phone upgrade phone', 'order reference number', 'phone upgrade', 'iphone xr', 'interested new phone', 'number order', 'phone ship', 'phone new phone', 'available upgrade', 'shipping confirmation', 'phone order', 'receive shipping', 'upgrade phone', 'iphone pro', 'tracking number', 'new phone', 'pick store']
,
['fido infinite', 'gb unlimited', 'offer offer', 'include gb', 'change wireless plan', 'special offer close', 'infinite plan', 'offer offer available', 'special offer offer', 'offer detail', 'offer fido', 'switch plan', 'detail window customer', 'bpo offer', 'plan include unlimited', 'close bpo offer', 'bpo offer detail', 'plan gb', 'offer detail', 'financing talk', 'share plan', 'wireless plan', 'enjoy offer fido', 'offer available enjoy']
,
['final bill', 'bill bill', 'bill wait', 'extra usage charge', 'extra usage', 'wait bill', 'usage charge wireless', 'pay late', 'apply offer discount', 'offer discount receive', 'discount receive', 'credit bill', 'bill text', 'charge wireless', 'month credit', 'month apply offer', 'work hospital call', 'apply offer', 'line worker', 'health care worker', 'extra charge', 'bill cycle']
,
['account wireless', 'port protection', 'number change', 'communications inc', 'cell number', 'change phone number', 'wireless account', 'add port', 'number change number', 'family plan', 'number area', 'responsibility account', 'new number', 'free service', 'phone number', 'port protection account', 'number change phone', 'transfer responsibility account', 'immediately transfer responsibility', 'number effective', 'transfer account']
,
['check phone lock', 'network unlock code', 'code check iphone', 'unlock code', 'device lock', 'phone unlock lock', 'lock fido', 'verify imei', 'unlock old samsung', 'lock unlock', 'lock fido', 'phone unlock old', 'unlock complete', 'unlocked unlock phone', 'lock unlock free', 'unlock lock', 'old samsung', 'unlock complete verify', 'account unlock iphone', 'iphone account unlock', 'old phone unlock', 'verify imei unlock', 'complete verify', 'unlock code check', 'phone unlock', 'code check', 'iphone unlock', 'order phone unlock']
,
['usage rocket hub', 'add gb', 'reach data', 'data plan', 'data month', 'month plus', 'data overage', 'check data', 'data receive', 'add data', 'find data', 'check data usage', 'usage service', 'turn data', 'text data', 'data gb', 'charge overage', 'speed pass', 'month gb', 'usage past']
,
['line cancel line', 'phone cancel', 'cancel wireless', 'cancel wireless service', 'account cancel account', 'line cancel', 'cancel line line', 'cancel account cancel', 'cancel service', 'cancel cell', 'line end', 'cancel line', 'cancel cell phone', 'cancel account', 'account cancel', 'cancel line cancel', 'cancel fido', 'time cancel']
,
['website change', 'sign fido', 'caller id', 'fido account', 'postal code', 'email email', 'account link', 'account email', 'get account', 'account account', 'number email', 'account number account', 'fido account account', 'find account', 'account link account', 'account number', 'phone number account', 'email bill', 'link account', 'number account number']
,
['payment payment', 'online banking', 'pay payment', 'payment new', 'payment arrangement', 'payment online banking', 'arrangement pay', 'payment bill', 'balance show', 'month pay', 'bill phone', 'visa debit', 'payment arrangement pay', 'payment online', 'payment arrangement past', 'past balance', 'account payment', 'pay month pay', 'payment arrangement payment', 'suppose payment', 'arrangement past', 'pay service', 'payment fido', 'payment month', 'arrangement payment']
,
['apple watch', 'data cost', 'watch online', 'purchase apple', 'watch add', 'order apple watch', 'watch add plan', 'apple watch online', 'purchase apple watch', 'apple watch add', 'plan tablet', 'financing plan', 'order apple']
,
['switch fido', 'exist plan', 'telus fido', 'exist phone', 'transfer line', 'fido switch', 'line fido', 'plan line', 'line plan', 'fido number', 'line phone', 'add line', 'share line', 'fill form', 'fido fido', 'non share']
,
['sim card new', 'card old sim', 'activate new', 'card account', 'old sim card', 'new sim card', 'sim card activate', 'number sim', 'current sim', 'new sim', 'change sim card', 'card old', 'sim card old', 'sim card phone', 'activate sim', 'activate sim card', 'sim number', 'sim card work', 'get new sim', 'card number sim', 'receive sim', 'old sim']
,
['roam home charge', 'bill roam home', 'distance charge', 'canada reverse', 'roam home', 'bill roam', 'home leave', 'leave canada', 'charge charge roam', 'home charge', 'charge roam home', 'canada reverse charge', 'border close', 'long distance', 'long distance charge', 'charge roam', 'roam home leave']
,
['protection add', 'remove device', 'device protection remove', 'protection remove', 'protection plan', 'device protection online', 'device protection', 'premium device protection', 'remove premium', 'cancel premium', 'premium device', 'protection remove premium', 'device protection plan', 'protection online', 'remove premium device', 'remove device protection', 'device protection cancel', 'cancel premium device', 'protection cancel', 'device protection add']
,
['cost phone', 'contract expire', 'device pay', 'plan buy', 'balance leave', 'phone contract', 'leave phone', 'contract end']
,
['phone year', 'unlimited gb', 'perform request', 'voicemail phone', 'iphone visual', 'get disconnected', 'send text', 'data infinite', 'error perform', 'iphone visual voicemail', 'phone work', 'unknown error', 'work set', 'set voicemail', 'error perform request']
,
['exclusive offer account', 'receive email', 'offer account', 'new promotional']
]


CBL_BOLD_SEED_1 = [
['internet promo', 'set expire', 'come end', 'expire month', 'promotion expire', 'promotion internet', 'offer available', 'offer end offer', 'internet promotion end', 'internet plan expire',
 'internet promotion expire', 'plan expire', 'offer deal', 'loyal customer', 'promotion expire soon', 'know promotion', 'internet promo end', 'internet promotion', 'price internet',
  'soon offer', 'expire soon offer', 'new promotion', 'promo end']
,
['charge month', 'charge late payment', 'late payment charge', 'high month', 'charge late', 'month home', 'pay time', 'cycle end', 'late payment', 'new monthly', 'billing billing', 'say billing', 'bill bill', 'bill increase',
'bill go', 'monthly bill', 'increase bill', 'internet bill', 'account balance', 'bill high', 'billing issue',
'current bill', 'overage charge', 'billing inquiry']
,
['service cancel', 'question cancel', 'internet service cancel', 'cancel internet cancel', 'cancel service cancel', 'anymore cancel', 'cancel internet', 'phone cancel', 'cancel cancel', 'cancel cable',
'cancel service', 'cancel home internet', 'cancel rogers', 'service end month', 'service cancel internet', 'service cancel service', 'close account', 'cancel subscription', 'cancel account',
'cancel internet service', 'cancel tv', 'cancel home', 'cancel home phone', 'internet cancel internet']
,
['option account', 'deal internet', 'plan home internet', 'internet ignite', 'cost upgrade', 'change internet plan', 'change internet package', 'question upgrade', 'change plan change', 'internet unlimited',
 'internet plan change', 'data usage', 'new plan', 'internet time', 'roger internet', 'internet deal', 'upgrade internet speed', 'internet package', 'plan home', 'home internet plan', 'internet internet', 'home internet']
,
['rogers online', 'password rogers', 'telecommunication service view', 'message able', 'display info', 'online message', 'say able display', 'account number register', 'manage company telecommunication',
 'work fix', 'online message able', 'telecommunication service', 'manage company', 'reset voicemail', 'fix able display', 'service view', 'number register', 'password rogers online', 'company telecommunication',
  'able display', 'fix issue', 'fix able', 'reset voicemail password', 'rogers online message']
,
['tv internet phone', 'phone tv', 'tv bundle', 'bundle tv internet', 'ignite bundle', 'change flex', 'ignite premier', 'internet home phone', 'ignite popular', 'ignite popular bundle',
 'bundle ignite', 'popular bundle', 'phone bundle', 'home phone tv', 'channel flex', 'bundle tv', 'channel flex channel', 'change flex channel', 'internet phone', 'ignite tv bundle']
,
['modem return', 'modem send', 'modem tell', 'charge return', 'send return', 'return cable box', 'tell send', 'return label', 'store open', 'receive label', 'equipment return', 'wait return', 'return cable',
 'equipment equipment', 'box return', 'label rogers', 'internet modem', 'return old', 'confirm receive', 'shipping label', 'service address', 'modem store', 'return modem', 'return equipment', 'label rogers send']
,
['new modem work', 'issue time', 'status provide', 'box status', 'serial number check', 'provide serial',
 'number check', 'provide serial number', 'speed slow', 'work internet', 'area internet', 'modem work',
  'internet work internet', 'internet connection', 'provide serial', 'phone work', 'internet issue']
,
['come way', 'receive text', 'come rogers', 'reschedule appointment', 'number tell', 'technician come', 'install internet', 'technician install', 'cancel installation', 'installation date']
,
['add channel add', 'theme pack', 'crave tv', 'tv rogers', 'channel tv', 'nfl ticket', 'channel tv package', 'add channel', 'channel add channel', 'rogers nhl']
,
['package change package', 'tv package popular', 'package package', 'change package', 'package popular']
,
['charge pay', 'payment payment', 'credit card info', 'pay internet', 'card info', 'payment online', 'change payment', 'pay bill',
 'payment arrangement', 'internet pay']
,
['number change', 'number link', 'set myroger account', 'account change', 'number end', 'place order', 'account number set', 'number account', 'old account', 'line new', 'change phone', 'number set',
 'receive service', 'link account', 'line change', 'change service', 'internet usage', 'rogers account']
,
['standard long distance', 'distance charge', 'distance plan', 'long distance phone', 'distance phone',
 'long distance plan', 'long distance', 'long distance charge', 'home phone',
  'phone plan', 'standard long']
,
['service area', 'transfer service', 'new address', 'new address new', 'current address', 'address new', 'new place',
 'address new address', 'service new address', 'service hold', 'transfer service new']
,
['safe sign internet', 'safe sign', 'sign internet', 'email state', 'email receive email', 'email rogers', 'sign internet service', 'service limited', 'stay safe sign', 'automatically apply']
]


WIR_BOLD_SEED_1 = [
['plan include unlimited', 'plan gb', 'unlimited canada', 'unlimited data plan', 'change wireless plan', 'gb plan', 'rogers infinite', 'infinite plan', 'unlimited canadawide', 'employee discount',
 'gb unlimited', 'plan plan', 'share plan', 'unlimited canadawide send', 'change plan plan', 'change wireless', 'canada plan', 'unlimited data', 'rogers infinite plan', 'new plan', 'change service', 'rogers offer',
  'canadawide send', 'data plan month', 'canadawide send receive', 'plan include', 'include unlimited']
,
['charge credit', 'activation fee', 'fee waive', 'tell pay', 'health care', 'healthcare worker', 'health care worker',
 'month credit', 'start billing', 'credit apply', 'month free', 'payment charge', 'receive month', 'charge late', 'late payment', 'fee suppose waive', 'pay late', 'credit month', 'monthly bill', 'bill high', 'question bill',
  'month apply', 'pay late payment', 'line worker', 'offer month', 'bill balance', 'billing billing', 'bill bill', 'bill increase', 'bill go', 'monthly bill', 'increase bill', 'bill high', 'billing issue']
,
['samsung note', 'upgrade phone upgrade', 'samsung ultra', 'upgrade phone', 'pro max', 'phone order', 'phone new phone', 'iphone pro', 'available upgrade', 'upgrade upgrade', 'new iphone',
 'gb upgrade', 'upgrade samsung', 'upgrade current', 'phone contract', 'interested new phone', 'break phone', 'phone upgrade phone', 'interested new', 'device balance', 'contract end', 'contract expire', 'pay device',
  'upgrade device', 'phone iphone', 'phone interested', 'upgrade iphone']
,
['network unlock code', 'unlock code', 'iphone account unlock', 'unlock phone', 'old phone unlock', 'unlock iphone chat', 'phone lock require', 'unlock old phone',
 'disconnect unlock complete', 'unlock complete verify', 'speak unlock iphone', 'account unlock iphone', 'unlock complete', 'chat disconnect unlock', 'old samsung', 'upfront edge',
  'unlock iphone account', 'iphone account', 'unlock old samsung', 'unlock old', 'code check check', 'iphone account account', 'verify imei', 'network unlock']
,
['new address', 'protection line account', 'iphone visual voicemail', 'port protection', 'port protection line', 'port protection account', 'change credit card', 'account add',
 'visual voicemail add', 'voicemail add', 'add port', 'change credit', 'port protection phone', 'change account change', 'protection phone', 'rate add preferred', 'line add', 'add line', 'address update', 'add port protection']
,
['update device', 'shipping confirmation', 'pre order', 'waybill number', 'order note ultra', 'receive shipping confirmation', 'note ultra pre', 'number order reference',
 'reference number order', 'new phone ship', 'reference number', 'ultra pre order', 'phone ship', 'order note', 'receive shipping', 'update device upgrade', 'order reference number',
  'pre order note', 'ultra pre', 'number order', 'order reference', 'courier tracking', 'receive shipment confirmation', 'tracking number device', 'receive shipment', 'shipment confirmation', 'confirmation email courier']
,
['line cancel', 'pay leave', 'cancel line', 'line cancel line', 'cancel account cancel', 'cancel wireless', 'cancel service', 'cancel line line', 'cancel wireless service', 'cancel plan',
 'phone cancel', 'cancel cell', 'account cancel', 'line end', 'account cancel account', 'cancel cell phone', 'cancel account', 'cancel line cancel', 'cancel cancel', 'close account']
,
['text say', 'charge overage', 'data limit', 'usage data', 'text say data', 'switch gb', 'gb gb', 'data usage', 'data usage data', 'message say', 'check data usage', 'receive message',
 'receive text', 'gb change', 'say data', 'usage rocket', 'turn data', 'usage data usage', 'overage charge', 'usage service', 'usage rocket hub', 'use data', 'data overage', 'data overage charge']
,
['account link account', 'account account number', 'display info work', 'work fix issue', 'display info', 'online account', 'postal code', 'able display', 'upgrade plan', 'work fix',
 'info work fix', 'info work', 'account number', 'know account number', 'able display info', 'link account', 'fix issue', 'account number account', 'account link']
,
['suppose payment', 'payment payment', 'arrangement payment', 'arrangement payment arrangement', 'arrangement past', 'payment arrangement', 'online banking', 'payment online banking', 'arrangement pay', 'pay month',
 'payment arrangement past', 'payment rogers', 'payment arrangement pay', 'service payment', 'credit card payment', 'able payment', 'account payment', 'visa debit', 'payment arrangement payment', 'payment online']
,
['apple watch apple', 'watch add', 'watch add plan', 'watch apple watch', 'apple watch online', 'purchase apple watch', 'order apple', 'ipad pro',
 'purchase apple', 'data cost', 'apple watch', 'watch online', 'apple watch add', 'order apple watch', 'watch apple', 'tablet data']
,
['old sim card', 'change sim', 'card sim', 'card activate', 'sim card new', 'sim card number', 'activate new', 'card old sim', 'lose phone new', 'change sim card', 'activate sim', 'sim card old',
 'card number', 'activate sim card', 'card new sim', 'old sim', 'phone lose', 'update new', 'card new', 'activate new sim', 'card old', 'sim card', 'new sim', 'sim card sim', 'sim card activate']
,
['change area', 'number change phone', 'number phone number', 'number change', 'change phone number', 'phone number', 'number change number', 'number area code',
 'change area code', 'phone number change', 'phone number phone', 'change phone', 'area code', 'phone number number', 'number area',]
,
['roam home', 'charge reverse', 'leave canada', 'roam home leave', 'reverse charge roam', 'long distance charge', 'roam home charge', 'charge roam home', 'home leave',
 'charge roam', 'notice charge', 'long distance', 'charge charge roam', 'reverse charge', 'home charge', 'distance charge']
,
['remove device protection', 'cancel premium device', 'protection online', 'contact customer care', 'device protection cancel', 'contact customer', 'remove device', 'device protection plan',
 'premium device', 'remove premium device', 'protection remove premium', 'protection remove', 'protection cancel', 'device protection remove', 'protection plan', 'device protection', 'cancel premium',
  'device protection online', 'premium device protection', 'remove premium', 'protection cancel premium', 'customer care']
,
['cell number', 'effective immediately transfer', 'global communications cell', 'connex global', 'cell number effective', 'communications cell number', 'number effective', 'number effective immediately',
 'immediately transfer responsibility', 'transfer account', 'authorize connex global', 'global communications', 'communications cell', 'immediately transfer', 'authorize account',
  'account transfer', 'connex global communications', 'account holder', 'transfer responsibility', 'authorize connex']
,
['extra usage charge', 'pay plan', 'plan pay', 'charge wireless', 'extra usage', 'text send', 'usage charge wireless']
,
['change email address', 'email say change', 'email say', 'exclusive offer', 'receive email']
,
[ 'error perform', 'number set', 'error perform request', 'set voicemail', 'perform request', 'voicemail set', 'say error perform', 'unknown error']
]


FIDO_BOLD_SEED_2 = [
['device upgrade', 'samsung ultra', 'update device upgrade', 'upgrade update', 'upgrade phone upgrade', 'iphone gb', 'upgrade update device', 'order iphone', 'order reference', 'iphone pro max', 'interested upgrade', 'upgrade iphone pro', 'phone upgrade phone', 'order reference number', 'phone upgrade', 'iphone xr', 'interested new phone', 'number order', 'phone ship', 'phone new phone', 'available upgrade', 'shipping confirmation', 'phone order', 'receive shipping', 'upgrade phone', 'iphone pro', 'tracking number', 'new phone', 'pick store']
,
['fido infinite', 'gb unlimited', 'offer offer', 'include gb', 'change wireless plan', 'special offer close', 'infinite plan', 'offer offer available', 'special offer offer', 'offer detail', 'offer fido', 'switch plan', 'detail window customer', 'bpo offer', 'plan include unlimited', 'close bpo offer', 'bpo offer detail', 'plan gb', 'offer detail', 'financing talk', 'share plan', 'wireless plan', 'enjoy offer fido', 'offer available enjoy']
,
['final bill', 'bill bill', 'bill wait', 'extra usage charge', 'extra usage', 'wait bill', 'usage charge wireless', 'pay late', 'apply offer discount', 'offer discount receive', 'discount receive', 'credit bill', 'bill text', 'charge wireless', 'month credit', 'month apply offer', 'work hospital call', 'apply offer', 'line worker', 'health care worker', 'extra charge', 'bill cycle']
,
['account wireless', 'port protection', 'number change', 'communications inc', 'cell number', 'change phone number', 'wireless account', 'add port', 'number change number', 'family plan', 'number area', 'responsibility account', 'new number', 'free service', 'phone number', 'port protection account', 'number change phone', 'transfer responsibility account', 'immediately transfer responsibility', 'number effective', 'transfer account']
,
['check phone lock', 'network unlock code', 'code check iphone', 'unlock code', 'device lock', 'phone unlock lock', 'lock fido', 'verify imei', 'unlock old samsung', 'lock unlock', 'lock fido', 'phone unlock old', 'unlock complete', 'unlocked unlock phone', 'lock unlock free', 'unlock lock', 'old samsung', 'unlock complete verify', 'account unlock iphone', 'iphone account unlock', 'old phone unlock', 'verify imei unlock', 'complete verify', 'unlock code check', 'phone unlock', 'code check', 'iphone unlock', 'order phone unlock']
,
['usage rocket hub', 'add gb', 'reach data', 'data plan', 'data month', 'month plus', 'data overage', 'check data', 'data receive', 'add data', 'find data', 'check data usage', 'usage service', 'turn data', 'text data', 'data gb', 'charge overage', 'speed pass', 'month gb', 'usage past']
,
['line cancel line', 'phone cancel', 'cancel wireless', 'cancel wireless service', 'account cancel account', 'line cancel', 'cancel line line', 'cancel account cancel', 'cancel service', 'cancel cell', 'line end', 'cancel line', 'cancel cell phone', 'cancel account', 'account cancel', 'cancel line cancel', 'cancel fido', 'time cancel']
,
['website change', 'sign fido', 'caller id', 'fido account', 'postal code', 'email email', 'account link', 'account email', 'get account', 'account account', 'number email', 'account number account', 'fido account account', 'find account', 'account link account', 'account number', 'phone number account', 'email bill', 'link account', 'number account number']
,
['payment payment', 'online banking', 'pay payment', 'payment new', 'payment arrangement', 'payment online banking', 'arrangement pay', 'payment bill', 'balance show', 'month pay', 'bill phone', 'visa debit', 'payment arrangement pay', 'payment online', 'payment arrangement past', 'past balance', 'account payment', 'pay month pay', 'payment arrangement payment', 'suppose payment', 'arrangement past', 'pay service', 'payment fido', 'payment month', 'arrangement payment']
,
['apple watch', 'data cost', 'watch online', 'purchase apple', 'watch add', 'order apple watch', 'watch add plan', 'apple watch online', 'purchase apple watch', 'apple watch add', 'plan tablet', 'financing plan', 'order apple']
,
['switch fido', 'exist plan', 'telus fido', 'exist phone', 'transfer line', 'fido switch', 'line fido', 'plan line', 'line plan', 'fido number', 'line phone', 'add line', 'share line', 'fill form', 'fido fido', 'non share']
,
['sim card new', 'card old sim', 'activate new', 'card account', 'old sim card', 'new sim card', 'sim card activate', 'number sim', 'current sim', 'new sim', 'change sim card', 'card old', 'sim card old', 'sim card phone', 'activate sim', 'activate sim card', 'sim number', 'sim card work', 'get new sim', 'card number sim', 'receive sim', 'old sim']
,
['roam home charge', 'bill roam home', 'distance charge', 'canada reverse', 'roam home', 'bill roam', 'home leave', 'leave canada', 'charge charge roam', 'home charge', 'charge roam home', 'canada reverse charge', 'border close', 'long distance', 'long distance charge', 'charge roam', 'roam home leave']
,
['protection add', 'remove device', 'device protection remove', 'protection remove', 'protection plan', 'device protection online', 'device protection', 'premium device protection', 'remove premium', 'cancel premium', 'premium device', 'protection remove premium', 'device protection plan', 'protection online', 'remove premium device', 'remove device protection', 'device protection cancel', 'cancel premium device', 'protection cancel', 'device protection add']
,
['cost phone', 'contract expire', 'device pay', 'plan buy', 'balance leave', 'phone contract', 'leave phone', 'contract end']
,
['phone year', 'unlimited gb', 'perform request', 'voicemail phone', 'iphone visual', 'get disconnected', 'send text', 'data infinite', 'error perform', 'iphone visual voicemail', 'phone work', 'unknown error', 'work set', 'set voicemail', 'error perform request']
,
['exclusive offer account', 'receive email', 'offer account', 'new promotional']
]


CBL_BOLD_SEED_2 = [
['internet promo', 'set expire', 'come end', 'expire month', 'promotion expire', 'promotion internet', 'offer available', 'offer end offer', 'internet promotion end', 'internet plan expire',
 'internet promotion expire', 'plan expire', 'offer deal', 'loyal customer', 'promotion expire soon', 'know promotion', 'internet promo end', 'internet promotion', 'price internet',
  'soon offer', 'expire soon offer', 'new promotion', 'promo end']
,
['charge month', 'charge late payment', 'late payment charge', 'high month', 'charge late', 'month home', 'pay time', 'cycle end', 'late payment', 'new monthly', 'billing billing', 'say billing', 'bill bill', 'bill increase',
'bill go', 'monthly bill', 'increase bill', 'internet bill', 'account balance', 'bill high', 'billing issue',
'current bill', 'overage charge', 'billing inquiry']
,
['service cancel', 'question cancel', 'internet service cancel', 'cancel internet cancel', 'cancel service cancel', 'anymore cancel', 'cancel internet', 'phone cancel', 'cancel cancel', 'cancel cable',
'cancel service', 'cancel home internet', 'cancel rogers', 'service end month', 'service cancel internet', 'service cancel service', 'close account', 'cancel subscription', 'cancel account',
'cancel internet service', 'cancel tv', 'cancel home', 'cancel home phone', 'internet cancel internet']
,
['option account', 'deal internet', 'plan home internet', 'internet ignite', 'cost upgrade', 'change internet plan', 'change internet package', 'question upgrade', 'change plan change', 'internet unlimited',
 'internet plan change', 'data usage', 'new plan', 'internet time', 'roger internet', 'internet deal', 'upgrade internet speed', 'internet package', 'plan home', 'home internet plan', 'internet internet', 'home internet']
,
['rogers online', 'password rogers', 'telecommunication service view', 'message able', 'display info', 'online message', 'say able display', 'account number register', 'manage company telecommunication',
 'work fix', 'online message able', 'telecommunication service', 'manage company', 'reset voicemail', 'fix able display', 'service view', 'number register', 'password rogers online', 'company telecommunication',
  'able display', 'fix issue', 'fix able', 'reset voicemail password', 'rogers online message']
,
['tv internet phone', 'phone tv', 'tv bundle', 'bundle tv internet', 'ignite bundle', 'change flex', 'ignite premier', 'internet home phone', 'ignite popular', 'ignite popular bundle',
 'bundle ignite', 'popular bundle', 'phone bundle', 'home phone tv', 'channel flex', 'bundle tv', 'channel flex channel', 'change flex channel', 'internet phone', 'ignite tv bundle']
,
['modem return', 'modem send', 'modem tell', 'charge return', 'send return', 'return cable box', 'tell send', 'return label', 'store open', 'receive label', 'equipment return', 'wait return', 'return cable',
 'equipment equipment', 'box return', 'label rogers', 'internet modem', 'return old', 'confirm receive', 'shipping label', 'service address', 'modem store', 'return modem', 'return equipment', 'label rogers send']
,
['new modem work', 'issue time', 'status provide', 'box status', 'serial number check', 'provide serial',
 'number check', 'provide serial number', 'speed slow', 'work internet', 'area internet', 'modem work',
  'internet work internet', 'internet connection', 'provide serial', 'phone work', 'internet issue']
,
['come way', 'receive text', 'come rogers', 'reschedule appointment', 'number tell', 'technician come', 'install internet', 'technician install', 'cancel installation', 'installation date']
,
['add channel add', 'theme pack', 'crave tv', 'tv rogers', 'channel tv', 'nfl ticket', 'channel tv package', 'add channel', 'channel add channel', 'rogers nhl']
,
['package change package', 'tv package popular', 'package package', 'change package', 'package popular']
,
['charge pay', 'payment payment', 'credit card info', 'pay internet', 'card info', 'payment online', 'change payment', 'pay bill',
 'payment arrangement', 'internet pay']
,
['number change', 'number link', 'set myroger account', 'account change', 'number end', 'place order', 'account number set', 'number account', 'old account', 'line new', 'change phone', 'number set',
 'receive service', 'link account', 'line change', 'change service', 'internet usage', 'rogers account']
,
['standard long distance', 'distance charge', 'distance plan', 'long distance phone', 'distance phone',
 'long distance plan', 'long distance', 'long distance charge', 'home phone',
  'phone plan', 'standard long']
,
['service area', 'transfer service', 'new address', 'new address new', 'current address', 'address new', 'new place',
 'address new address', 'service new address', 'service hold', 'transfer service new']
,
['safe sign internet', 'safe sign', 'sign internet', 'email state', 'email receive email', 'email rogers', 'sign internet service', 'service limited', 'stay safe sign', 'automatically apply']
]


WIR_BOLD_SEED_2 = [
['plan include unlimited', 'plan gb', 'unlimited canada', 'unlimited data plan', 'change wireless plan', 'gb plan', 'rogers infinite', 'infinite plan', 'unlimited canadawide', 'employee discount',
 'gb unlimited', 'plan plan', 'share plan', 'unlimited canadawide send', 'change plan plan', 'change wireless', 'canada plan', 'unlimited data', 'rogers infinite plan', 'new plan', 'change service', 'rogers offer',
  'canadawide send', 'data plan month', 'canadawide send receive', 'plan include', 'include unlimited']
,
['charge credit', 'activation fee', 'fee waive', 'tell pay', 'health care', 'healthcare worker', 'health care worker',
 'month credit', 'start billing', 'credit apply', 'month free', 'payment charge', 'receive month', 'charge late', 'late payment', 'fee suppose waive', 'pay late', 'credit month', 'monthly bill', 'bill high', 'question bill',
  'month apply', 'pay late payment', 'line worker', 'offer month', 'bill balance', 'billing billing', 'bill bill', 'bill increase', 'bill go', 'monthly bill', 'increase bill', 'bill high', 'billing issue']
,
['samsung note', 'upgrade phone upgrade', 'samsung ultra', 'upgrade phone', 'pro max', 'phone order', 'phone new phone', 'iphone pro', 'available upgrade', 'upgrade upgrade', 'new iphone',
 'gb upgrade', 'upgrade samsung', 'upgrade current', 'phone contract', 'interested new phone', 'break phone', 'phone upgrade phone', 'interested new', 'device balance', 'contract end', 'contract expire', 'pay device',
  'upgrade device', 'phone iphone', 'phone interested', 'upgrade iphone']
,
['network unlock code', 'unlock code', 'iphone account unlock', 'unlock phone', 'old phone unlock', 'unlock iphone chat', 'phone lock require', 'unlock old phone',
 'disconnect unlock complete', 'unlock complete verify', 'speak unlock iphone', 'account unlock iphone', 'unlock complete', 'chat disconnect unlock', 'old samsung', 'upfront edge',
  'unlock iphone account', 'iphone account', 'unlock old samsung', 'unlock old', 'code check check', 'iphone account account', 'verify imei', 'network unlock']
,
['new address', 'protection line account', 'iphone visual voicemail', 'port protection', 'port protection line', 'port protection account', 'change credit card', 'account add',
 'visual voicemail add', 'voicemail add', 'add port', 'change credit', 'port protection phone', 'change account change', 'protection phone', 'rate add preferred', 'line add', 'add line', 'address update', 'add port protection']
,
['update device', 'shipping confirmation', 'pre order', 'waybill number', 'order note ultra', 'receive shipping confirmation', 'note ultra pre', 'number order reference',
 'reference number order', 'new phone ship', 'reference number', 'ultra pre order', 'phone ship', 'order note', 'receive shipping', 'update device upgrade', 'order reference number',
  'pre order note', 'ultra pre', 'number order', 'order reference', 'courier tracking', 'receive shipment confirmation', 'tracking number device', 'receive shipment', 'shipment confirmation', 'confirmation email courier']
,
['line cancel', 'pay leave', 'cancel line', 'line cancel line', 'cancel account cancel', 'cancel wireless', 'cancel service', 'cancel line line', 'cancel wireless service', 'cancel plan',
 'phone cancel', 'cancel cell', 'account cancel', 'line end', 'account cancel account', 'cancel cell phone', 'cancel account', 'cancel line cancel', 'cancel cancel', 'close account']
,
['text say', 'charge overage', 'data limit', 'usage data', 'text say data', 'switch gb', 'gb gb', 'data usage', 'data usage data', 'message say', 'check data usage', 'receive message',
 'receive text', 'gb change', 'say data', 'usage rocket', 'turn data', 'usage data usage', 'overage charge', 'usage service', 'usage rocket hub', 'use data', 'data overage', 'data overage charge']
,
['account link account', 'account account number', 'display info work', 'work fix issue', 'display info', 'online account', 'postal code', 'able display', 'upgrade plan', 'work fix',
 'info work fix', 'info work', 'account number', 'know account number', 'able display info', 'link account', 'fix issue', 'account number account', 'account link']
,
['suppose payment', 'payment payment', 'arrangement payment', 'arrangement payment arrangement', 'arrangement past', 'payment arrangement', 'online banking', 'payment online banking', 'arrangement pay', 'pay month',
 'payment arrangement past', 'payment rogers', 'payment arrangement pay', 'service payment', 'credit card payment', 'able payment', 'account payment', 'visa debit', 'payment arrangement payment', 'payment online']
,
['apple watch apple', 'watch add', 'watch add plan', 'watch apple watch', 'apple watch online', 'purchase apple watch', 'order apple', 'ipad pro',
 'purchase apple', 'data cost', 'apple watch', 'watch online', 'apple watch add', 'order apple watch', 'watch apple', 'tablet data']
,
['old sim card', 'change sim', 'card sim', 'card activate', 'sim card new', 'sim card number', 'activate new', 'card old sim', 'lose phone new', 'change sim card', 'activate sim', 'sim card old',
 'card number', 'activate sim card', 'card new sim', 'old sim', 'phone lose', 'update new', 'card new', 'activate new sim', 'card old', 'sim card', 'new sim', 'sim card sim', 'sim card activate']
,
['change area', 'number change phone', 'number phone number', 'number change', 'change phone number', 'phone number', 'number change number', 'number area code',
 'change area code', 'phone number change', 'phone number phone', 'change phone', 'area code', 'phone number number', 'number area',]
,
['roam home', 'charge reverse', 'leave canada', 'roam home leave', 'reverse charge roam', 'long distance charge', 'roam home charge', 'charge roam home', 'home leave',
 'charge roam', 'notice charge', 'long distance', 'charge charge roam', 'reverse charge', 'home charge', 'distance charge']
,
['remove device protection', 'cancel premium device', 'protection online', 'contact customer care', 'device protection cancel', 'contact customer', 'remove device', 'device protection plan',
 'premium device', 'remove premium device', 'protection remove premium', 'protection remove', 'protection cancel', 'device protection remove', 'protection plan', 'device protection', 'cancel premium',
  'device protection online', 'premium device protection', 'remove premium', 'protection cancel premium', 'customer care']
,
['cell number', 'effective immediately transfer', 'global communications cell', 'connex global', 'cell number effective', 'communications cell number', 'number effective', 'number effective immediately',
 'immediately transfer responsibility', 'transfer account', 'authorize connex global', 'global communications', 'communications cell', 'immediately transfer', 'authorize account',
  'account transfer', 'connex global communications', 'account holder', 'transfer responsibility', 'authorize connex']
,
['extra usage charge', 'pay plan', 'plan pay', 'charge wireless', 'extra usage', 'text send', 'usage charge wireless']
,
['change email address', 'email say change', 'email say', 'exclusive offer', 'receive email']
,
[ 'error perform', 'number set', 'error perform request', 'set voicemail', 'perform request', 'voicemail set', 'say error perform', 'unknown error']
]


GLDA_SEEDS = [

 ['price plan', 'plan price', 'infinite data', 'unlimited data', 'infinite', 'discount', 'share', 'offer', 'canadawide',
  'data', 'plan', 'unlimited', 'cost', 'price'],  ######Price Plan Inquiry

 ['set', 'sim', 'active', 'activate', 'card', 'add', 'new', 'account', 'line', 'activation', 'register'],
 ######SIM activation

 ['device protection', 'break phone', 'phone break', 'crack phone', 'phone crack', 'device', 'protection',
  'replace phone',
  'repair', 'break', 'broken', 'water', 'problematic', 'faulty', 'repair phone', 'repair device', 'replace device',
  'device crack', 'phone crack', 'cracked screen', 'crack phone', 'device break', 'device damage',
  'damage', 'damaged', 'crack', 'cracked', 'slip', 'drop', 'brightstar', 'screen', 'replacement'],
 ######device protection
 #####not sure whether include wefix  ...'device', 'protection',

 ['credit', 'fee', 'waive', 'pay', 'extra fee', 'waive fee', 'billing cycle', 'billing', 'free', 'payment', 'late',
  'extra',
  'waive', 'invoice', 'cycle', 'balance', 'billing'],  ######payment inquiery

 ['roam', 'canada', 'long', 'distance', 'long distance',
  'international', 'country'],  ####roaming

 ['shipping confirmation', 'order confirmation', 'order number', 'order reference', 'shipping', 'confirmation', 'order',
  'reference', 'ship', 'shipment', 'store', 'deliver', 'delivery'],  ####Order Inquiry

 ['unlock', 'lock', 'samsung', 'upfront', 'edge']  ####Unlock Device

]

# COMMAND ----------

import numpy as np
seed_list=[]
for seed_name in [GLDA_SEEDS,WIR_BOLD_SEED_1,WIR_BOLD_SEED_2,CBL_BOLD_SEED_1,CBL_BOLD_SEED_2,FIDO_BOLD_SEED_1,FIDO_BOLD_SEED_2]:
  for phraselist in seed_name:
    for phrase in phraselist:
      seed_list.append(phrase.split(' '))
def flatten(xss):
    return [x for xs in xss for x in xs]
seed_list=list(set(flatten(seed_list)))
print(seed_list)#shipping immediately interested tracking financing

# COMMAND ----------

FINAL_INTERNAL_STOPWORDS_ALIGN=list(set(INTERNAL_STOPWORDS_ALIGN)-set(seed_list))

# COMMAND ----------

VERINT_STOPWORDS = ['okay', 'believe', 'june', 'to', 'use', 'happen', 'without', 'dont', 'yet', 'blah',
                    'morning', 'thanks', 'little', 'please', 'perfect', 'sorry', 'think', 'ca', 'blah blah',
                    'guy', 'else', 'call', 'hey', 'however', 'thats', 'thx', 'say', 'every', 'try', 'blah blah blah',
                    'sure', 'come', 'uh', 'well', 'seem', 'yeah', 'know', 'fine', 'might', 'hello', 'password', 'passwords',
                    'want', 'tell', 'ok', 'since', 'cant', 'take', 'sir', 'two', 'maybe', 'im', 'great', 'hold',
                    'oh', 'one', 'chat', 'already', 'nope', 'thank', 'someone', 'roger', 'today', 'give',
                    'good', 'hi', 'bye', 'could', 'also', 'would', 'look', 'find', 'may', 'show', 'alright',
                    'wonder', 'still', 'need', 'actually', 'mean', 'ago', 'rogers', 'go', 'yes',
                    'right', 'though', 'get', 'wait', 'etc', 'see', 'let', 'even', 'august', 'july', 'thing',
                    'lol', 'like', 'um', 'stuff', 'pci', 'com', 'dot', 'yahoo', 'underscore', 'date', 'birth',
                   'postal', 'code', 'number', 'email', 'dollar', 'cent', 'buck', 'gmail', 'hotmail',"leave", "message", 
                   "tone", "leave", "hang", "press", "pound", "option", "est", "que","pas","vous","euh","oui","pour","moi","parce",
                   "mais", "people" , "est" , "year",  "company", "kind" , "point" , "plus" , "card" , "way" , "person" , "non" , 
                   "big" , "letter" , "pour" , "happy" , "start" , "live" , "door" , "money", "best" , "car" , "family" ,"dollars" , 
                   "school" , "son" , "building" , "absolutely" ,  "god" , "everybody","husband", "wife", "mom", "dad", "black",
                   "gonna", "son", "daughter", "white", "guys", "bien", "mois", "puis", "vai", "cinq", "bye", "goodbye", "blue",
                   "color", "anymore", "worry", "somebody"]
VERINT_FINAL_STOPWORDS=list(set(VERINT_STOPWORDS)-set(seed_list))
print(VERINT_FINAL_STOPWORDS)
print(len(VERINT_STOPWORDS))
print(len(VERINT_FINAL_STOPWORDS))
print(set(VERINT_STOPWORDS)&set(seed_list))

# COMMAND ----------

ENGLISH_STOPWORDS = ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't", 'as', 
                     'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', "can't", 'cannot', 
                     'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 
                     'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll", 
                     "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his', 'how', "how's", 'i', "i'd", "i'll", 
                     "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', "let's", 'me', 'more', 'most',
                     "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 
                     'ours', 'ourselves', 'out', 'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', 
                     "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 
                     'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 
                     'to', 'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', 
                     "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 'whom', 'why', 
                     "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 
                     'yourself', 'yourselves']
ENGLISH_FINAL_STOPWORDS=list(set(ENGLISH_STOPWORDS)-set(seed_list))
print(ENGLISH_FINAL_STOPWORDS)
print(len(ENGLISH_STOPWORDS))
print(len(ENGLISH_FINAL_STOPWORDS))
print(set(ENGLISH_STOPWORDS)&set(seed_list))

# COMMAND ----------

BOLDCHAT_STOPWORDS = ['week', 'thank', 'thanks', 'ask', 'ago', 'im', 'sure', 'sound', 'mean', 'lot',
                      'look', 'ok', 'okay', 'thats', 'yes', 'want', 'couple', 'correct', 'use', 'nice',
                      'good', 'day', 'let', 'great', 'fine', 'hi', 'hope', 'already', 'also', 'able',
                      'could', 'else', 'etc', 'guy', 'hello', 'hey', 'however', 'lol', 'maybe',
                      'might', 'morning', 'nope', 'oh', 'perfect', 'please', 'right', 'since', 'someone',
                      'sorry', 'still', 'though', 'thx', 'well', 'today', 'wonder', 'would', 'yeah',
                      'yet', 'even', 'believe', 'think', 'wish', 'mind', 'plz', 'glad', 'say',
                      'possible', 'sept', 'be', 'i', 'the', 'a', 'will', 'mo', 'instead', 'pls', 'go',
                      'of', 'the', 'that', 'to', 'do', 'for', 'if', 'need', 'tomorrow', 'simply',
                     'in', 'but', 'or', 'and', 'about', 'just', 'have', 'an', 'on', 'know', 'not',
                     'yesterday', 'today', 'try', 'google', 'research', 'appreciate', 'help', 'like',
                     'oct', 'aug', 'jun', 'july', 'nov', 'dec', 'stay', 'safe', 'tell', 'bye',
                     'speak', 'chat']
BOLDCHAT_FINAL_STOPWORDS=list(set(BOLDCHAT_STOPWORDS)-set(seed_list))
print(BOLDCHAT_FINAL_STOPWORDS)
print(len(BOLDCHAT_STOPWORDS))
print(len(BOLDCHAT_FINAL_STOPWORDS))
print(set(BOLDCHAT_STOPWORDS)&set(seed_list))

# COMMAND ----------

print(FINAL_INTERNAL_STOPWORDS_ALIGN)

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
        .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.0.1") \
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
#wir_ser_spacy = boldchat_etl.wir_ser_etl_spacy()
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

# MAGIC %md
# MAGIC below for verint

# COMMAND ----------

import logging
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from sparknlp.base import *
from sparknlp.annotator import *
from datetime import datetime
from rogers.cd.assets import ALL_VERINT_STOPWORDS
import spacy
import en_core_web_md

class VerintETL:
    """
    This class covers the required functionalities to perform
    primilinary ETL jobs on cbu_rog_conversation_sumfct
    to prepare an integraded dateset for the Normalizer Pipeline.
    """

    sumfct_keep_cols = [
                        'SPEECH_ID_VERINT',
                        'TEXT_CUSTOMER_FULL',
                        'TEXT_AGENT_FULL',
                        'TEXT_OVERLAP',
                        'TEXT_ALL',
                        'CONNECTION_ID',
                        'RECEIVING_SKILL',
                        'CONVERSATION_DATE',
                        'Year',
                        'Month',
                        'CTN',
                        'CUSTOMER_ID'
                        ]
    # interaction_id corrupted
    # sumfct_keep_cols suggested ['TEXT_CUSTOMER_FULL','TEXT_AGENT_FULL','TEXT_ALL','CUSTOMER_ID','CTN',
    #      'SPEECH_ID_VERINT', 'CONNECTION_ID', 'RECEIVING_SKILL', 'CONVERSATION_DATE','Year','Month']
    def __init__(self, verint_sumfct_df):
        """
        :param verint_df: Spark DataFrame containing raw cbu_rog_conversation_sumfct for a given time period
        """
        self.logger = logging.getLogger(__name__)
        verint_sumfct_df = verint_sumfct_df.distinct()
        self.verint_sumfct_df = verint_sumfct_df
        # self.household_df = household_df

        # load data later than set date
        # last_load = "2021-03-10"
        #self.verint_df = self.__last_load_input_data(self.verint_sumfct_df, last_load)
        #self.logger.info("Number of verint records: {}".format(self.verint_df.count()))
        # filter for only consumer vqs
        #self.verint_df = self.__filter_input_data(self.verint_df, self.consumer_vq_df)
        #self.logger.info("Number of verint records: {}".format(self.verint_df.count()))

        # keep necessary columns only
        self.verint_df = self.verint_sumfct_df.select(VerintETL.sumfct_keep_cols)
        print("count rows: {}".format(self.verint_df.count()))
        self.verint_df = self.verint_df.distinct()
        
        # self.merge_df = self.__merge_df(self.verint_df, self.household_df)
        
        # self.merge_df = self.merge_df.distinct()
        print("distinct count rows: {}".format(self.verint_df.count()))
        print("full consumer rows")
        self.logger.info("Number of verint records: {}".format(self.verint_df.count()))
        try:
            assert (self.verint_df.count() > 0)
        except AssertionError:
            self.logger.error("The joined Verint dataframe is empty.")

    def __last_load_input_data(self, verint_sumfct_df, last_load):
        """
        Filtering data later than set date
        :return: DF that loaded later than set date
        """

        sum_date_counts = verint_sumfct_df.groupBy('record_insert_dt').count().sort('record_insert_dt').collect()
        sum_date_dict = {}

        for row in sum_date_counts:
            sum_date_dict[row['record_insert_dt']] = row['count']
        verint_sumfct_df = verint_sumfct_df.where(verint_sumfct_df.record_insert_dt > last_load)
        return verint_sumfct_df


    def __filter_input_data(self, verint_sumfct_df, consumer_vq_df):
        """
        Filtering for only consumer vqs
        :return: joined verint sumfct DF
        """
        # join condition
        print("join")
        join_cond = [consumer_vq_df.VQ == verint_sumfct_df.receiving_skill]
        return verint_sumfct_df.join(consumer_vq_df, join_cond, 'inner')
    

    def __get_batch_start_date_and_end_date(self, verint_df):
        print("new data rows")
        print(verint_df.count())
        if verint_df.count() == 0:
            sys.exit('No new data loaded')
        load_date_counts = verint_df.groupBy('record_insert_dt').count().sort('record_insert_dt').collect()
        load_date_dict = {}
        for row in load_date_counts:
            load_date_dict[row['record_insert_dt']] = row['count']
        new_last_load = sorted(load_date_dict.keys())[-1]
        new_first_load = sorted(load_date_dict.keys())[0]
        print(f'load date range from {new_first_load} to {new_last_load}')
        new_last_load = new_last_load.replace("-", "")
        new_first_load = new_first_load.replace("-", "")
        return (new_last_load, new_first_load)

    ############################
    #### Spark Transformers ####
    ############################

    def __filter_by_receiving_skill(self, receiving_skill):
        """
            Transformer function for filtering the verint data based on receiving_skill (cabel, wireless, etc.)
        """
        print("cable skill")
        def transform(verint_df):
            return verint_df.where("receiving_skill LIKE '%_{}_%'".format(receiving_skill))
        return transform


    def __wireless_cable_vqs(self, verint_df):
        """
        Get wireless and vqs
        :return: filtered verint DF
        """
        print("wireless or cable vqs")
        return verint_df.where(
            "receiving_skill LIKE 'ROG_EN_%' AND (receiving_skill LIKE '%_WIR%' OR receiving_skill LIKE '%_CBL%')")

    def __wireless_cable(self, cbl, wir):
        def cbl_wir(verint_df):
            return verint_df.where(
                "receiving_skill LIKE '%_{0}%' OR receiving_skill LIKE '%_{1}%'".format(cbl, wir))
        return cbl_wir

      
    def __msg_not_empty_spark(self, token_col):
        "Removing all the records with no value for the customer message (empty messages)" #91
        def curry(verint_df):
            msg_not_empty_udf = udf(lambda msg: len(msg) > 1, BooleanType())
            return verint_df.where(msg_not_empty_udf(verint_df[token_col])).drop(token_col)
        return curry
      
    def __msg_not_empty(self, msg_col):
        "Removing all the records with no value for the customer message (empty messages)" 
        def curry(verint_df):
            msg_not_empty_udf = udf(lambda msg: len(msg.strip()) > 0 , BooleanType())
            return verint_df.where(msg_not_empty_udf(verint_df[msg_col]))
        return curry
      
    def __extract_cus_msg_spacy(self, verint_df):
        print("extract customer msg")
        extract_cus_msg_udf = udf(VerintETL.extract_cus_msg_spacy, StringType())
        return verint_df.withColumn("CLEAN_TEXT", extract_cus_msg_udf(verint_df['TEXT_CUSTOMER_FULL']))

    def __extract_cus_msg_spark(self, verint_df): 
        documentAssembler = DocumentAssembler() \
            .setInputCol("TEXT_CUSTOMER_FULL") \
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
      
        normalizer1 = Normalizer() \
            .setInputCols(["token"]) \
            .setOutputCol("nonDigitTokens") \
            .setLowercase(True)\
            .setCleanupPatterns(["""[0-9]"""])
 
        lemmatizer = LemmatizerModel.pretrained("lemma_spacylookup","en") \
            .setInputCols(["nonDigitTokens"]) \
            .setOutputCol("lemma")
  
        stopwords_cleaner = StopWordsCleaner() \
            .setInputCols(["lemma"]) \
            .setOutputCol("cleanTokens1")\
              .setStopWords(list(ALL_VERINT_STOPWORDS))
        #91
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
            .setOutputCols("CLEAN_TEXT") \
            .setOutputAsArray(False)\
            .setCleanAnnotations(False)\
            .setAnnotationSplitSymbol(" ")

        nlp_pipeline = Pipeline(
            stages=
            [documentAssembler, documentNormalizer1, documentNormalizer2,documentNormalizer3, documentNormalizer4,  tokenizer, normalizer1, lemmatizer,  stopwords_cleaner, stopWords, normalizer2, finisher]
        )
        verint_df_col=list(verint_df.columns)
        verint_df_col.append("CLEAN_TEXT")
        verint_df_col.append("onlyAlphaTokens")
        print(verint_df_col)
        return nlp_pipeline\
            .fit(verint_df)\
            .transform(verint_df).select(verint_df_col)      
    # def __extract_cus_msg(self, verint_df):
    #     documentAssembler = DocumentAssembler() \
    #         .setInputCol("text_customer_full") \
    #         .setOutputCol("document")

    #     tokenizer = Tokenizer() \
    #         .setInputCols(["document"]) \
    #         .setOutputCol("token")

    #     normalizer = Normalizer() \
    #         .setInputCols(["token"]) \
    #         .setOutputCol("normalized") \
    #         .setLowercase(True)

    #     lemmatizer = LemmatizerModel().pretrained()\
    #         .setInputCols(["normalized"]) \
    #         .setOutputCol("lemma")

    #     stopwords_cleaner = StopWordsCleaner() \
    #         .setInputCols(["lemma"]) \
    #         .setOutputCol("cleanTokens")\
    #         .setStopWords(list(ALL_VERINT_STOPWORDS))

    #     finisher = Finisher() \
    #         .setInputCols(["cleanTokens"]) \
    #         .setOutputCols("clean_text") \
    #         .setOutputAsArray(False) \
    #         .setCleanAnnotations(True)\
    #         .setAnnotationSplitSymbol(" ")

    #     nlp_pipeline = Pipeline(
    #         stages=
    #         [documentAssembler, tokenizer, normalizer, lemmatizer, stopwords_cleaner, finisher]
    #     )

    #     return nlp_pipeline\
    #         .fit(verint_df)\
    #         .transform(verint_df)

    def __drop_for_extracting_cus_msg(self, verint_df):
        print("drop for extracting customer msg")
        return verint_df.drop('TEXT_AGENT_FULL', 'TEXT_OVERLAP', 'TEXT_UNKNOWN')

    def __text_df(self, verint_df):
        return verint_df.where(f.length('CLEAN_TEXT') > 0)


    def __no_text_df(self, verint_df):
        return verint_df.where(f.length('CLEAN_TEXT') == 0)

    def __competitor_mention(self, verint_df):
        print("competitor mention")
        comp_udf = udf(VerintETL.competitor_mention, StringType())
        return verint_df.withColumn("COMPETITOR_MENTION", comp_udf(verint_df['CLEAN_TEXT']))
    
    def __product_mention(self, verint_df):
        print("product mention")
        comp_udf = udf(VerintETL.product_mention, StringType())
        return verint_df.withColumn("PRODUCT_MENTION", comp_udf(verint_df['CLEAN_TEXT']))

    def __rogers_fido_mention(self, verint_df):
        print("rogers fido mention")
        comp_udf = udf(VerintETL.rogers_fido_mention, StringType())
        return verint_df.withColumn("ROGERS_FIDO_MENTION", comp_udf(verint_df['CLEAN_TEXT']))


    ##########################
    ### Cable Service ETL ####
    ##########################

    def get_verint_df(self):
        return self.verint_df
    
    # def get_household_df(self):
    #     return self.household_df

    def en_rogers_fido_mention_etl(self, df):
        return df \
            .transform(self.__filter_by_receiving_skill('EN')) \
            .transform(self.__rogers_fido_mention)

    def en_product_mention_etl(self, df):
        return df \
            .transform(self.__filter_by_receiving_skill('EN')) \
            .transform(self.__product_mention) 

        # removed a line: .transform(self.__wireless_cable('CBL', 'WIR')) \

    def cbl_ser_etl_spacy(self):
        return self.verint_df \
            .transform(self.__extract_cus_msg_spacy) \
            .transform(self.__msg_not_empty("CLEAN_TEXT"))\
            .transform(self.__drop_for_extracting_cus_msg)\
            .transform(self.__competitor_mention)
            # .transform(self.__text_df) #removing empty values
            #.transform(self.__filter_by_receiving_skill('EN'))\
            # .transform(self.__drop_for_extracting_cus_msg)\
            
            #.transform(self.__wireless_cable_vqs) \
            #.transform(self.__filter_by_receiving_skill('CBL')) \
            
    def cbl_ser_etl_spark(self):
        return self.verint_df \
            .transform(self.__extract_cus_msg_spark) \
            .transform(self.__msg_not_empty_spark("onlyAlphaTokens"))\
            .transform(self.__drop_for_extracting_cus_msg)\
            .transform(self.__competitor_mention)
    ##########################
    ### Wireless Service ETL ####
    ##########################
    def wir_ser_etl_spacy(self):
        return self.verint_df \
            .transform(self.__wireless_cable_vqs) \
            .transform(self.__filter_by_receiving_skill('WIR')) \
            .transform(self.__extract_cus_msg_spacy) \
            .transform(self.__msg_not_empty("CLEAN_TEXT"))\
            .transform(self.__drop_for_extracting_cus_msg)
      
    def wir_ser_etl_spark(self):
        return self.verint_df \
            .transform(self.__wireless_cable_vqs) \
            .transform(self.__filter_by_receiving_skill('WIR')) \
            .transform(self.__extract_cus_msg_spark) \
            .transform(self.__msg_not_empty_spark("onlyAlphaTokens"))\
            .transform(self.__drop_for_extracting_cus_msg)
    ###########################
    #### helper functions ####
    ###########################
    def no_text_result(df):
        def add_na(ph):
            return 'N/A'
        add_na_udf = f.udf(add_na, StringType())
        df = df.withColumn('Top_1_topic', f.lit('Undefined'))
        df = df.withColumn('Top_2_topic', f.lit('Undefined'))
        df = df.withColumn('Top_1_prob', f.lit(1.0))
        df = df.withColumn('Top_2_prob', f.lit(1.0))
        df = df.withColumn('Top_1_keyword', add_na_udf(df.Top_1_topic))
        return df

    @staticmethod
    def competitor_mention(msg):
        comp_list = ['Bell','Telus','Cogeco','Freedom','Virgin','TekSavvy','Shaw','Public Mobile','Chatr','Koodo','Fonus'] # 'Fido', 'Rogers'
        men_list = []
        msg_list = msg.split()
        for comp in comp_list:
            if comp.lower() in msg_list:
                men_list.append(comp)
        return " | ".join(men_list)
    
    @staticmethod
    def rogers_fido_mention(msg):
        comp_list = ['Rogers','Fido']
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
                     "ignite bundles": "Ignite Bundles", "bundles": "Ignite Bundles", 
                     "smart home": "Smart Home", "smarthome": "Smart Home",
                     "wireless home internet": "Wireless Home Internet", "wireless home": "Wireless Home Internet",
                     "wireless internet": "Wireless Home Internet"
                     }
        for key, val in prod_dict.items():
            if key.lower() in msg:
                if val.lower() not in men_list:
                    men_list.append(val)
        return " | ".join(men_list)

    @staticmethod
    def extract_cus_msg_spacy(msg_text, stop_w=ALL_VERINT_STOPWORDS):

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
            lemma_list = [(tok.lemma_.lower()) for tok in doc if tok_filter(tok)]
            lemma_list = [(word.replace("datum", "data")) for word in lemma_list]
            #lemma_list=[(tok.lemma_.replace("datum","data")) for tok in doc if tok_filter(tok)]
            no_stop_list = [word for word in lemma_list if word not in stop_w and len(word) > 2]
            if len(no_stop_list) < 2:
                return ''
            return " ".join(no_stop_list).replace("e mail", "email").replace("caller d", "caller id")      
          
          
        spacy_model = get_spacy_model()
        chat_doc = spacy_model(msg_text)
        clean_msg = lemma_msg(chat_doc, stop_w)
        return clean_msg

# COMMAND ----------

# from rogers.cd.SparkOPs import DataBricks
import logging

logger = logging.getLogger("Household_DataOPs")

class Household:
    """
        This class wraps data operations for household
    """

    ### Table names & queries on Databricks ###
    # household_table = "APP_IBRO.IBRO_HOUSEHOLD_ACTIVITY"
    household_table= "ml_etl_output_data.IBRO_HOUSEHOLD_ACTIVITY_CHURN_SCHEDULED"
    nonchurn_table = "ml_etl_output_data.IBRO_HOUSEHOLD_ACTIVITY_NONCHURN_SCHEDULED"

    @staticmethod
    def load_data(spark_s = None, churn = True):
        """
        Loading ibro_household_activity data to spark dataframes
        :param condition: Optional condition to be passed to household_query e.g. "where []"
        :return ibro_household_activity dataframes respectively
        """

        # those columns can be used to filter our churned customers
        # find out CAN, treated as account_number
        # ARPA_OUT = -1 means deac, ARPA_OUT = 1 means winback
        if churn:
            household_query = "select * from {0}".format(Household.household_table)

        if not churn:
            household_query = "select * from {0}".format(Household.nonchurn_table)

        logger.info("reading ibro_household_activity data from Databricks.")
        logger.debug("running queries: {0}\n".format(household_query))
        return spark_s.sql(household_query) 

# COMMAND ----------

# from rogers.cd.SparkOPs import DataBricks
import logging


logger = logging.getLogger("Verint_DataOPs")


class Verint:
    """
        This class wraps data operations for verint
    """

    ### Table names & queries on Databricks ###
    verint_table = "VERINT.CBU_ROG_CONVERSATION_SUMFCT"
    booked_table = "VERINT.SESSIONS_BOOKED"
    

    @staticmethod
    def load_data(min_date = None, max_date = None, categories = [], instances = [], spark_s = None):
        """
        Loading cbu_rog_conversation_sumfct data to spark dataframes
        :param condition: Optional condition to be passed to verint_query e.g. "where []"
        :return cbu_rog_conversation_sumfct dataframes respectively
        """

        if instances != [] and categories != []:
            print('Categories and instances provided')
            #Category id can be used to filter our churned customers
            verint_query = "SELECT sumfct.*, booked.* FROM \
                (SELECT sid_key FROM VERINT.SESSIONS_CATEGORIES WHERE category_id in ('{0}') AND instance_id in ('{1}')) \
                as category \
                INNER JOIN \
                (SELECT CONCAT(unit_num, '0', channel_num) as speech_id, sid_key, (sess_duration/60) as session_duration, number_of_holds, (total_hold_time/60) as hold_time_minutes, p7_value as account_number, CAST(local_start_time as DATE) as local_start_date, local_start_time FROM {2}) \
                as booked ON booked.sid_key = category.sid_key \
                INNER JOIN \
                (SELECT *, YEAR(conversation_date) as Year, MONTH(conversation_date) as Month FROM {3} WHERE conversation_date >= '{4}') \
                as sumfct ON sumfct.speech_id_verint = booked.speech_id"\
                .format(("','".join(categories)),("','".join(instances)),Verint.booked_table,Verint.verint_table, min_date)

        elif instances == [] and categories != []:
            print('Categories provided')
            verint_query = "SELECT sumfct.*, booked.* FROM \
                (SELECT sid_key FROM VERINT.SESSIONS_CATEGORIES WHERE category_id in ('{0}')) \
                as category \
                INNER JOIN \
                (SELECT CONCAT(unit_num, '0', channel_num) as speech_id, sid_key, (sess_duration/60) as session_duration, number_of_holds, (total_hold_time/60) as hold_time_minutes, p7_value as account_number, CAST(local_start_time as DATE) as local_start_date, local_start_time FROM {1}) \
                as booked ON booked.sid_key = category.sid_key \
                INNER JOIN \
                (SELECT *, YEAR(conversation_date) as Year, MONTH(conversation_date) as Month FROM {2} WHERE conversation_date >= '{3}') \
                as sumfct ON sumfct.speech_id_verint = booked.speech_id"\
                .format(("','".join(categories)),Verint.booked_table,Verint.verint_table, min_date)

        elif instances != [] and categories == []:
            print('Instances provided')
            verint_query = "SELECT sumfct.*, booked.* FROM \
                (SELECT sid_key FROM VERINT.SESSIONS_CATEGORIES WHERE instance_id in ('{0}')) \
                as category \
                INNER JOIN \
                (SELECT CONCAT(unit_num, '0', channel_num) as speech_id, sid_key, (sess_duration/60) as session_duration, number_of_holds, (total_hold_time/60) as hold_time_minutes, p7_value as account_number, CAST(local_start_time as DATE) as local_start_date, local_start_time FROM {1}) \
                as booked ON booked.sid_key = category.sid_key \
                INNER JOIN \
                (SELECT *, YEAR(conversation_date) as Year, MONTH(conversation_date) as Month FROM {2} WHERE conversation_date >= '{3}') \
                as sumfct ON sumfct.speech_id_verint = booked.speech_id"\
                .format(("','".join(instances)),Verint.booked_table,Verint.verint_table, min_date)
       
        else:
            print('Only date provided')
            verint_query = "SELECT sumfct.*, booked.* FROM \
                (SELECT CONCAT(unit_num, '0', channel_num) as speech_id, sid_key, (sess_duration/60) as session_duration, number_of_holds, (total_hold_time/60) as hold_time_minutes, p7_value as account_number, CAST(local_start_time as DATE) as local_start_date, local_start_time FROM {0}) \
                as booked \
                INNER JOIN \
                (SELECT *, YEAR(conversation_date) as Year, MONTH(conversation_date) as Month FROM {1} WHERE conversation_date >= '{2}') \
                as sumfct ON sumfct.speech_id_verint = booked.speech_id"\
                .format(Verint.booked_table, Verint.verint_table, min_date)
        
        #consumer_vq_input = "dbfs:/FileStore/contacts_driver/consumer_vqs.parquet"

        #Payment Training
        #verint_query = "SELECT * FROM VERINT.CBU_ROG_CONVERSATION_SUMFCT as a INNER JOIN (SELECT LOWER(Conn_ID) as conn_id, L2 FROM ml_etl_output_data.verint_payments_sample WHERE L2 = 'Bill Inquiry') as b ON a.connection_id = b.conn_id"
        #Payment in Full Training
        #verint_query = "SELECT sumfct.* FROM (SELECT sid_key FROM VERINT.SESSIONS_CATEGORIES WHERE instance_id = '504111') as category INNER JOIN (SELECT CONCAT(unit_num, '0', channel_num) as speech_id, sid_key FROM VERINT.SESSIONS_BOOKED) as booked ON booked.sid_key = category.sid_key INNER JOIN VERINT.CBU_ROG_CONVERSATION_SUMFCT as sumfct ON sumfct.speech_id_verint = booked.speech_id INNER JOIN (SELECT LOWER(Conn_ID) as conn_id FROM ml_etl_output_data.verint_full_balance_sample) as sample ON sumfct.connection_id = sample.conn_id"
        #Competitive Intelligence Training
        #verint_query = "SELECT sumfct.* FROM (SELECT sid_key FROM VERINT.SESSIONS_CATEGORIES WHERE category_id in ('121000167', '125001150') AND instance_id in ('504121', '504125')) as category INNER JOIN (SELECT CONCAT(unit_num, '0', channel_num) as speech_id, sid_key FROM VERINT.SESSIONS_BOOKED) as booked ON booked.sid_key = category.sid_key INNER JOIN VERINT.CBU_ROG_CONVERSATION_SUMFCT as sumfct ON sumfct.speech_id_verint = booked.speech_id INNER JOIN (SELECT LOWER(ConnID) as conn_id FROM ml_etl_output_data.verint_competitive_mention_sample) as sample ON sumfct.connection_id = sample.conn_id"
        
        if max_date is not None:
            verint_query = "{0} where {1}".format(verint_query, "conversation_date < '{}'".format(max_date))
            
        
        # household table
        # household_query = "select * from {0}".format(Verint.household_table)

        logger.info("reading cbu_rog_conversation_sumfct data from Databricks.")
        logger.debug("running queries: {0}\n".format(verint_query))
        return spark_s.sql(verint_query) #, spark_s.sql(household_query)  #DataBricks.spark.read.format("parquet").load(consumer_vq_input)

# COMMAND ----------


import argparse
import os
import logging
from datetime import datetime, timedelta
from pyspark.sql.functions import *
import sparknlp

spark = sparknlp.start()

print("Spark NLP version", sparknlp.version())
print("Apache Spark version:", spark.version)

logger = logging.getLogger("Churn_ETL_test")


today = datetime.today()
min_date = (today - timedelta(days=3)).strftime('%Y-%m-%d')
max_date = (today - timedelta(days=2)).strftime('%Y-%m-%d')


# min_date <= conversation_date < max_date

# " ***** min_date should be read from a parameter storage on Azure, assigning manually for now **** "
#min_date = '2022-07-13'
#max_date = '2022-07-14'
print(min_date)
print(max_date)

###################################
## Creating a VerintETL Object ##
## to perform ETL jobs on the    ##
## input data.                   ##
###################################
spark_session = spark
verint= Verint.load_data(min_date = min_date, max_date = max_date, spark_s = spark_session) 
print(verint.count())
print(verint.columns)

household_df = Household.load_data(spark_s = spark_session, churn=True)
print("=======================household columns===============================")
print(household_df.columns)


# merge before etl transformation
# ibro_verint_df = verint.join(household_df, verint.account_number == household_df.ACCOUNT_NUMBER, 'inner')\
#     .drop(household_df.CUSTOMER_ID)\
#     .drop(household_df.CUSTOMER_COMPANY)\
#     .drop(household_df.CUSTOMER_ACCOUNT)\
#     .drop(verint.account_number)\
#     .drop(household_df.REPORT_DATE)
verint=verint.withColumnRenamed('customer_id','CUSTOMER_ID').withColumnRenamed('speech_id_verint','SPEECH_ID_VERINT')\
    .withColumnRenamed('text_customer_full','TEXT_CUSTOMER_FULL').withColumnRenamed('text_agent_full','TEXT_AGENT_FULL')\
    .withColumnRenamed('text_overlap','TEXT_OVERLAP').withColumnRenamed('text_all','TEXT_ALL')\
    .withColumnRenamed('ctn','CTN').withColumnRenamed('connection_id','CONNECTION_ID')\
    .withColumnRenamed('sid_key','SID_KEY').withColumnRenamed('conversation_date','CONVERSATION_DATE')\
    .withColumnRenamed('interaction_id','INTERACTION_ID').withColumnRenamed('text_unknown','TEXT_UNKNOWN')
ibro_verint_df = verint.join(household_df, verint.CUSTOMER_ID == household_df.HASH_LKP_ACCOUNT, 'inner')\
    .drop(household_df.CUSTOMER_ID)\
    .drop(household_df.CUSTOMER_COMPANY)\
    .drop(household_df.CUSTOMER_ACCOUNT)\
    .drop(verint.account_number)\
    .drop(household_df.REPORT_DATE)
print("=======================merged ibro and verint===============================")
print(ibro_verint_df.columns)
print("ibro_verint count rows: {}".format(ibro_verint_df.count()))


verint_etl = VerintETL(ibro_verint_df) 


verint_df = verint_etl.cbl_ser_etl_spark() # preprocess
final_df = verint_etl.en_rogers_fido_mention_etl(verint_df) 


print("======================preprocess===============================")
print(final_df.count())
#print("writing ETL output results to "+str(etl_output)+"...")
#os.makedirs(etl_output, exist_ok=True)
#final_df.coalesce(1).write.mode("overwrite").option("header", True).parquet(etl_output)

# COMMAND ----------

display(final_df)

# COMMAND ----------


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

# COMMAND ----------

def extract_cus_msg_spark(boldchat_df): 
        #比spacy少"datum，email,caller id"处理(ok),只有一个词时的处理(变空)(ok),去掉pronoun(ok)和每个词必须要有字母,符号如'+'要去掉(ok)以及基础停词的消除(ok)
        #spacy的pronoun不算在stopwords里，spark的pronoun被stopwords包含
        documentAssembler = DocumentAssembler() \
            .setInputCol("clean_text_spark") \
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
 
        lemmatizer = LemmatizerModel.pretrained("lemma_spacylookup","en") \
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
            [documentAssembler, documentNormalizer1, documentNormalizer2,documentNormalizer3, documentNormalizer4,  tokenizer, normalizer1, lemmatizer,  stopwords_cleaner, stopWords, normalizer2, finisher]
        )
        boldchat_df_col=list(boldchat_df.columns)
        boldchat_df_col.append("clean_text")
        boldchat_df_col.append("onlyAlphaTokens")
        print(boldchat_df_col)
        return nlp_pipeline\
            .fit(boldchat_df)\
            .transform(boldchat_df).select(boldchat_df_col)

# COMMAND ----------

sparktext=spark.sql('select * from hive_metastore.default.spark_text_v5')
sparktext=sparktext.withColumnRenamed('clean_text','clean_text_spark').withColumnRenamed('session_id','session_id_spark')

# COMMAND ----------

display(extract_cus_msg_spark(sparktext))

# COMMAND ----------


