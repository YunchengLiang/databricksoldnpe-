# Databricks notebook source
# MAGIC %md
# MAGIC must use new version spark pyspark==3.2.1, change cluster

# COMMAND ----------

# MAGIC %md
# MAGIC stopwords

# COMMAND ----------

ORD_NUM = [
'first',
'second',
'third',
'fourth',
'fifth',
'sixth',
'seventh',
'eighth',
'ninth',
'tenth',
'eleventh',
'twelfth',
'thirteenth',
'fourteenth',
'fifteenth',
'sixteenth',
'seventeenth',
'eighteenth',
'nineteenth',
'twentieth',
'thirtieth',
'twenty first',
'twenty second',
'twenty third',
'twenty fourth',
'twenty fifth',
'twenty sixth',
'twenty seventh',
'twenty eighth',
'twenty ninth',
'thirty first']

NUMBERS = [
'zero',
'one',
'two',
'three',
'four',
'five',
'six',
'seven',
'eight',
'nine',
'ten',
'eleven',
'twelve',
'thirteen',
'fourteen',
'fifteen',
'sixteen',
'seventeen',
'eighteen',
'nineteen',
'twenty',
'thirty',
'forty',
'fifty',
'sixty',
'seventy',
'eighty',
'ninety',
'hundred',
'thousand']

MONTH = [
'january',
'jan',
'february',
'feb',
'march',
'mar',
'april',
'apr',
'may',
'june',
'jun',
'july',
'jul',
'august',
'aug',
'september',
'sep',
'october',
'oct',
'november',
'nov',
'december',
'dec']

DAYS = [
'monday',
'mon',
'tuesday',
'tue',
'wednesday',
'wed',
'thursday',
'thu',
'friday',
'fri',
'saturday',
'sat',
'sunday',
'sun']

BOLDCHAT_FINAL_STOPWORDS=['still', 'for', 'sound', 'appreciate', 'could', 'please', 'about', 'july', 'want', 
'since', 'yesterday', 'though', 'hey', 'aug', 'hi', 'yeah', 'a', 'right', 'wish', 'great', 'nov', 
'simply', 'perfect', 'even', 'lot', 'might', 'morning', 'today', 'be', 'help', 'mo', 'someone', 'fine',
 'that', 'to', 'not', 'believe', 'ok', 'else', 'glad', 'hello', 'nope', 'sorry', 'google', 'thats', 
 'plz', 'guy', 'hope', 'possible', 'well', 'jun', 'yes', 'wonder', 'mind', 'research', 'lol', 'sept',
  'couple', 'already', 'pls', 'i', 'thx', 'but', 'the', 'try', 'im', 'instead', 'just', 'have', 'and',
 'or', 'thanks', 'week', 'let', 'like', 'also', 'in', 'if', 'think', 'oct', 'thank', 'need', 'will',
 'correct', 'maybe', 'however', 'of', 'oh', 'nice', 'okay', 'dec', 'do', 'on', 'an', 'ask', 'look',
 'would', 'bye', 'good', 'ago', 'tomorrow', 'day', 'mean', 'etc', 'sure', 'yet']

ALL_BOLDCHAT_STOPWORDS = set(ORD_NUM + NUMBERS + MONTH + DAYS + BOLDCHAT_FINAL_STOPWORDS)

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

FRENCH_STOPWORDS = ["a","abord","absolument","afin","ah","ai","aie","aient","aies","ailleurs","ainsi","ait","allaient",
                    "allo","allons","allô","alors","anterieur","anterieure","anterieures","apres","après","as","assez",
                    "attendu","au","aucun","aucune","aucuns","aujourd","aujourd'hui","aupres","auquel","aura","aurai",
                    "auraient","aurais","aurait","auras","aurez","auriez","aurions","aurons","auront","aussi","autant",
                    "autre","autrefois","autrement","autres","autrui","aux","auxquelles","auxquels","avaient","avais",
                    "avait","avant","avec","avez","aviez","avions","avoir","avons","ayant","ayez","ayons","b","bah",
                    "bas","basee","bat","beau","beaucoup","bien","bigre","bon","boum","bravo","brrr","c","car","ce",
                    "ceci","cela","celle","celle-ci","celle-là","celles","celles-ci","celles-là","celui","celui-ci",
                    "celui-là","celà","cent","cependant","certain","certaine","certaines","certains","certes","ces","cet",
                    "cette","ceux","ceux-ci","ceux-là","chacun","chacune","chaque","cher","chers","chez","chiche","chut","chère",
                    "chères","ci","cinq","cinquantaine","cinquante","cinquantième","cinquième","clac","clic","combien",
                    "comme","comment","comparable","comparables","compris","concernant","contre","couic","crac","d","da",
                    "dans","de","debout","dedans","dehors","deja","delà","depuis","dernier","derniere","derriere","derrière",
                    "des","desormais","desquelles","desquels","dessous","dessus","deux","deuxième","deuxièmement","devant",
                    "devers","devra","devrait","different","differentes","differents","différent","différente","différentes",
                    "différents","dire","directe","directement","dit","dite","dits","divers","diverse","diverses","dix","dix-huit",
                    "dix-neuf","dix-sept","dixième","doit","doivent","donc","dont","dos","douze","douzième","dring","droite",
                    "du","duquel","durant","dès","début","désormais","e","effet","egale","egalement","egales","eh","elle",
                    "elle-même","elles","elles-mêmes","en","encore","enfin","entre","envers","environ","es","essai","est",
                    "et","etant","etc","etre","eu","eue","eues","euh","eurent","eus","eusse","eussent","eusses","eussiez",
                    "eussions","eut","eux","eux-mêmes","exactement","excepté","extenso","exterieur","eûmes","eût","eûtes","f",
                    "fais","faisaient","faisant","fait","faites","façon","feront","fi","flac","floc","fois","font","force",
                    "furent","fus","fusse","fussent","fusses","fussiez","fussions","fut","fûmes","fût","fûtes","g","gens","h",
                    "ha","haut","hein","hem","hep","hi","ho","holà","hop","hormis","hors","hou","houp","hue","hui","huit",
                    "huitième","hum","hurrah","hé","hélas","i","ici","il","ils","importe","j","je","jusqu","jusque","juste",
                    "k","l","la","laisser","laquelle","las","le","lequel","les","lesquelles","lesquels","leur","leurs","longtemps",
                    "lors","lorsque","lui","lui-meme","lui-même","là","lès","m","ma","maint","maintenant","mais","malgre",
                    "malgré","maximale","me","meme","memes","merci","mes","mien","mienne","miennes","miens","mille","mince",
                    "mine","minimale","moi","moi-meme","moi-même","moindres","moins","mon","mot","moyennant","multiple",
                    "multiples","même","mêmes","n","na","naturel","naturelle","naturelles","ne","neanmoins","necessaire",
                    "necessairement","neuf","neuvième","ni","nombreuses","nombreux","nommés","non","nos","notamment","notre",
                    "nous","nous-mêmes","nouveau","nouveaux","nul","néanmoins","nôtre","nôtres","o","oh","ohé","ollé","olé",
                    "on","ont","onze","onzième","ore","ou","ouf","ouias","oust","ouste","outre","ouvert","ouverte","ouverts",
                    "o|","où","p","paf","pan","par","parce","parfois","parle","parlent","parler","parmi","parole","parseme",
                    "partant","particulier","particulière","particulièrement","pas","passé","pendant","pense","permet","personne",
                    "personnes","peu","peut","peuvent","peux","pff","pfft","pfut","pif","pire","pièce","plein","plouf",
                    "plupart","plus","plusieurs","plutôt","possessif","possessifs","possible","possibles","pouah","pour",
                    "pourquoi","pourrais","pourrait","pouvait","prealable","precisement","premier","première","premièrement",
                    "pres","probable","probante","procedant","proche","près","psitt","pu","puis","puisque","pur","pure","q",
                    "qu","quand","quant","quant-à-soi","quanta","quarante","quatorze","quatre","quatre-vingt","quatrième",
                    "quatrièmement","que","quel","quelconque","quelle","quelles","quelqu'un","quelque","quelques","quels",
                    "qui","quiconque","quinze","quoi","quoique","r","rare","rarement","rares","relative","relativement",
                    "remarquable","rend","rendre","restant","reste","restent","restrictif","retour","revoici","revoilà",
                    "rien","s","sa","sacrebleu","sait","sans","sapristi","sauf","se","sein","seize","selon","semblable",
                    "semblaient","semble","semblent","sent","sept","septième","sera","serai","seraient","serais","serait",
                    "seras","serez","seriez","serions","serons","seront","ses","seul","seule","seulement","si","sien","sienne",
                    "siennes","siens","sinon","six","sixième","soi","soi-même","soient","sois","soit","soixante","sommes",
                    "son","sont","sous","souvent","soyez","soyons","specifique","specifiques","speculatif","stop","strictement",
                    "subtiles","suffisant","suffisante","suffit","suis","suit","suivant","suivante","suivantes","suivants",
                    "suivre","sujet","superpose","sur","surtout","t","ta","tac","tandis","tant","tardive","te","tel","telle",
                    "tellement","telles","tels","tenant","tend","tenir","tente","tes","tic","tien","tienne","tiennes","tiens",
                    "toc","toi","toi-même","ton","touchant","toujours","tous","tout","toute","toutefois","toutes","treize",
                    "trente","tres","trois","troisième","troisièmement","trop","très","tsoin","tsouin","tu","té","u","un","une",
                    "unes","uniformement","unique","uniques","uns","v","va","vais","valeur","vas","vers","via","vif","vifs",
                    "vingt","vivat","vive","vives","vlan","voici","voie","voient","voilà","voire","vont","vos","votre","vous",
                    "vous-mêmes","vu","vé","vôtre","vôtres","w","x","y","z","zut","à","â","ça","ès","étaient","étais","était",
                    "étant","état","étiez","étions","été","étée","étées","étés","êtes","être","ô"]

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

VERINT_FINAL_STOPWORDS=['still', 'mom', 'actually', 'pour', 'pas', 'guys', 'little', 'could', 'please', 'july', 'want', 'point',
 'blah blah', 'tone', 'since', 'car', 'moi', 'though', 'hey', 'sir', 'hi', 'birth', 'vous', 'letter',
  'yeah', 'worry', 'big', 'underscore', 'right', 'great', 'may', 'cant', 'perfect', 'even', 'might',
  'today', 'morning', 'door', 'take', 'vai', 'someone', 'fine', 'happy', 'to', 'believe', 'every',
  'one', 'school', 'gmail', 'ok', 'wife', 'else', 'hotmail', 'hello', 'nope', 'sorry', 'cent', 'son',
  'thats', 'guy', 'give', 'dad', 'well', 'press', 'somebody', 'wonder', 'yes', 'everybody', 'gonna',
  'cinq', 'lol', 'com', 'people', 'already', 'dot', 'june', 'stuff', 'person', 'blah', 'black', 
  'uh', 'thx', 'yahoo', 'pound', 'kind', 'money', 'color', 'try', 'im', 'um', 'puis', 'blah blah blah',
  'dollar', 'dont', 'mois', 'est', 'absolutely', 'ca', 'thanks', 'let', 'like', 'also', 'august',
  'see', 'happen', 'best', 'passwords', 'dollars', 'think', 'alright', 'buck', 'thank', 'need',
  'parce', 'pci', 'seem', 'que', 'thing', 'euh', 'maybe', 'hang', 'however', 'oh', 'daughter',
  'mais', 'blue', 'okay', 'husband', 'building', 'look', 'bye', 'would', 'live', 'white', 'good', 
  'ago', 'goodbye', 'mean', 'god', 'bien', 'etc', 'two', 'sure', 'yet', 'without', 'oui']

ALL_VERINT_STOPWORDS = set(ORD_NUM + NUMBERS + MONTH + DAYS + VERINT_FINAL_STOPWORDS + BOLDCHAT_FINAL_STOPWORDS + FRENCH_STOPWORDS + ENGLISH_STOPWORDS)


# COMMAND ----------

# MAGIC %md
# MAGIC verint.py

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
       
        
        if max_date is not None:
            verint_query = "{0} where {1}".format(verint_query, "conversation_date < '{}'".format(max_date))
            
        
        # household table
        # household_query = "select * from {0}".format(Verint.household_table)

        logger.info("reading cbu_rog_conversation_sumfct data from Databricks.")
        logger.debug("running queries: {0}\n".format(verint_query))
        return spark_s.sql(verint_query) #, spark_s.sql(household_query)  #DataBricks.spark.read.format("parquet").load(consumer_vq_input)
    #91  filter version for categories and instances
    @staticmethod
    def load_data_filter(min_date = None, max_date = None, categories = [], instances = [], spark_s = None):
        """
        Loading cbu_rog_conversation_sumfct data to spark dataframes
        :param condition: Optional condition to be passed to verint_query e.g. "where []"
        :return cbu_rog_conversation_sumfct dataframes respectively
        """
        #91 add distinct for sid_key
        if instances != [] and categories != []:
            print('Categories and instances provided')
            #Category id can be used to filter our churned customers
            verint_query = "SELECT * FROM ( \
                (SELECT distinct speech_id FROM \
                (SELECT distinct sid_key FROM VERINT.SESSIONS_CATEGORIES WHERE category_id in ('{0}') AND instance_id in ('{1}')) as category \
                INNER JOIN \
                (SELECT CONCAT(unit_num, '0', channel_num) as speech_id, sid_key FROM {1}) as booked \
                ON booked.sid_key = category.sid_key) as merged \
                INNER JOIN \
                (SELECT distinct SPEECH_ID_VERINT,TEXT_AGENT_FULL, TEXT_CUSTOMER_FULL, TEXT_OVERLAP, TEXT_ALL, CUSTOMER_ID, CTN, \
                AGENT_EMP_ID,CONNECTION_ID,RECEIVING_SKILL,LANGUAGE_INDICATOR, CONVERSATION_DATE, YEAR(conversation_date) as Year, \
                MONTH(conversation_date) as Month FROM {3} WHERE conversation_date >= '{4}') \
                as sumfct ON sumfct.speech_id_verint = merged.speech_id) as final"\
                .format(("','".join(categories)),("','".join(instances)),Verint.booked_table,Verint.verint_table, min_date)
        #91 add distinct for sid_key
        elif instances == [] and categories != []:
            print('Categories provided')
            verint_query = "SELECT * FROM ( \
                (SELECT distinct speech_id FROM \
                (SELECT distinct sid_key FROM VERINT.SESSIONS_CATEGORIES WHERE category_id in ('{0}')) as category \
                INNER JOIN \
                (SELECT CONCAT(unit_num, '0', channel_num) as speech_id, sid_key FROM {1}) as booked \
                ON booked.sid_key = category.sid_key) as merged \
                INNER JOIN \
                (SELECT distinct SPEECH_ID_VERINT,TEXT_AGENT_FULL, TEXT_CUSTOMER_FULL, TEXT_OVERLAP, TEXT_ALL, CUSTOMER_ID, CTN, \
                AGENT_EMP_ID,CONNECTION_ID,RECEIVING_SKILL,LANGUAGE_INDICATOR, CONVERSATION_DATE,  YEAR(conversation_date) as Year, \
                MONTH(conversation_date) as Month FROM {2} WHERE conversation_date >= '{3}') \
                as sumfct ON sumfct.speech_id_verint = merged.speech_id) as final" \
                .format(("','".join(categories)),Verint.booked_table,Verint.verint_table, min_date)
        #91 add distinct for sid_key
        elif instances != [] and categories == []:
            print('Instances provided')
            verint_query = "SELECT * FROM ( \
                (SELECT distinct speech_id FROM \
                (SELECT distinct sid_key FROM VERINT.SESSIONS_CATEGORIES WHERE instance_id in ('{0}')) as category \
                INNER JOIN \
                (SELECT CONCAT(unit_num, '0', channel_num) as speech_id, sid_key FROM {1}) as booked \
                ON booked.sid_key = category.sid_key) as merged \
                INNER JOIN \
                (SELECT distinct SPEECH_ID_VERINT,TEXT_AGENT_FULL, TEXT_CUSTOMER_FULL, TEXT_OVERLAP, TEXT_ALL, CUSTOMER_ID, CTN, \
                AGENT_EMP_ID,CONNECTION_ID,RECEIVING_SKILL,LANGUAGE_INDICATOR, CONVERSATION_DATE,  YEAR(conversation_date) as Year, \
                MONTH(conversation_date) as Month FROM {2} WHERE conversation_date >= '{3}') \
                as sumfct ON sumfct.speech_id_verint = merged.speech_id"\
                .format(("','".join(instances)),Verint.booked_table,Verint.verint_table, min_date)
       
        else:
            print('Only date provided')
            verint_query = "SELECT * FROM (SELECT distinct SPEECH_ID_VERINT,TEXT_AGENT_FULL, TEXT_CUSTOMER_FULL, TEXT_OVERLAP, \
                TEXT_ALL, CUSTOMER_ID, CTN, AGENT_EMP_ID, CONNECTION_ID, RECEIVING_SKILL, \
                LANGUAGE_INDICATOR, CONVERSATION_DATE, YEAR(conversation_date) as Year, MONTH(conversation_date) as Month \
                FROM {1} WHERE conversation_date >= '{2}') as final" \
                .format(Verint.booked_table, Verint.verint_table, min_date)
        
    
        if max_date is not None:
            verint_query = "{0} where {1}".format(verint_query, "conversation_date < '{}'".format(max_date))
            
        
        # household table
        # household_query = "select * from {0}".format(Verint.household_table)

        logger.info("reading cbu_rog_conversation_sumfct data from Databricks.")
        logger.debug("running queries: {0}\n".format(verint_query))
        return spark_s.sql(verint_query) #, spark_s.sql(household_query)  #DataBricks.spark.read.format("parquet").load(consumer_vq_input)
    @staticmethod
    def load_data_filter_not(min_date = None, max_date = None, categories = [], instances = [], spark_s = None):
        """
        Loading cbu_rog_conversation_sumfct data to spark dataframes
        :param condition: Optional condition to be passed to verint_query e.g. "where []"
        :return cbu_rog_conversation_sumfct dataframes respectively
        """
        #91 add distinct for sid_key
        if instances != [] and categories != []:
            print('Categories and instances provided')
            #Category id can be used to filter our churned customers
            verint_query = "SELECT * FROM ( \
                (SELECT distinct speech_id FROM \
                (SELECT distinct sid_key FROM VERINT.SESSIONS_CATEGORIES WHERE category_id not in ('{0}') AND instance_id not in ('{1}')) as category \
                INNER JOIN \
                (SELECT CONCAT(unit_num, '0', channel_num) as speech_id, sid_key FROM {1}) as booked \
                ON booked.sid_key = category.sid_key) as merged \
                INNER JOIN \
                (SELECT distinct SPEECH_ID_VERINT,TEXT_AGENT_FULL, TEXT_CUSTOMER_FULL, TEXT_OVERLAP, TEXT_ALL, CUSTOMER_ID, CTN, \
                AGENT_EMP_ID,CONNECTION_ID,RECEIVING_SKILL,LANGUAGE_INDICATOR, CONVERSATION_DATE, YEAR(conversation_date) as Year, \
                MONTH(conversation_date) as Month FROM {3} WHERE conversation_date >= '{4}') \
                as sumfct ON sumfct.speech_id_verint = merged.speech_id) as final"\
                .format(("','".join(categories)),("','".join(instances)),Verint.booked_table,Verint.verint_table, min_date)
        #91 add distinct for sid_key
        elif instances == [] and categories != []:
            print('Categories provided')
            verint_query = "SELECT * FROM ( \
                (SELECT distinct speech_id FROM \
                (SELECT distinct sid_key FROM VERINT.SESSIONS_CATEGORIES WHERE category_id not in ('{0}')) as category \
                INNER JOIN \
                (SELECT CONCAT(unit_num, '0', channel_num) as speech_id, sid_key FROM {1}) as booked \
                ON booked.sid_key = category.sid_key) as merged \
                INNER JOIN \
                (SELECT distinct SPEECH_ID_VERINT,TEXT_AGENT_FULL, TEXT_CUSTOMER_FULL, TEXT_OVERLAP, TEXT_ALL, CUSTOMER_ID, CTN, \
                AGENT_EMP_ID,CONNECTION_ID,RECEIVING_SKILL,LANGUAGE_INDICATOR, CONVERSATION_DATE,  YEAR(conversation_date) as Year, \
                MONTH(conversation_date) as Month FROM {2} WHERE conversation_date >= '{3}') \
                as sumfct ON sumfct.speech_id_verint = merged.speech_id) as final" \
                .format(("','".join(categories)),Verint.booked_table,Verint.verint_table, min_date)
        #91 add distinct for sid_key
        elif instances != [] and categories == []:
            print('Instances provided')
            verint_query = "SELECT * FROM ( \
                (SELECT distinct speech_id FROM \
                (SELECT distinct sid_key FROM VERINT.SESSIONS_CATEGORIES WHERE instance_id not in ('{0}')) as category \
                INNER JOIN \
                (SELECT CONCAT(unit_num, '0', channel_num) as speech_id, sid_key FROM {1}) as booked \
                ON booked.sid_key = category.sid_key) as merged \
                INNER JOIN \
                (SELECT distinct SPEECH_ID_VERINT,TEXT_AGENT_FULL, TEXT_CUSTOMER_FULL, TEXT_OVERLAP, TEXT_ALL, CUSTOMER_ID, CTN, \
                AGENT_EMP_ID,CONNECTION_ID,RECEIVING_SKILL,LANGUAGE_INDICATOR, CONVERSATION_DATE,  YEAR(conversation_date) as Year, \
                MONTH(conversation_date) as Month FROM {2} WHERE conversation_date >= '{3}') \
                as sumfct ON sumfct.speech_id_verint = merged.speech_id"\
                .format(("','".join(instances)),Verint.booked_table,Verint.verint_table, min_date)
       
        else:
            print('Only date provided')
            verint_query = "SELECT * FROM (SELECT distinct SPEECH_ID_VERINT,TEXT_AGENT_FULL, TEXT_CUSTOMER_FULL, TEXT_OVERLAP, \
                TEXT_ALL, CUSTOMER_ID, CTN, AGENT_EMP_ID, CONNECTION_ID, RECEIVING_SKILL, \
                LANGUAGE_INDICATOR, CONVERSATION_DATE, YEAR(conversation_date) as Year, MONTH(conversation_date) as Month \
                FROM {1} WHERE conversation_date >= '{2}') as final" \
                .format(Verint.booked_table, Verint.verint_table, min_date)

        
        if max_date is not None:
            verint_query = "{0} where {1}".format(verint_query, "conversation_date < '{}'".format(max_date))
            
        
        # household table
        # household_query = "select * from {0}".format(Verint.household_table)

        logger.info("reading cbu_rog_conversation_sumfct data from Databricks.")
        logger.debug("running queries: {0}\n".format(verint_query))
        return spark_s.sql(verint_query) #, spark_s.sql(household_query)  #DataBricks.spark.read.format("parquet").load(consumer_vq_input)

# COMMAND ----------

# MAGIC %md
# MAGIC verintETL

# COMMAND ----------

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

class VerintETL:
    """
    This class covers the required functionalities to perform
    primilinary ETL jobs on cbu_rog_conversation_sumfct
    to prepare an integraded dateset for the Normalizer Pipeline.
    """

    sumfct_keep_cols = [
                        'label',
                        'TEXT_CUSTOMER_FULL',
                        'TEXT_AGENT_FULL',
                        'TEXT_ALL',
                        'MULTI_BRAND', 
                        'VOL_INVOL_IND', 
                        'PLATFORM', 
                        'ACTIVITY_GRADE_CODE',
                        'RECEIVING_SKILL',
                        'CTN',
                        'CUSTOMER_ID',
                        'SPEECH_ID_VERINT',
                        'CONVERSATION_DATE',
                        'Year',
                        'Month'
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
        verint_df_col=list(verint_df.columns)
        if "CLEAN_TEXT_CUSTOMER" in verint_df_col:
            input_col="AGENT"
        else:
            input_col="CUSTOMER"
        if input_col=="CUSTOMER":
            first="TEXT_CUSTOMER_FULL"
        else:
            first="TEXT_AGENT_FULL"
        documentAssembler = DocumentAssembler() \
            .setInputCol(first) \
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
 
        lemmatizer = LemmatizerModel.pretrained() \
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
            .setOutputCol("onlyAlphaTokens_"+input_col) \
            .setLowercase(True)\
            .setCleanupPatterns(["""[^A-Za-z]"""]) #91 only keep alphabet letters, could remove "+"       
        
        #91 outputasarray -> true
        finisher = Finisher() \
            .setInputCols(["onlyAlphaTokens_"+input_col]) \
            .setOutputCols("CLEAN_TEXT_"+input_col) \
            .setOutputAsArray(False)\
            .setCleanAnnotations(False)\
            .setAnnotationSplitSymbol(" ")

        nlp_pipeline = Pipeline(
            stages=
            [documentAssembler, documentNormalizer1, documentNormalizer2,documentNormalizer3, documentNormalizer4,  tokenizer, normalizer1, lemmatizer,  stopwords_cleaner, stopWords, normalizer2, finisher]
        )
        verint_df_col.append("CLEAN_TEXT_"+input_col)
        verint_df_col.append("onlyAlphaTokens_"+input_col)
        print(verint_df_col)
        return nlp_pipeline\
            .fit(verint_df)\
            .transform(verint_df).select(verint_df_col)      

    def __drop_for_extracting_cus_msg(self, verint_df):
        print("drop for extracting customer msg")
        return verint_df.drop('TEXT_AGENT_FULL', 'TEXT_OVERLAP', 'TEXT_UNKNOWN')

    def __text_df(self, verint_df):
        return verint_df.where(f.length('CLEAN_TEXT_CUSTOMER') > 0)


    def __no_text_df(self, verint_df):
        return verint_df.where(f.length('CLEAN_TEXT_CUSTOMER') == 0)

    def __competitor_mention(self, verint_df):
        print("competitor mention")
        comp_udf = udf(VerintETL.competitor_mention, StringType())
        return verint_df.withColumn("COMPETITOR_MENTION", comp_udf(verint_df['CLEAN_TEXT_CUSTOMER']))
    
    def __product_mention(self, verint_df):
        print("product mention")
        comp_udf = udf(VerintETL.product_mention, StringType())
        return verint_df.withColumn("PRODUCT_MENTION", comp_udf(verint_df['CLEAN_TEXT_CUSTOMER']))

    def __rogers_fido_mention(self, verint_df):
        print("rogers fido mention")
        comp_udf = udf(VerintETL.rogers_fido_mention, StringType())
        return verint_df.withColumn("ROGERS_FIDO_MENTION", comp_udf(verint_df['CLEAN_TEXT_CUSTOMER']))



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

    ##########################
    ### Wireless Service ETL ####
    ##########################
    def wir_ser_etl(self):
        return self.verint_df \
            .transform(self.__wireless_cable_vqs) \
            .transform(self.__filter_by_receiving_skill('WIR')) \
            .transform(self.__extract_cus_msg_spacy) \
            .transform(self.__drop_for_extracting_cus_msg)

    def en_product_mention_etl(self, df):
        return df \
            .transform(self.__filter_by_receiving_skill('EN')) \
            .transform(self.__product_mention) 

        # removed a line: .transform(self.__wireless_cable('CBL', 'WIR')) \

    def cbl_ser_etl(self):
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
            .transform(self.__extract_cus_msg_spark)\
            .transform(self.__msg_not_empty_spark("onlyAlphaTokens_CUSTOMER"))\
            .transform(self.__extract_cus_msg_spark)\
            .transform(self.__msg_not_empty_spark("onlyAlphaTokens_AGENT"))\
            .transform(self.__competitor_mention)\
            .transform(self.__rogers_fido_mention)
    ##########################
    ### Wireless Service ETL ####
    ##########################
    def wir_ser_etl(self):
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
            .transform(self.__extract_cus_msg_spark)\
            .transform(self.__msg_not_empty_spark("onlyAlphaTokens_CUSTOMER"))\
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

# MAGIC %md
# MAGIC household.py

# COMMAND ----------

import logging

logger = logging.getLogger("Household_DataOPs")

class Household:
    """
        This class wraps data operations for household
    """

    ### Table names & queries on Databricks ###
    # household_table = "APP_IBRO.IBRO_HOUSEHOLD_ACTIVITY"
    household_table= "ml_etl_output_data.IBRO_HOUSEHOLD_ACTIVITY_CHURN_SCHEDULED"
    # nonchurn_table = "DEFAULT.IBRO_HOUSEHOLD_ACTIVITY_NONCHURN_SCHEDULED"
    # change the nonchurn table for new cluster
    nonchurn_table = "ml_etl_output_data.IBRO_HOUSEHOLD_ACTIVITY_NONCHURN_SCHEDULED"
    all_household= "ml_etl_output_data.IBRO_HOUSEHOLD_ACTIVITY_SCHEDULED"

    @staticmethod
    def load_data(spark_s = None, churn = None):
        """
        Loading ibro_household_activity data to spark dataframes
        :param condition: Optional condition to be passed to household_query e.g. "where []"
        :return ibro_household_activity dataframes respectively
        """

        # those columns can be used to filter our churned customers
        # find out CAN, treated as account_number
        # ARPA_OUT = -1 means deac, ARPA_OUT = 1 means winback
        if churn is None:
            household_query = "select * from {0}".format(Household.all_household)
            
        elif churn == True:
            household_query = "select * from {0}".format(Household.household_table)

        elif churn == False:
            household_query = "select * from {0}".format(Household.nonchurn_table)

        logger.info("reading ibro_household_activity data from Databricks.")
        logger.debug("running queries: {0}\n".format(household_query))
        return spark_s.sql(household_query) 

# COMMAND ----------

# MAGIC %md
# MAGIC churn & non-churn classification

# COMMAND ----------

# MAGIC %md
# MAGIC ibro data preparation

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

from sparknlp.base import *
from pyspark.ml import Pipeline
data = spark.createDataFrame([["Spark NLP is an open-source text processing library."]]).toDF("text")
documentAssembler = DocumentAssembler().setInputCol("text").setOutputCol("document")
result = documentAssembler.transform(data)
print(result)


logger = logging.getLogger("Churn_ETL_test")
today = datetime.today() - timedelta(hours=5)

min_date = '2022-05-01'
max_date = '2022-07-01'
###################################
## Creating a VerintETL Object ##
## to perform ETL jobs on the    ##
## input data.                   ##
###################################
spark_session = spark
#verint= Verint.load_data(min_date = min_date, max_date = max_date, spark_s = spark_session) 
# # 1. load cumfct table

#verint= Verint.load_data(min_date = min_date, max_date = max_date, categories = ('101000264', '101001648', '109000006', '109000011'), spark_s = spark_session) 
#verint= Verint.load_data_filter(min_date = min_date, max_date = max_date, categories = ['101000264', '101001648', '109000006', '109000011'], spark_s = spark_session) 
verint= Verint.load_data_filter(min_date = min_date, max_date = max_date, spark_s = spark_session) 
print(verint.count())

#churn
# 2. merge before etl transformation
household_df_churn = Household.load_data(spark_s = spark_session, churn = True)
print("=======================churn household columns===============================")
print(household_df_churn.columns)

ibro_verint_df_churn = verint.join(household_df_churn, verint.CUSTOMER_ID == household_df_churn.HASH_LKP_ACCOUNT, 'inner')\
    .drop(household_df_churn.CUSTOMER_ID)\
    .drop(household_df_churn.CUSTOMER_COMPANY)\
    .drop(household_df_churn.CUSTOMER_ACCOUNT)\
    .drop(household_df_churn.REPORT_DATE)
## moving .drop(verint.account_number) because no such column
ibro_verint_df_churn=ibro_verint_df_churn.withColumn("label",lit("churn"))
print("=======================churn merged ibro and verint===============================")
print(ibro_verint_df_churn.columns)
print("ibro_verint count rows: {}".format(ibro_verint_df_churn.count()))

#non_churn
# 2. merge before etl transformation
household_df_nonchurn = Household.load_data(spark_s = spark_session, churn = False)
print("=======================nonchurn household columns===============================")
print(household_df_nonchurn.columns)

ibro_verint_df_nonchurn = verint.join(household_df_nonchurn, verint.CUSTOMER_ID == household_df_nonchurn.HASH_LKP_ACCOUNT, 'inner')\
    .drop(household_df_nonchurn.CUSTOMER_ID)\
    .drop(household_df_nonchurn.CUSTOMER_COMPANY)\
    .drop(household_df_nonchurn.CUSTOMER_ACCOUNT)\
    .drop(household_df_nonchurn.REPORT_DATE)
## moving .drop(verint.account_number) because no such column
ibro_verint_df_nonchurn=ibro_verint_df_nonchurn.withColumn("label",lit("nonchurn"))
print("=======================nonchurn merged ibro and verint===============================")
print(ibro_verint_df_nonchurn.columns)
print("ibro_verint count rows: {}".format(ibro_verint_df_nonchurn.count()))


# COMMAND ----------

churn_count=ibro_verint_df_churn.count()
ibro_verint_df_churn_modified=ibro_verint_df_churn.select(['SPEECH_ID_VERINT', 'TEXT_AGENT_FULL', 'TEXT_CUSTOMER_FULL', 'TEXT_OVERLAP', 'TEXT_ALL', 'CUSTOMER_ID', 'CTN', 'AGENT_EMP_ID', 'CONNECTION_ID', 'RECEIVING_SKILL', 'LANGUAGE_INDICATOR', 'CONVERSATION_DATE', 'Year', 'Month', 'ACCOUNT_NUMBER', 'ARPA_OUT', 'ENTERPRISE_ID', 'MULTI_BRAND', 'VOL_INVOL_IND', 'PLATFORM', 'ACTIVITY_GRADE_CODE', 'HASH_LKP_ACCOUNT', 'HASH_LKP_ECID', 'label'])
ibro_verint_df_nonchurn_modified=ibro_verint_df_nonchurn.limit(churn_count)
ibro_verint_df=ibro_verint_df_churn_modified.union(ibro_verint_df_nonchurn_modified)

# COMMAND ----------

display(ibro_verint_df)

# COMMAND ----------

# MAGIC %md
# MAGIC preprocessing spark test

# COMMAND ----------

verint_etl = VerintETL(ibro_verint_df) 
verint_df = verint_etl.cbl_ser_etl_spark() # preprocess
final_df = verint_etl.en_product_mention_etl(verint_df) # 3. merge as transformation

print("======================preprocess===============================")
print(final_df.count())

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.coalesce(1).write.mode("overwrite").option("header", True).format("parquet").saveAsTable("default"+".churn_classification_preprocessing")

# COMMAND ----------

# MAGIC %md
# MAGIC final df data wrangling for "mention" columns

# COMMAND ----------

final_df=final_df.fillna("NONE")
def split(value):
    return value.split("|")
split_udf = udf(split, ArrayType(StringType()))
final_df=final_df.withColumn("COMPETITOR_MENTION_LIST", split_udf(col('COMPETITOR_MENTION')))
final_df=final_df.withColumn("ROGERS_FIDO_MENTION_LIST", split_udf(col('ROGERS_FIDO_MENTION')))
final_df=final_df.withColumn("PRODUCT_MENTION_LIST", split_udf(col('PRODUCT_MENTION')))

# COMMAND ----------

print("Spark NLP version", sparknlp.version())
print("Apache Spark version:", spark.version)

# COMMAND ----------

competitors=['Bell','Telus','Cogeco','Freedom','Virgin','TekSavvy','Shaw','Public Mobile','Chatr','Koodo','Fonus'] 
#for competitors need to bundle minority groups to "other"
ROGERS_FIDO=['Rogers','Fido']
products = ["Ignite SmartStream","Ignite TV","Ignite Internet","Ignite Bundles","Smart Home","Wireless Home Internet"]
def replaceNonEU(c):
    cond = c == competitors[0]
    for competitor in competitors[1:]:
        cond |= (c == competitor)
    return when(cond, c).otherwise(lit("None"))

final_df = final_df.withColumn("COMPETITOR_MENTION_LIST", array_distinct(transform("COMPETITOR_MENTION_LIST", replaceNonEU)))
for c in competitors:
    final_df = final_df.withColumn(c, array_contains("COMPETITOR_MENTION_LIST", c).cast("int"))
final_df = final_df.drop("COMPETITOR_MENTION_LIST")
def replaceNonEU(c):
    cond = c == ROGERS_FIDO[0]
    for each in ROGERS_FIDO[1:]:
        cond |= (c == each)
    return when(cond, c).otherwise(lit("None"))

final_df = final_df.withColumn("ROGERS_FIDO_MENTION_LIST", array_distinct(transform("ROGERS_FIDO_MENTION_LIST", replaceNonEU)))
for c in ROGERS_FIDO:
    final_df = final_df.withColumn(c, array_contains("ROGERS_FIDO_MENTION_LIST", c).cast("int"))
final_df = final_df.drop("ROGERS_FIDO_MENTION_LIST")
def replaceNonEU(c):
    cond = c == products[0]
    for each in products[1:]:
        cond |= (c == each)
    return when(cond, c).otherwise(lit("None"))

final_df = final_df.withColumn("PRODUCT_MENTION_LIST", array_distinct(transform("PRODUCT_MENTION_LIST", replaceNonEU)))
for c in products:
    final_df = final_df.withColumn(c, array_contains("PRODUCT_MENTION_LIST", c).cast("int"))
final_df = final_df.drop("PRODUCT_MENTION_LIST")

# COMMAND ----------

display(final_df)

# COMMAND ----------

import sparknlp

from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
import pandas as pd
import os

spark = sparknlp.start(gpu = True)# for GPU training >> sparknlp.start(gpu = True)

print("Spark NLP version", sparknlp.version())
print("Apache Spark version:", spark.version)

spark

# COMMAND ----------

final_df_churn=final_df.filter(col("label")=='churn')
churn_train, churn_test=final_df_churn.randomSplit([0.7,0.3],19)
final_df_nonchurn=final_df.filter(col("label")=='nonchurn')
nonchurn_train, nonchurn_test=final_df_nonchurn.randomSplit([0.7,0.3],19)
trainDataset=churn_train.union(nonchurn_train)
testDataset=churn_test.union(nonchurn_test)

# COMMAND ----------

print(final_df_churn.count())
print(final_df_nonchurn.count())

# COMMAND ----------

print(trainDataset.count())
print(testDataset.count())
print(churn_train.count())
print(nonchurn_train.count())

# COMMAND ----------

trainDataset.columns

# COMMAND ----------

# MAGIC %md
# MAGIC trail one: only use CLEAN_TEXT_CUSTOMER and use bert

# COMMAND ----------

#could have more than 1 document assembler, get embeddings and deliver to classifier for both agent and customer
document_assembler = DocumentAssembler() \
    .setInputCol("CLEAN_TEXT_CUSTOMER") \
    .setOutputCol("document")

tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")
      
bert_embeddings = BertEmbeddings().pretrained("bert_embeddings_bert_large_uncased_whole_word_masking","en") \
    .setInputCols(["document",'token'])\
    .setOutputCol("embeddings")

embeddingsSentence = SentenceEmbeddings() \
    .setInputCols(["document", "embeddings"]) \
    .setOutputCol("sentence_embeddings") \
    .setPoolingStrategy("AVERAGE")

classsifierdl = ClassifierDLApproach()\
    .setInputCols(["sentence_embeddings"])\
    .setOutputCol("class")\
    .setLabelColumn("label")\
    .setMaxEpochs(10)\
    .setLr(0.001)\
    .setBatchSize(64)\
    .setEnableOutputLogs(True)
    #.setOutputLogsPath('logs')

pipeline = Pipeline(stages=[
    document_assembler,
    tokenizer,
    bert_embeddings,
    embeddingsSentence,
    classsifierdl
])

pipelineModel = pipeline.fit(trainDataset)

# COMMAND ----------

!cd ~/annotator_logs && ls -l

# COMMAND ----------

save_cols=['label',
 'TEXT_CUSTOMER_FULL',
 'TEXT_AGENT_FULL',
 'MULTI_BRAND',
 'VOL_INVOL_IND',
 'PLATFORM',
 'ACTIVITY_GRADE_CODE',
 'RECEIVING_SKILL',
 'CTN',
 'CUSTOMER_ID',
 'SPEECH_ID_VERINT',
 'CONVERSATION_DATE',
 'Year',
 'Month',
 'CLEAN_TEXT_CUSTOMER',
 'CLEAN_TEXT_AGENT',
 'COMPETITOR_MENTION',
 'ROGERS_FIDO_MENTION',
 'PRODUCT_MENTION',
 'Bell',
 'Telus',
 'Cogeco',
 'Freedom',
 'Virgin',
 'TekSavvy',
 'Shaw',
 'Public Mobile',
 'Chatr',
 'Koodo',
 'Fonus',
 'Rogers',
 'Fido',
 'Ignite SmartStream',
 'Ignite TV',
 'Ignite Internet',
 'Ignite Bundles',
 'Smart Home',
 'Wireless Home Internet',
 expr("class.result[0]").alias("class"),
 expr("class.metadata[0].churn").alias("prob_churn_string")
          ]
#"class.metadata.churn" need to be conversted to float

# COMMAND ----------

from sklearn.metrics import classification_report
prediction_train = pipelineModel.transform(trainDataset)
predsPd_train = prediction_train.select('label','CLEAN_TEXT_CUSTOMER',"class.result").toPandas()
predsPd_train['result'] = predsPd_train['result'].apply(lambda x : x[0])
print (classification_report(predsPd_train['result'], predsPd_train['label']))

# COMMAND ----------

display(prediction_train)

# COMMAND ----------

prediction_test = pipelineModel.transform(testDataset)
predsPd_test = prediction_test.select('label','CLEAN_TEXT_CUSTOMER',"class.result").toPandas()
predsPd_test['result'] = predsPd_test['result'].apply(lambda x : x[0])
print (classification_report(predsPd_test['result'], predsPd_test['label']))

# COMMAND ----------

# MAGIC %md
# MAGIC trail two: use both CLEAN_TEXT_CUSTOMER and CLEAN_TEXT_AGENT and use doc2vec

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE verint.cbu_rog_conversation_sumfct

# COMMAND ----------

#could have more than 1 document assembler, get embeddings and deliver to classifier for both agent and customer
document1 = DocumentAssembler()\
  .setInputCol("CLEAN_TEXT_CUSTOMER")\
  .setOutputCol("document1")

token1 = Tokenizer()\
  .setInputCols("document1")\
  .setOutputCol("token1")
  
doc2Vec1 = Doc2VecApproach()\
  .setInputCols("token1")\
  .setOutputCol("sentence_embeddings_CUSTOMER")\
  .setMaxSentenceLength(500)\
  .setStepSize(0.025)\
  .setMinCount(5)\
  .setVectorSize(100)\
  .setNumPartitions(1)\
  .setMaxIter(1)\
  .setSeed(44)

document2 = DocumentAssembler()\
  .setInputCol("CLEAN_TEXT_AGENT")\
  .setOutputCol("document2")

token2 = Tokenizer()\
  .setInputCols("document2")\
  .setOutputCol("token2")
  
doc2Vec2 = Doc2VecApproach()\
  .setInputCols("token2")\
  .setOutputCol("sentence_embeddings_AGENT")\
  .setMaxSentenceLength(500)\
  .setStepSize(0.025)\
  .setMinCount(5)\
  .setVectorSize(100)\
  .setNumPartitions(1)\
  .setMaxIter(1)\
  .setSeed(44)

classifierdl = ClassifierDLApproach()\
  .setInputCols(["sentence_embeddings_CUSTOMER","sentence_embeddings_AGENT"])\
  .setOutputCol("class")\
  .setLabelColumn("label")\
  .setMaxEpochs(5)\
  .setEnableOutputLogs(True)\
  .setOutputLogsPath('/dbfs/tmp/log/')

pipeline2 = Pipeline(
    stages = [
        document1,
        token1,
        doc2Vec1,
        document2,
        token2,
        doc2Vec2,
        classifierdl
    ])
pipelineModel2 = pipeline2.fit(trainDataset)

# COMMAND ----------

from sklearn.metrics import classification_report
prediction_train = pipelineModel2.transform(trainDataset)
predsPd_train = prediction_train.select('label','CLEAN_TEXT_CUSTOMER','CLEAN_TEXT_AGENT',"class.result").toPandas()
predsPd_train['result'] = predsPd_train['result'].apply(lambda x : x[0])
print (classification_report(predsPd_train['result'], predsPd_train['label']))

# COMMAND ----------

prediction_test = pipelineModel2.transform(testDataset)
predsPd_test = prediction_test.select('label','CLEAN_TEXT_CUSTOMER','CLEAN_TEXT_AGENT',"class.result").toPandas()
predsPd_test['result'] = predsPd_test['result'].apply(lambda x : x[0])
print (classification_report(predsPd_test['result'], predsPd_test['label']))

# COMMAND ----------

# MAGIC %md
# MAGIC work on the original dataframe, incorporate categorical feature

# COMMAND ----------

print(final_df.columns) 
#saved_train 
#saved_test

# COMMAND ----------

trail=final_df.select('MULTI_BRAND', 'VOL_INVOL_IND', 'PLATFORM','ACTIVITY_GRADE_CODE', 'RECEIVING_SKILL','COMPETITOR_MENTION', 'ROGERS_FIDO_MENTION', 'PRODUCT_MENTION','label')
print(trail.select('MULTI_BRAND','label').groupby('MULTI_BRAND','label').count().show())
print(trail.select('VOL_INVOL_IND','label').groupby('VOL_INVOL_IND','label').count().show(),trail.select('VOL_INVOL_IND').distinct().count())
print(trail.select('PLATFORM','label').groupby('PLATFORM','label').count().show(),trail.select('PLATFORM').distinct().count())
print(trail.select('ACTIVITY_GRADE_CODE','label').groupby('ACTIVITY_GRADE_CODE','label').count().show(truncate=False),trail.select('ACTIVITY_GRADE_CODE').distinct().count())
#print(trail.select('RECEIVING_SKILL').groupby('RECEIVING_SKILL').count().show(),trail.select('RECEIVING_SKILL').distinct().count())
print('***********')
#14967+15316=30283

# COMMAND ----------

display(trail.select('label','ACTIVITY_GRADE_CODE').groupby('label','ACTIVITY_GRADE_CODE').count())

# COMMAND ----------

print(trail.filter(col('COMPETITOR_MENTION').isNotNull()).count())
print(trail.filter(col('ROGERS_FIDO_MENTION').isNotNull()).count())
print(trail.filter(col('PRODUCT_MENTION').isNotNull()).count())

# COMMAND ----------

save_cols=['label',
 'TEXT_CUSTOMER_FULL',
 'TEXT_AGENT_FULL',
 'MULTI_BRAND',
 'VOL_INVOL_IND',
 'PLATFORM',
 'ACTIVITY_GRADE_CODE',
 'RECEIVING_SKILL',
 'CTN',
 'CUSTOMER_ID',
 'SPEECH_ID_VERINT',
 'CONVERSATION_DATE',
 'Year',
 'Month',
 'CLEAN_TEXT_CUSTOMER',
 'CLEAN_TEXT_AGENT',
 'COMPETITOR_MENTION',
 'ROGERS_FIDO_MENTION',
 'PRODUCT_MENTION',
 expr("class.result[0]").alias("class"),
 expr("class.metadata[0].churn").alias("prob_churn_string")
          ]
#"class.metadata.churn" need to be conversted to float

# COMMAND ----------

display(prediction_test.selectExpr("class.metadata[0].churn"))

# COMMAND ----------

display(prediction_test)

# COMMAND ----------

display(prediction_test.select(save_cols).withColumn("prob_churn",col("prob_churn_string").cast("float")))

# COMMAND ----------

saved_train=prediction_train.select(save_cols).withColumn("prob_churn",col("prob_churn_string").cast("float"))
saved_test=prediction_test.select(save_cols).withColumn("prob_churn",col("prob_churn_string").cast("float"))

# COMMAND ----------

import pyspark.ml
import numpy as np
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator 
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier

# COMMAND ----------

categoricalCols=['MULTI_BRAND', 'VOL_INVOL_IND', 'PLATFORM'] #RECEIVING_SKILL ,'ACTIVITY_GRADE_CODE' should be dealt with embedding technique
indexOutputCols=[x + "_Index" for x in categoricalCols]
oheOutputCols=[x+"_OHE" for x in categoricalCols]
stringIndexer= StringIndexer(inputCols=categoricalCols, outputCols=indexOutputCols, handleInvalid="skip")
oheEncoder = OneHotEncoder(dropLast=True, inputCols=indexOutputCols, outputCols=oheOutputCols)
numericalCols=["prob_churn"]
'''numericalCols=['Bell','Telus','Cogeco','Freedom','Virgin','TekSavvy','Shaw','Public Mobile','Chatr','Koodo','Fonus','Rogers','Fido',
 'Ignite SmartStream','Ignite TV','Ignite Internet','Ignite Bundles','Smart Home','Wireless Home Internet', "prob_churn"]'''
assemblerInputs=oheOutputCols+numericalCols
vecAssembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
rfClassifier=RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50)
secondStagePipeline=Pipeline(stages=[stringIndexer, oheEncoder, vecAssembler, rfClassifier])


'''xgb_classifier = XgboostClassifier(missing=0.0, num_workers=9, labelCol="label"
    validationIndicatorCol='isVal', weightCol='weight',
    early_stopping_rounds=1, eval_metric='logloss')'''
#classifier = XgboostClassifier(num_workers=N, use_gpu=True, **{other params})    9.1ML run time
#xgb_classifier = XgboostClassifier(max_depth=5, missing=0.0, num_workers=9, labelCol="cnt"
#    validationIndicatorCol='isVal', weightCol='weight',
#    early_stopping_rounds=1, eval_metric='logloss')
'''xgb_clf_model = xgb_classifier.fit(df_train)
xgb_clf_model.transform(df_test).show()'''


# COMMAND ----------

saved_train_label_trans=saved_train.withColumnRenamed("label","label_string")
saved_train_label_trans=saved_train_label_trans.withColumn("label", when(saved_train_label_trans.label_string=="churn",1.0).otherwise(0.0))
saved_test_label_trans=saved_test.withColumnRenamed("label","label_string")
saved_test_label_trans=saved_test_label_trans.withColumn("label",when(saved_test_label_trans.label_string=="churn",1.0).otherwise(0.0))

# COMMAND ----------

display(saved_train_label_trans)#check to make sure features and label col are correct

# COMMAND ----------

secondStagePipelineModel=secondStagePipeline.fit(saved_train_label_trans)

# COMMAND ----------

secondStageTrainPrediction=secondStagePipelineModel.transform(saved_train_label_trans)
secondStageTestPrediction=secondStagePipelineModel.transform(saved_test_label_trans)

# COMMAND ----------

display(secondStageTrainPrediction)

# COMMAND ----------

secondStageTrainPredictionPandas = secondStageTrainPrediction.select('label',"prediction").toPandas()
print (classification_report(secondStageTrainPredictionPandas['prediction'], secondStageTrainPredictionPandas['label']))
secondStageTestPredictionPandas = secondStageTestPrediction.select('label',"prediction").toPandas()
print (classification_report(secondStageTestPredictionPandas['prediction'], secondStageTestPredictionPandas['label']))

# COMMAND ----------

secondStageTestPredictionPandas = secondStageTestPrediction.select('label',"prediction").toPandas()
print (classification_report(secondStageTestPredictionPandas['prediction'], secondStageTestPredictionPandas['label']))

# COMMAND ----------

#feature importance ? data leakage?

# COMMAND ----------

# MAGIC %md
# MAGIC trail three: use both CLEAN_TEXT_CUSTOMER and CLEAN_TEXT_AGENT and use doc2vec and incorporate other features, need to fillna first    
# MAGIC do not need embedding, but need churn probability,

# COMMAND ----------


