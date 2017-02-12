from pyspark import SparkContext,Row,SQLContext,SparkConf
from elasticsearch import Elasticsearch
from pyspark.mllib.feature import HashingTF, IDF
import imp
import json
import ast
import string
import re
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.clustering import DistributedLDAModel,LDA
RedisConfig = imp.load_source('RedisConfig', '/redis_conf/RedisConfig.py')
from datetime import datetime
import redis
from RedisConfig import RedisConfig
import types

cfg = RedisConfig()
SPARK_IP = cfg.SPARK_IP
SPARK_PORT = cfg.SPARK_PORT
SPARK_APP_NAME = cfg.SPARK_APP_NAME
AWS_ACCESS_KEY_ID = cfg.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = cfg.AWS_SECRET_ACCESS_KEY

conf = SparkConf() \
    .setMaster("spark://%s:%s" %
        (SPARK_IP, SPARK_PORT)) \
    .setAppName(SPARK_APP_NAME)
sc = SparkContext(conf=conf)
sc_sql = SQLContext(sc)


def getValues(keys):
	redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=1)
	values=[]
        for i in keys:
                v=redis_db.get(i)
		values.append((i,v))
	return values

def process(docs):
	result =[]
	for doc in docs:
		json_data = ast.literal_eval(doc[1])
                try:
                        meta_data = ast.literal_eval(json_data['data'])
                        desc = meta_data['desc']
                        name = meta_data['repo_name']
                        result.append(Row(idd=name,words=desc.split(' ')))
                except:
                        print doc
                        pass

	return result

def stem_words(wordList) :
	rows =[]
	for word in wordList:
		strs =[]
        	for w in word[1] :
                	s = w.encode('ascii')
                	for c in string.punctuation:
                        	s=s.replace(c,"")
                	if (len(s)>2):
                                ss=PorterStemmer().stem(s)
                                strs.append(ss)

        	row = Row(idd = word[0], words =strs)
		rows.append(row)
        return  rows

def get_names(repos):
	names =[]
        for repo in repos:
               	json_data =  ast.literal_eval(data[1])
                name = ast.literal_eval(json_data['meta'])['repo_name']
                names.append((repo[0].encode('ascii'),name))
        return names
def doc_name(data):
	json_data =  ast.literal_eval(data[1])
        meta = ast.literal_eval(json_data['data'])
        name = meta['repo_name']
        stars = json_data['stars']
        url =meta['url']
        actor=meta['actor']
        res = (name,(data[0],stars, url, actor))
        return res

def extraxt(x):
        d=[]
        for word, weight in zip (x[1],x[2]):
                w = vocabArray[word].encode('ascii')
                d.append((x[0],w,weight))
        return d


def write_stars(record):
        redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=10)
        for repo in record[1]:
                        redis_db.rpush(record[0], json.dumps(repo))
                        #print 'insert to 4',  redis_db.get(record[0])
        print 'insert',  redis_db.rpop(record[0])
        return True

def write_terms(word):
	redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=11)
        for record in records:
                redis_db.set(record[0], json.dumps(record[1]))
                print 'insert to 4',  redis_db.get(record[0])

def repo_topic(trans):
	redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=5)
	res=[]
	for record in trans:
		key = record.idd.encode('ascii')
		value = record.topicDistribution.argmax()
		redis_db.set(key, value)
		res.append((key, value))
	return res

def write_topics(records):
        redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=13)
        for record in records:
                redis_db.set(record[0], json.dumps(record[1]))
                print 'insert',  redis_db.get(record[0])

def similar_repo(repos):
	result =[]
	for repo in repos:
		if isinstance(repo[1][0],int):
			topic_id = repo[1][0]
			data = repo[1][1]
		else :
			topic_id = repo[1][1]
                        data = repo[1][0]
		json_obj={}
                json_obj['repo_name']=repo[0]
                json_obj['repo_id']=data[0]
                json_obj['stars']=data[1]
                json_obj['updated']=data[2]
                json_obj['url']=data[3]
                json_obj['actor']=data[4]
                json_obj['desc']=data[4]
                result.append((topic_id,json_obj))

	return result
def findMax(record):
        w = record[0]
        max_w=0
        max_t=0
        for topic, weight in record[1]:
                if (weight > max_w):
                        max_w=weight
                        max_t=topic
        return (w,max_t,max_w)

			
if __name__ == "__main__":
       	REDIS_IP=sc.broadcast(cfg.REDIS_IP)
	REDIS_PORT=sc.broadcast(cfg.REDIS_PORT)
	REDIS_PASS =sc.broadcast(cfg.REDIS_PASS)
	redis_db = redis.Redis(host=cfg.REDIS_IP, port=cfg.REDIS_PORT,password=cfg.REDIS_PASS, db=1)
	
	#retrieve repositories form db
	keys = redis_db.keys()
	rawData = sc.parallelize(keys)
	data = rawData.mapPartitions(getValues)
	rawData.unpersist()

	#prepare description for LDA
	docs = data.mapPartitions(process)
	docDF = sc_sql.createDataFrame(docs)
	docs.unpersist()
	res = StopWordsRemover(inputCol="words", outputCol="filtered").transform(docDF)
	df = res.drop("words")
	docDF.unpersist()
	stem  = df.rdd.mapPartitions(lambda x :stem_words(x))
	df.unpersist()
	
	#create data fram of repos with their features vectors 
	df =  sc_sql.createDataFrame(stem)
	Vector = CountVectorizer(inputCol="words", outputCol="features")
	model = Vector.fit(df)
	result = model.transform(df)
	
	#LDA modeling
	lda = LDA(k=60, maxIter=10,optimizer='em')
	ldaModel = lda.fit(result)
	transformed = ldaModel.transform(result)
	
	#writre in the form of (repo_name,topic_id) to redis 
	trans = transformed.rdd.mapPartitions(repo_topic)

	#group results as (topic_id, repos)
	ful_data = trans.join(data)
        topic_repo = ful_data.map(lambda x : (x[1][0],x[1][1]))
        topic_repos = topic_repo.groupByKey().map(lambda x : (x[0], list(x[1])))
	
	#sort by number of stars and write to database
        sort_stars = topic_repos.map(lambda x : (x[0],sorted(x[1], key=lambda i:ast.literal_eval(i)['stars'],reverse=True)))
	sort_stars.foreach(write_stars)

	'''vocabulary'''
	vocabArray = model.vocabulary
	topicIndices = ldaModel.describeTopics(200)
	print("The topics described by their top-weighted terms:")
	topicIndices.show(truncate=False)
	topics = ldaModel.topicsMatrix()

	#get words with their topic weights 
	topic_ind = topicIndices.rdd.flatMap(extraxt)
	#group topics weight for word and assign to the max topic weight write to rd (word, (topic, weight))
	word_topic=topic_ind.map(lambda x : (x[1],(x[0],x[2]))).groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x : findMax(x))
	words_topic=word_topic.map(lambda x : (x[0], {'topic':x[1],'weight':x[2]}))
	words_topic.foreachPartition(write_terms)
	
	#group words associated with each topic with max weight
	topics_json = word_topic.map(lambda x: (x[1],{'word':x[0],'weight':x[2]})).groupByKey().map(lambda x : (x[0], list(x[1])))
        topics_json.foreachPartition(write_topics)
	sc.stop()

