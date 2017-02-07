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
RedisConfig = imp.load_source('RedisConfig', '/home/ubuntu/Insigh-DataEngineering-gitsight/src/main/redis_conf/RedisConfig.py')
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
		json_data =  ast.literal_eval(doc[1])
		desc = ast.literal_eval(json_data['meta'])['desc']
		name = ast.literal_eval(json_data['meta'])['repo_name']
		result.append(Row(idd=name,words=desc.split(' ')))
	return result

def stem_words(wordList) :
	rows =[]
	for word in wordList:
		strs =[]
        	for w in word[1] :
                	s = w.encode('ascii')
                	for c in string.punctuation:
                        	s=s.replace(c,"")
                	strs.append(s)
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
        meta = ast.literal_eval(json_data['meta'])
	name = meta['repo_name']
	stars = json_data['stars']
	updated =datetime.fromtimestamp(int(json_data['created_at'])/1000000).strftime('%Y-%m-%d %H:%M:%S')
	url =meta['url']
	actor=meta['actor']
	res = (name,(data[0],stars, updated, url, actor))
	return res 
	
def extraxt(x):
        d=[]
        for word, weight in zip (x[1],x[2]):
                w = vocabArray[word].encode('ascii')
                d.append((x[0],w,weight))
        return d

def write_time(record):
        redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=2)
	redis_db.set(record[0], record[1])
        print 'insert',  redis_db.get(record[0])
	return True

def write_stars(record):
        redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=3)
	redis_db.set(record[0], record[1])
	print 'insert',  redis_db.get(record[0]) 
	return True 

def write_terms(word):
	print 'hey insert_t', word
	redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=4)
	redis_db.set(word[1], (word[0],word[2]))
	print 'term_insert',  redis_db.get(word[1])
	print 'hey in write',word
	return True

def repo_topic(trans):
	redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=5)
	res=[]
	for record in trans:
		key = record.idd.encode('ascii')
		value = record.topicDistribution.argmax()
		redis_db.set(key, value)
		res.append((key, value))
	return res
def write_pairs(record):
        redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=6)
        redis_db.set(record[0], record[1])
        print 'insert',  redis_db.get(record[0])
        return True

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
		result.append((topic_id,json_obj))
	return result
def bateekh(word):
	print 'zeft',word
	return True

			
if __name__ == "__main__":
       	REDIS_IP=sc.broadcast(cfg.REDIS_IP)
	REDIS_PORT=sc.broadcast(cfg.REDIS_PORT)
	REDIS_PASS =sc.broadcast(cfg.REDIS_PASS)
	redis_db = redis.Redis(host=cfg.REDIS_IP, port=cfg.REDIS_PORT,password=cfg.REDIS_PASS, db=1)
	keys = redis_db.keys()
	rawData = sc.parallelize(keys)
	data = rawData.mapPartitions(getValues)
	rawData.unpersist()
	docs = data.mapPartitions(process)
	docDF = sc_sql.createDataFrame(docs)
	docs.unpersist()
	res = StopWordsRemover(inputCol="words", outputCol="filtered").transform(docDF)
	df = res.drop("words")
	docDF.unpersist()
	stem  = df.rdd.mapPartitions(lambda x :stem_words(x))
	df.unpersist()
	df =  sc_sql.createDataFrame(stem)
	Vector = CountVectorizer(inputCol="words", outputCol="features")
	model = Vector.fit(df)
	result = model.transform(df)
	lda = LDA(k=20, maxIter=10,optimizer='em')
	ldaModel = lda.fit(result)
	transformed = ldaModel.transform(result)
	trans = transformed.rdd.mapPartitions(repo_topic)
	del transformed
	repo_names = data.map(lambda x : doc_name(x))
	ful_data = trans.union(repo_names)
	collect_data = ful_data.groupByKey().map(lambda x : (x[0], list(x[1])))
	print collect_data.take(1)
	sim_doc = collect_data.mapPartitions(similar_repo).groupByKey().map(lambda x : (x[0], list(x[1])))
	sort_time = sim_doc.map(lambda x : (x[0],sorted(x[1], key=lambda tup: datetime.strptime(tup['updated'], '%Y-%m-%d %H:%M:%S'),reverse=True)))
	sort_stars = sim_doc.map(lambda x : (x[0],sorted(x[1], key=lambda tup:tup['stars'],reverse=True)))
	write = sort_time.map(lambda x : write_time(x))
	print write.take(2)
	sort_stars.foreach(write_stars)
	'''vocabulary'''
	vocabArray = model.vocabulary
	topicIndices = ldaModel.describeTopics(5)
	print("The topics described by their top-weighted terms:")
	topicIndices.show(truncate=False)
	topics = ldaModel.topicsMatrix()
	topic_ind = topicIndices.rdd.flatMap(extraxt)
	print  topic_ind.take(2),'indeces'
	res =topic_ind.map(write_terms).collect()
	print res
	redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=4)
	topic_words=topic_ind.map(lambda x : (x[0],(x[1],x[2]))).groupByKey().map(lambda x : (x[0], list(x[1])))
	del topic_ind
	res = topic_words.map(lambda x : write_pairs(x))
	redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=6)
	#print res.take(2)
	sc.stop()

