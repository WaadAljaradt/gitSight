import sys
import redis
from operator import add
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark import SparkContext,Row,SQLContext
from pyspark.ml.feature import StopWordsRemover
from itertools import islice, cycle
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import *
import csv
import  json
from riak import RiakClient
from elasticsearch import Elasticsearch
from pyspark.ml.clustering import LDA
import re , string
from pyspark.ml.feature import CountVectorizer
sys.path.insert(0, 'Insigh-DataEngineering-gitsight/src/main/redis_conf')
from RedisConfig import RedisConfig
import gc
from datetime import datetime
cfg = RedisConfig()
SPARK_IP = cfg.SPARK_IP
SPARK_PORT = cfg.SPARK_PORT


SPARK_APP_NAME = cfg.SPARK_APP_NAME


RIAK_IP = cfg.RIAK_IP
RIAK_PORT = cfg.RIAK_PORT

ES_IP = cfg.ES_IP
ES_PORT = cfg.ES_PORT
ELPASS=cfg.ELPASS
ELUSER=cfg.ELUSER
es = Elasticsearch([{'host': ES_IP, 'port': ES_PORT}], http_auth=(ELUSER, ELPASS), verify_certs=False, timeout=120)

AWS_ACCESS_KEY_ID = cfg.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = cfg.AWS_SECRET_ACCESS_KEY

S3_BUCKET = cfg.S3_BUCKET
S3_Month__BLOB = cfg.S3_Month__BLOB
# Setup context
###############################################################################
conf = SparkConf() \
    .setMaster("spark://%s:%s" %
        (SPARK_IP, SPARK_PORT)) \
    .setAppName(SPARK_APP_NAME)
sc = SparkContext(conf=conf)
sc_sql = SQLContext(sc)
###############################################################################

REDIS_IP=sc.broadcast(cfg.REDIS_IP)
REDIS_PORT=sc.broadcast(cfg.REDIS_PORT)
REDIS_PASS =sc.broadcast(cfg.REDIS_PASS)

typesin = ['CreateEvent', 'DeleteEvent','WatchEvent','PushEvent']
types = sc.broadcast(typesin)
df= sc_sql.read.format("com.databricks.spark.avro").load(
    "s3a://%s:%s@%s/%s" %
    (AWS_ACCESS_KEY_ID,
     AWS_SECRET_ACCESS_KEY,
     S3_BUCKET,'2016*.avro')).rdd.filter(lambda x: x.type in types.value).persist(StorageLevel(True, True, False, False, 1))

first_re = re.compile(r'my first')
demo_re =re.compile(r'demo')
test_re = re.compile(r'test')
sample_re = re.compile(r'sample')
first = sc.broadcast(first_re) 
demo = sc.broadcast(demo_re)
test = sc.broadcast(test_re)
sample = sc.broadcast(sample_re)

def filDesc(x):
	
	repoDesc = json.loads(x.payload)['description']
 	if( (not repoDesc) or (len(repoDesc.split()) <4) or
                (first.value.search(repoDesc))or  (demo.value.search(repoDesc) and len(repoDesc.split())<10) or
                ( test.value.search(repoDesc) and  len(repoDesc.split()) < 10)or
                (sample.value.search(repoDesc) and len(repoDesc.split()) < 10)):
                        return True
	return False

pre_docs  = df.filter(lambda x : x.type == 'CreateEvent')
deleted = df.filter(lambda x : x.type == 'DeleteEvent' and json.loads(x.payload)['ref_type']=='branch' and json.loads(x.payload)['ref']=='master').map(lambda x :x.repo.id)

deleted_repo = sc.broadcast(deleted.collect())
#deleted.registerTempTable('deleted')
deleted.unpersist()
docs = pre_docs.filter(lambda x : x.repo.id not in deleted_repo.value).map(lambda x  : (x.repo.id,{'repo_name':x.repo.name.encode('utf-8'),'desc':json.loads(x.payload)['description'].encode('utf-8'),'actor':  x.actor.login.encode('utf-8'),'url':x.repo.url.encode('utf-8')}))
del deleted_repo

docs = pre_docs.filter(lambda x :not filDesc(x))
pre_docs.unpersist()
cons = docs.map(lambda x :x.repo.id).collect()

first.unpersist()
demo.unpersist()
test.unpersist()
sample.unpersist()

repos = sc.broadcast(cons)

print docs.take(2)
	
watch = df.filter(lambda x : x.type == 'WatchEvent' and x.repo.id  in repos.value)
push = df.filter (lambda x : x.type == 'PushEvent' and x.repo.id  in repos.value)

del cons
repos.unpersist()
deleted.unpersist()
df.unpersist()

latest = push.map(lambda x: (x.repo.id,str(x.created_at))).reduceByKey(max)
push.unpersist()
stars_count = watch.map(lambda x : (x.repo.id,1)).reduceByKey(add)
watch.unpersist()


starred =stars_count.map(lambda x : x[0]).collect()
updated = latest.map(lambda x : x[0]).collect()

#remove repos that are not starred or never been updated 


stars = sc.broadcast(starred)
updates = sc.broadcast(updated)
dis_docs = docs.filter(lambda x : x[0] in stars.value and x[0] in updates.value)
key_docs = dis_docs.groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x : (x[0],x[1][0]))
key_lates = latest.filter(lambda x : x[0] in stars.value and x[0] in updates.value)
key_stars = stars_count.filter(lambda x : x[0] in stars.value and x[0] in updates.value)

stars_count.unpersist()
latest.unpersist()
docs.unpersist()
stars.unpersist()
updates.unpersist()

data = key_docs.union(key_lates).union(key_stars)
key_stars.unpersist()
key_lates.unpersist()
key_docs.unpersist()
stars_count.unpersist()
latest.unpersist()

data = data.groupByKey().map(lambda x : (x[0], list(x[1])))
#def repo_name(repos):
#	for repo in repos:
		

def write_to_redis(items):
	redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=1)
	raw_data={}
	for i in items:
		for data in i[1]:
			if isinstance(data, str):
				raw_data['created_at']=data
			elif isinstance(data,dict):
				json_meta = json.dumps(data)
				raw_data['meta']=json_meta
			elif isinstance(data,int):
				raw_data['stars']=data
		json.dumps(raw_data, ensure_ascii=False)
        	redis_db.set(i[0], raw_data)
		print 'insert',  redis_db.get(i[0])
	yield None

d = data.mapParitions(repo_name)
d.foreachPartition(write_to_redis)
print "collect data DONE"
sc.stop()
	

