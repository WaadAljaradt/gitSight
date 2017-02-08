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

typesin = ['CreateEvent', 'DeleteEvent','WatchEvent']
types = sc.broadcast(typesin)
df= sc_sql.read.format("com.databricks.spark.avro").load(
    "s3a://%s:%s@%s/%s" %
    (AWS_ACCESS_KEY_ID,
     AWS_SECRET_ACCESS_KEY,
     S3_BUCKET,'*.avro')).rdd.filter(lambda x: x.type in types.value).persist(StorageLevel(True, True, False, False, 1))

first_re = re.compile(r'my first')
demo_re =re.compile(r'demo')
test_re = re.compile(r'test')
sample_re = re.compile(r'sample')
first = sc.broadcast(first_re) 
demo = sc.broadcast(demo_re)
test = sc.broadcast(test_re)
sample = sc.broadcast(sample_re)

def filDesc(x):
	
	repoDesc = x[1]['desc']
 	if( (not repoDesc) or (len(repoDesc.split()) <4) or
                (first.value.search(repoDesc))or  (demo.value.search(repoDesc) and len(repoDesc.split())<10) or
                ( test.value.search(repoDesc) and  len(repoDesc.split()) < 10)or
                (sample.value.search(repoDesc) and len(repoDesc.split()) < 10)):
                        return True
	return False

pre_docs  = df.filter(lambda x : x.type == 'CreateEvent')
deleted = df.filter(lambda x : x.type == 'DeleteEvent' and json.loads(x.payload)['ref_type']=='branch' and json.loads(x.payload)['ref']=='master').map(lambda x :x.repo.id).collect()

deletedId = sc.broadcast(deleted)
print deletedId

docsex = pre_docs.filter(lambda x : x.repo.id not in deletedId.value).map(lambda x  : (x.repo.id,{'repo_id':x.repo.id,'repo_name':x.repo.name.encode('utf-8'),'desc':json.loads(x.payload)['description'],'actor':  x.actor.login.encode('utf-8'),'url':x.repo.url.encode('utf-8')}))
docs = docsex.groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x :  (x[0],x[1][0]))
docsex.unpersist()

del deleted
pre_docs.unpersist()

repos = docs.filter(lambda x :not filDesc(x))
docs.unpersist()

cons = repos.map(lambda x :x[0]).collect()

first.unpersist()
demo.unpersist()
test.unpersist()
sample.unpersist()

val_repo = sc.broadcast(cons)
	
watch = df.filter(lambda x : x.type == 'WatchEvent' and x.repo.id  in val_repo.value)
df.unpersist()

stars_count = watch.map(lambda x : (x.repo.id,1)).reduceByKey(add)
watch.unpersist()
del cons
del val_repo

#remove repos that have stars less than 10

dis_docs=repos.union(stars_count)
stars_count.unpersist()
repos.unpersist()

data = dis_docs.groupByKey().map(lambda x : (x[0], list(x[1]))).filter(lambda x : len(x[1])>1)

dis_docs.unpersist()

print data.take(10)
	
	
def write_to_redis(items):
	redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=1)
	raw_data={}
	for i in items:
		for data in i[1]:
			if isinstance(data,dict):
				repo_name = data['repo_name']
				json_meta = json.dumps(data)
				raw_data['meta']=json_meta
			elif isinstance(data,int):
				raw_data['stars']=data
		json.dumps(raw_data, ensure_ascii=False)
        	redis_db.set(repo_name, raw_data)
		print 'insert',  redis_db.get(repo_name)
	yield None

#d = data.mapPartitions(repo_name)
#data.foreachPartition(write_to_redis)

print "collect data DONE"
sc.stop()
	

