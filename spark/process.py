import sys
import redis
from operator import add
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark import SparkContext,Row,SQLContext
from pyspark.ml.feature import StopWordsRemover
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import *
import  json
import re , string
sys.path.insert(0, 'Insigh-DataEngineering-gitsight/src/main/redis_conf')
from RedisConfig import RedisConfig
from datetime import datetime
cfg = RedisConfig()

SPARK_IP = cfg.SPARK_IP
SPARK_PORT = cfg.SPARK_PORT
SPARK_APP_NAME = cfg.SPARK_APP_NAME


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


#remove repositories that have less information in their description
def filDesc(x):
        first = re.compile(r'my first')
        demo =re.compile(r'demo')
        test = re.compile(r'test')
        sample = re.compile(r'sample')
        repoDesc = x.data['desc']
        if( (not repoDesc) or (len(repoDesc.split()) <4) or
                (first.search(repoDesc))or  (demo.search(repoDesc) and len(repoDesc.split())<10) or
                ( test.search(repoDesc) and  len(repoDesc.split()) < 10)or
                (sample.search(repoDesc) and len(repoDesc.split()) < 10)):
                        return True
        return False

#create serialized objects to redis db
def write_to_redis(items):
        redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=1)
        raw_data={}
        for i in items:
                raw_data['data']= json.dumps(i.data)
                raw_data['stars']= i.stars
                repo_name =i.data['repo_name']
                raw_data['id']=i.id
                json.dumps(raw_data, ensure_ascii=False)
                redis_db.set(repo_name, raw_data)
                print 'insert',  redis_db.get(repo_name)
        yield None


#Read Data from S3

typesin = ['CreateEvent', 'DeleteEvent','WatchEvent']
types = sc.broadcast(typesin)
df= sc_sql.read.format("com.databricks.spark.avro").load(
    "s3a://%s:%s@%s/%s" %
    (AWS_ACCESS_KEY_ID,
     AWS_SECRET_ACCESS_KEY,
     S3_BUCKET,'*.avro')).rdd.filter(lambda x: x.type in types.value).persist(StorageLevel(True, True, False, False, 1))

#Get repositories with their meta data 
#Also get deleted ones to filter them out 
pre_docs  = df.filter(lambda x : x.type == 'CreateEvent')
deleted = df.filter(lambda x : x.type == 'DeleteEvent' \
and json.loads(x.payload)['ref_type']=='branch' and \
json.loads(x.payload)['ref']=='master').map(lambda x :Row(id=x.repo.id))


docs = pre_docs.map(lambda x  : Row(id =x.repo.id,\
data={'repo_id':x.repo.id,'repo_name':x.repo.name.encode('utf-8'),'desc':\
json.loads(x.payload)['description'],'actor':  x.actor.login.encode('utf-8'),\
'url':x.repo.url.encode('utf-8')}))

pre_docs.unpersist()

#create a SQL data frame for querying
docs_df = sc_sql.createDataFrame(docs)
del_df =sc_sql.createDataFrame(deleted)

#remove deleted docs
docs_df.registerTempTable("repos")
del_df.registerTempTable("deletedRepos")
valid_repos = sc_sql.sql("Select * FROM repos t1 where not exists (select 1 from deletedRepos t2 where t1.id = t2.id )" )
#filter repos based on description
key_repo = valid_repos.rdd.filter(lambda x :not filDesc(x))
docs.unpersist()
valid_repos.unpersist()

#get number of stars for every repositories 
watch = df.filter(lambda x : x.type == 'WatchEvent')


stars_count = watch.map(lambda x : Row(x.repo.id,1)).reduceByKey(add).filter(lambda x :x[1]>10) \
.map(lambda x : Row(id=x[0], stars=x[1]))
stars =sc_sql.createDataFrame(stars_count)
stars.registerTempTable("starred")

key_repo_df =sc_sql.createDataFrame(key_repo)
key_repo_df.registerTempTable("keyRepos")

#get star numbers for each repo
starred_repos = sc_sql.sql("Select * FROM keyRepos t1 join starred t2 on t1.id = t2.id" )
#write data to redis
starred_repos.foreachPartition(write_to_redis)

print "collect data DONE"
sc.stop()
