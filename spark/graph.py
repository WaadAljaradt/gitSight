import sys
import ast
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
def getRepos(topic_id):
        redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=10)
        repos = redis_db.lrange(topic_id, 0, -1 )
        return repos

def getSize():
        redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=10)
        size = redis_db.dbsize()
        return size

def getRowOfForks(x):
        #s = json.loads(x.payload)['repository']
        repo = x.repo.name
        user= x.actor.login
        return Row(repo=repo,user=user)
def getName(repo):
        jsonData =ast.literal_eval(repo)
        json_data= ast.literal_eval(jsonData)
        meta_data=json_data['data']
        meta = json.loads(meta_data)
        return  meta['repo_name'].encode('ascii')


def write_edges(records):
        redis_db = redis.Redis(host=REDIS_IP.value, port=REDIS_PORT.value,password=REDIS_PASS.value, db=8)
        for e in records:
                        edge ={'usera':e.userA.encode('ascii'),'userb':e.userB.encode('ascii')}
                        #print edge, e.topic_id, 'in Redis'
                        redis_db.rpush(e.topic_id, json.dumps(edge))
                        #print 'insert',  redis_db.rpop(e.topic_id)
        return True




df= sc_sql.read.format("com.databricks.spark.avro").load(
    "s3a://%s:%s@%s/%s" %
    (AWS_ACCESS_KEY_ID,
     AWS_SECRET_ACCESS_KEY,
    S3_BUCKET,'*.avro')).rdd.filter(lambda x: x.type == 'WatchEvent').persist(StorageLevel(True, True, False, False, 1))

#get repos for each topic
size = range(getSize())

keys = sc.parallelize(size)
repos = keys.flatMap(lambda x:(x, getRepos(x)))
 
re_repos = repos_rdd.map(lambda x : getName(x))
repo_user=re_repos.map(lambda x :Row(repo=x.split("/")[1],user= x.split("/")[0]))

repo_user_tbl = sc_sql.createDataFrame(repo_user)
repo_user_tbl.registerTempTable("repo_user_tbl")
#get star events relevant to repos in db
        
stars = df.map(lambda x : Row(topic_id = id,userA=x.actor.login, userB=x.repo.name.split("/")[0]))
stars_tbl = sc_sql.createDataFrame(stars)
stars_tbl.registerTempTable("stars_tbl")

edges = sc_sql.sql("Select * FROM stars_tbl t1 where t1.userA in  (select user from repo_user_tbl) and t1.userB in (select user from repo_user_tbl)" ) 

edges.rdd.foreachPartition(write_edges)
print edges.take(1)
