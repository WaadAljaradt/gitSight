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
