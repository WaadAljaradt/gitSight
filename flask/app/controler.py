import os.path
import redis
import sys
import imp
import json
RedisConfig = imp.load_source('RedisConfig', '/redis_conf/RedisConfig.py')
from RedisConfig import RedisConfig
from requests.auth import HTTPBasicAuth
from urllib2 import urlopen, Request
import requests
import time
from stop_words import get_stop_words
import string
import ast
import random

cfg = RedisConfig()
REDIS_PORT=cfg.REDIS_PORT
REDIS_IP=cfg.REDIS_IP
REDIS_PASS=cfg.REDIS_PASS
from nltk import PorterStemmer


def process(str):
        en_stop = get_stop_words('en')
        print 'str in process', str
        tokens = str.encode('ascii').split(' ')
        str=[]
        stopped_tokens = [i for i in tokens if not i in en_stop]
        for w in stopped_tokens :
                        for c in string.punctuation:
                                s=w.replace(c,"")
			ss=PorterStemmer().stem(s)
                        str.append(ss)
        return str




def getSimRepo(repo):
        json_response = []
        github_pass = cfg.github_pass
        try:
                request = Request(cfg.repo_url+repo)
                request.add_header('Authorization', 'token %s' % github_pass)
                response = urlopen(request, timeout=5)
                desc = json.loads(response.read())['description']
                res = process(desc)
                print 'res in getSimRepo', res
                if(res and len(res) > 0):
                        topicId = getMaxTop(res)
                        print 'in getSimRepo topicId=',topicId
                        return topicId
        except requests.exceptions.Timeout as e:
                time.sleep(30)
        except requests.exceptions.ConnectionError as e:
                time.sleep(30)
        except requests.exceptions.HTTPError as e:
                time.sleep(30)
        except Exception, e:
                print e


''' get related repos '''
def getRepoFromDbStr(data):
        res=[]
        for repo in data :
                d={}
                try:
                        jsonData =ast.literal_eval(repo)
                        json_data= ast.literal_eval(jsonData)
                        meta_data=json_data['data']
                        meta = json.loads(meta_data)
                        d['repo_name']= meta['repo_name'].encode('ascii')
                        url =  meta['url'].encode('ascii')
                        d['url']='https://github.com/'+url.split('/repos/', 1)[1]
                        d['desc']=meta['desc']
                        d['stars']=json_data['stars']
                        res.append(d)
                except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        print str(e)
                        print exc_tb.tb_lineno
                        pass
        return res


def getDocsbyTopicId(topicId,data_range):
        redis_db = redis.Redis(host=REDIS_IP, port=REDIS_PORT,password=REDIS_PASS, db=10)
        d =[]
        #random and default 
        if (data_range ==1 ):
                indx_list = random.sample(range(0,1000), 30)
                for indx in indx_list:
                        try :
                                value = redis_db.lindex(topicId,indx)
                                d.append(value)
                        except :
                                pass
        elif (data_range == 2): #top 30 
                d = redis_db.lrange(topicId,0,29)

        return d


def getTags(topic_id):
        redis_db = redis.Redis(host=REDIS_IP, port=REDIS_PORT,password=REDIS_PASS, db=13)
        lis = redis_db.get(topic_id)
        tags =ast.literal_eval(lis)
        return tags


def retrieve(str,data_range):
        ''' get topic of repo '''
        redis_db = redis.Redis(host=REDIS_IP, port=REDIS_PORT,password=REDIS_PASS, db=12)
        topic_id =redis_db.get(str)
        if (not topic_id):
                topic_id = getSimRepo(str)
        if (topic_id):
                repos = getDocsbyTopicId(topic_id,data_range)
                data=getRepoFromDbStr(repos)
                tags = getTags(topic_id)
                return (data,tags)
