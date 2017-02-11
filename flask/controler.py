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
