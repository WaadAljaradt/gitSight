from flask import Flask, flash, render_template, jsonify,request
import sys
import json
from controler import *
import redis
import imp


RedisConfig = imp.load_source('RedisConfig', 'redis_conf/RedisConfig.py')

from RedisConfig import RedisConfig

cfg = RedisConfig()
app = Flask(__name__)
app.secret_key = 'some_secret'

@app.route('/')
def index():
        words = getWords()
        return render_template("bubble.html",value ="",words=words)
