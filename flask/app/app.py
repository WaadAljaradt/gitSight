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



@app.route('/search',methods=['POST'])
def search():
        str = request.form.get('search')
        stars = request.form.getlist('sort_by')
        if(str.contains(""))
        data =[]
        if stars:
                data= retrieve(str,2)
        else :
                data= retrieve(str,1)
        flag = 0
        if(not data):
                docs =[]
                tags=[]
                flash(u'No results were found, please try again!', 'error')
        else:
                docs=data[0]
                tags=data[1]

        return render_template("index.html",hits = docs ,tags=tags,value = str)



@app.route('/byTopic',methods=['GET'])
def byTopic():
        term = request.args.get('w')
        data = getRepos(term)
        if (not data):
                docs =[]
                tags=[]
                flash(u'No results were found, please try again!', 'error')
        else:
                docs=data[0]
                tags=data[1]
        return render_template("index.html",hits = docs ,value =term,tags=tags)
