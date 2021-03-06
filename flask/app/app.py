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
    if('/' not in str.encode('ascii')):
         print 'here'
         data = getRepos(str)
         if (not data):
             docs =[]
             tags=[]
             flash(u'No results were found, please try again!', 'error')
         else:
             docs=data[0]
             tags=data[1]
         return render_template("index.html",hits = docs ,value =str,tags=tags)
    data =[]
    if stars:
        data= retrieve(str,2)
    else :
        data= retrieve(str,1)
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


@app.route('/getGraph')
def getGraph():
        t = request.args.get('t')
        print t
        return render_template("graph.html",topic_id=t)

@app.route('/jGraph')
def getjGraph():
        t = request.args.get('t')
        graph = jGraph(t)
        print graph
        return graph

@app.route('/getBytopicId')
def getRepoByTopicId():
        topic_id = request.args.get('t')
        if not topic_id :
                docs =[]
                tags=[]
                topic_id=""
                flash(u'No results were found, please try again!', 'error')
                return render_template("index.html",hits = docs ,value ="",tags=tags, topic_id=topic_id)
        data = getByTopicId(topic_id)
        if (not data):
                docs =[]
                tags=[]
                topic_id=""
                flash(u'No results were found, please try again!', 'error')

        else:
                docs=data[0]
                tags=data[1]
                topic_id=data[2]
        return render_template("index.html",hits = docs ,value ="",tags=tags, topic_id=topic_id)
