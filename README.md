# gitSight <br /> 
Find related Repositories on Github <br /> 
Search for repositories by topic <br /> 

<a href="gitsight.site">gitsight application</a> <br />
Gitsight was my project for Insight Data Engineering, Winter 2017. <br /> 

# Demo : <br /> 

[![](http://img.youtube.com/vi/0at2sHQquKg/0.jpg)]
(https://www.youtube.com/watch?v=0at2sHQquKg)

 <br /> 

# Slides : <br /> 
http://www.slideshare.net/waadjaradat/insightdataengineeringgitsight <br /> 


 <br /> 
# Motivation

Find interesting repositories on Github based on description modeling, also look up repositories based on topic key word, <br /> gitsight makes it possible to search a repository name and get similar ones. It also displays other word associated with this topic. <br /> The land page shows the top 200 keyword most frequent used in repositories descriptions.  <br />


#Technologies <br />
It makes use of the following technologies:
 <br />
Google BigQuery (for obtaining raw GitHub source files) <br />
Avro <br />
AWS S3 <br />
Apache Spark 2.0 <br />
Redis  <br />
Flask  <br />
D3.js  <br />

# pipeline :
<br />
<img width="1279" alt="screen shot 2017-02-13 at 6 41 18 pm" src="https://cloud.githubusercontent.com/assets/8670178/22912783/1089051a-f21c-11e6-9f95-fae81be8967d.png">

<br />
# Data : <br />
About 1.5 TB of 2015 and 2016 time line data serialized using Avro and saved into S3 file system  <br />

# pre-process :<br />
Data is loaded into Spark, cleaned, and parsed to extract relative fields such as description and login user for each one. <br />
The time line is then aggregated and deleted repositories are filtered out. <br />
also number of stars are aggregated and counted for each repositories through out the time line data using<br />
map and reduce techniques. <br />
repositories with no or small description as filtered out similarly repositories with stars number less than 10 <br />

# LDA modeling: <br />
The data is fed into a topic modeling technique, LDA was chosen because it is a distributed algorithm that counts words in<br /> documents (descriptions).<br />

The topic-repository matrix is retrieved and for each topic associated repositories are aggregated and similar words with high weights for each topic as well. <br />

# Database :<br />

Data is then saved into  Key-value Redis forfast querying, the data was denormalized and was saved redundantly to account for different queries the user enters to the system. <br />

(topic_id, repos)<br />
(repo, topic_id)<br />
(word, topic_id)<br />
(topic_id, words) <br />


# Graph visualization <br />
Owners of these repositories were represented as nodes. Star events happening between them were aggregated to create <br /> links of starring between these nodes. <br />
The more dense the graph is the more interactions took place between these repositories in each topic <br />



