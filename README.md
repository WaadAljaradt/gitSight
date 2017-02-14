# gitSight <br /> 
Find related Repositories on github <br /> 
Search for repositories by topic <br /> 

Gitsight was my project for Insight Data Engineering, Winter 2017. <br /> 

Demo : <br /> 

[![](http://img.youtube.com/vi/0at2sHQquKg/0.jpg)]
(https://www.youtube.com/watch?v=0at2sHQquKg)

 <br /> 

Slide : <br /> 
http://www.slideshare.net/waadjaradat/insightdataengineeringgitsight <br /> 


 <br /> 
Motivation

Find interesting repositories on github based on discription modeling, also look up repositories based on topic key word, gitsight make it possible to search a repo name and get similar ones it also displays others word associated with this topic, the land page shows the top 200 keyword most frequent used in repositories discriptions. 

 <br />
It makes use of the following technologies:
 <br />
Google BigQuery (for obtaining raw GitHub source files) <br />
Avro <br />
AWS S3 <br />
Apache Spark 2.0 <br />
Redis  <br />
Flask  <br />
D3.js  <br />
The pipeline :

<img width="1279" alt="screen shot 2017-02-13 at 6 41 18 pm" src="https://cloud.githubusercontent.com/assets/8670178/22912783/1089051a-f21c-11e6-9f95-fae81be8967d.png">


Data : <br />
2015 and 2016 time line data serialized using avro and saved into S3 file system <br />

Process :
data is loaded into spark cleaned and parsed to extract relative fields such as description and login user for each one, 
the time line is then aggregated and deleted repositories are filtered out. 
also number of stars are aggregated and counted for each repositories throught out the time line data using<br />
map and reduce techniques. 
repositories with no or small description as filtered out similary repositories with stars number less than 10 
sd

