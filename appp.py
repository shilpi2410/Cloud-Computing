
from flask import Flask,render_template,request,jsonify
import pymysql
import time
import hashlib
import json
import csv
import random
from datetime import datetime
import sys,os
from dateutil import parser
import redis
rediscache = redis.StrictRedis(host='.redis.cache.windows.net',
      port=, db=0, password='', ssl=True, charset="utf-8",  decode_responses=True)

for key in rediscache.keys():
   print(key)
  

headers=[]

username = ''
password = ''
database = ''
myConn = pymysql.connect(host='X.azure.com', user=username, passwd=password,db=database,local_infile=True)

app = Flask(__name__)


@app.route('/')
def index():
     return render_template('index.html')

# creating table using the csv and importing the data
@app.route('/createtable',methods=['POST'])
def createtable():
    
    cursor = myConn.cursor()
    file_name = 'C:/Users/shilpi/Desktop/USZipcodes.csv'
    droptbl = "DROP TABLE IF EXISTS Project2.uszipcodes;"
    cursor.execute(droptbl)
    with open(file_name, 'rt', encoding = 'Latin-1') as csvfile:
        reader = csv.reader(csvfile,quotechar='`')
        headers = next(reader)
    
    start_time = time.time()
    
    sqlcreate="create table if not exists uszipcodes("
    for i in range(0, len(headers)):
         sqlcreate +=  headers[i] + " varchar(100),"
    sqlcreate += "columnID int AUTO_INCREMENT PRIMARY KEY)"
    cursor.execute(sqlcreate)
    
    uploadqry="""LOAD DATA LOCAL INFILE 'C:/Users/shilpi/Desktop/USZipcodes.csv'
          INTO TABLE uszipcodes FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\r' IGNORE 1 ROWS;"""
    cursor.execute(uploadqry)
    myConn.commit()
    
    end_time = time.time()
    time_diff = end_time - start_time
    

    return render_template('index.html',time_taken=time_diff,success = "Data inserted into database")



@app.route('/rcache',methods=['POST'])
def memcac():
   cursor = myConn.cursor()
   city = request.form['count']
   result=[]
   results =[]
   print(city)
   foundmeme  = "yes"
   count = 0
   start_time = time.time()
   result =rediscache.lrange(city, 0, rediscache.llen(city))
   
   if not result:
       print("Not Present")
       sqlselect = "SELECT Name, city, CountryCode FROM starbucks WHERE CountryCode ='"+city+"'";
       cursor.execute(sqlselect)
       result = cursor.fetchall()
       print(result)
       for row in result:
          count = count+1
          rediscache.lpush(city, row)
       foundmeme  = "None"
   # else:
   #     print("LOL")
   #     print(result)
   count = rediscache.llen(city)
   end_time = time.time()
   time_diff = end_time - start_time
   
   
   #for row in result
   #print (row)
    #count = count + 1
   #     print(row)
   #     results.append(str(row))
       #results.append(str(row[0]))

   return render_template('index.html', resu=result, foundm=foundmeme, total_time = time_diff,counter = count)






@app.route('/fetchmag', methods=['POST'])
def fetchmag():
    magnitude=[]
    magnitudetype = request.form['magnitude']
    magnituderange = request.form['magRange']

    #magnituderange = float(magnituderange)
    cursor=myConn.cursor()
    start_time = time.time()

    query2 = "SELECT place FROM citydata where magType= %s and mag>= %s"
    cursor.execute(query2,(magnitudetype,magnituderange))
    end_time =time.time()
    time_diff = end_time - start_time
    magdata = cursor.fetchall()
   
    count=0
    for row in magdata:
        count=count+1
        magnitude.append("mag:"+row[0])
    return render_template('index.html', counter=count, totaltime = time_diff)


@app.route('/random', methods=['POST'])
def randomFunc():
    magnitude=[]
    count = request.form['count']
    #magnituderange = float(magnituderange)
    cursor=myConn.cursor()

    start_time = time.time()
    for i  in range(1,int(count)+1):
        rand = random.randrange(0,100)
        mag = int(rand)
        #print(mag)
        query2 = "SELECT place FROM citydata where depth>= %s"
        cursor.execute(query2,mag)
    end_time =time.time()
    time_diff = end_time - start_time
    magdata = cursor.fetchall()
    
    count=0
    for row in magdata:
        count=count+1
        magnitude.append("Place:"+row[0])
    return render_template('index.html', countr2=count,resu2 = magnitude,totaltimer = time_diff)


# Fetching the earthquake data where magnitude between 3 and 6
@app.route('/fetch',methods=['POST'])
def fetch():
    res=[]
    lat = request.form['lat']
    lon = request.form['lon']
    
    newlat=float(lat)+2
    newlon = float(lon)- 2
    
    cursor = myConn.cursor()
    start_time = time.time()
    query1 = "SELECT place FROM citydata where latitude between "+ str(lat)+" and "+str(newlat)+" and longitude between "+str(newlon)+" and "+str(lon)
    cursor.execute(query1)
    end_time = time.time()
    time_diff = end_time - start_time
    data = cursor.fetchall()
    count=0
    for row in data:
        count=count+1
        # res.append("Latitude:"+ row[1]+ "  ; Longitude:"+row[2])
        res.append("Place:"+row[0])
    return render_template('index.html', res=res,count=count, totaltime = time_diff)


@app.route('/innerjoin',methods=['POST'])
def innerjoin():
    result=[]
    #count = request.form['count']   
    
    cursor = myConn.cursor()
    start_time = time.time()
    query1 = "SELECT uszipcodes.county, starbucks.name FROM uszipcodes INNER JOIN starbucks ON uszipcodes.city = starbucks.city Limit 10"
    cursor.execute(query1)
    end_time = time.time()
    time_diff = end_time - start_time
    data = cursor.fetchall()
    count=0
    for row in data:
        count=count+1
        # res.append("Latitude:"+ row[1]+ "  ; Longitude:"+row[2])
        result.append("country:"+row[0]+"name:"+row[1])
    return render_template('index.html', iresu=result, icount=count, itime = time_diff)


# query
#select * from citydata where magType = 'ml' and mag >= '1.0' Limit 4000




port = os.getenv('PORT', '8080')
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(port))