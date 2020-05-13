
# coding: utf-8

# ## Import Packages

# In[1]:


import requests
import datetime
import psycopg2
import json
import dateutil.parser as parser
from datetime import datetime
from flask import Markup
from flask import Flask
from flask import render_template


# ## Calling API

# In[ ]:


# Set Start Date in ISO-1806 Format
date = datetime.strptime('Jan 1 2016  10:00AM', '%b %d %Y %I:%M%p')
StartTime= date.isoformat()

# Set End Date in ISO-1806 Format
date = datetime.strptime('May 8 2020  5:00PM', '%b %d %Y %I:%M%p')
EndTime= date.isoformat()

#Define Currencies
Currencies = ['BTC','ETH','XRP','LTC']

#Create Dictinary of responses along with it's key (Currency)
responses = {x: '' for x in Currencies}
ApiKey='17C82964-BB67-47A6-B5B5-69CBEABE84DB'
for Cur in Currencies:
    responses[Cur] = requests.get(
        'https://rest.coinapi.io/v1/ohlcv/'+Cur+'/USD/history?period_id=1DAY&time_start='+StartTime+'&time_end='+EndTime,
        headers={'Accept': 'application/json','X-CoinAPI-Key':ApiKey},)


# ## Initializing Connection to PostGres DB

# In[2]:


conn = psycopg2.connect(database="curren", user="postgres", password="P@ss1234")
cur = conn.cursor()


# ## Create Currencies DB

# In[ ]:


sql = '''CREATE TABLE curren(
         CUR_ID CHAR(10) NOT NULL,
         CUR_NAME CHAR(30)
      )'''
cur.execute(sql)


# ## Insert Currencies Values

# In[ ]:


sql = '''insert into curren values ('BTC','BitCoint')'''
cur.execute(sql)
sql = '''insert into curren values ('ETH','Ethereum')'''
cur.execute(sql)
sql = '''insert into curren values ('XRP','Ripple')'''
cur.execute(sql)
sql = '''insert into curren values ('LTC','LiteCoin')'''
cur.execute(sql)
conn.commit()


# ## Check that the insertion is Successfull

# In[ ]:


sql = """select * from curren"""
cur.execute(sql)
row = cur.fetchone()
while row is not None:
    print(row)
    row = cur.fetchone()


# ## Create Currency Trade History

# In[ ]:


sql = '''CREATE TABLE cur_trad_hist(
         cur_id CHAR(10),
         insert_ts TIMESTAMP,
         price_close FLOAT(3),
         price_high FLOAT(3),
         price_low FLOAT(3),
         price_open FLOAT(3),
         time_close TIMESTAMP,
         time_open TIMESTAMP,
         time_period_end TIMESTAMP,
         time_period_start TIMESTAMP,
         trades_count INT,
         volume_traded FLOAT(8))'''
cur.execute(sql)
conn.commit()


# ## I'm using this block whenever there is an error in the DB

# In[ ]:


#cur.execute("ROLLBACK")
conn.commit()


# ## Inserting the repsonses into the DB

# In[ ]:


for Cur in Currencies:
    #For each response of the responses
    for i in range(0,len(responses[Cur].json())):
        #change time close format to match DB input format
        datestr = responses[Cur].json()[i]['time_close'][:-9]
        newdate = datetime.strptime(datestr,'%Y-%m-%dT%H:%M:%S')
        time_close = newdate.strftime("%Y-%m-%d %H:%M:%S")

        #change time open format to match DB input format
        datestr = responses[Cur].json()[i]['time_open'][:-9]
        newdate = datetime.strptime(datestr,'%Y-%m-%dT%H:%M:%S')
        time_open = newdate.strftime("%Y-%m-%d %H:%M:%S")

        #change time period end format to match DB input format
        datestr = responses[Cur].json()[i]['time_period_end'][:-9]
        newdate = datetime.strptime(datestr,'%Y-%m-%dT%H:%M:%S')
        time_period_end = newdate.strftime("%Y-%m-%d %H:%M:%S")

        #change time period start format to match DB input format
        datestr = responses[Cur].json()[i]['time_period_start'][:-9]
        newdate = datetime.strptime(datestr,'%Y-%m-%dT%H:%M:%S')
        time_period_start = newdate.strftime("%Y-%m-%d %H:%M:%S")

        #get current time of insertion
        now = datetime.now() # current date and time
        date_time = now.strftime("%Y-%m-%d %H:%M:%S")
        
        #Build the SQL Statment
        sql = """INSERT INTO cur_trad_hist VALUES ('"""+Cur+"""','"""        +date_time+'''','''        +str(responses[Cur].json()[i]['price_close'])+""","""        +str(responses[Cur].json()[i]['price_high'])+""","""        +str(responses[Cur].json()[i]['price_low'])+""","""        +str(responses[Cur].json()[i]['price_open'])+""",'"""        +time_close+"""','"""        +time_open+"""','"""        +time_period_end+"""','"""        +time_period_start+"""',"""        +str(responses[Cur].json()[i]['trades_count'])+""","""        +str(responses[Cur].json()[i]['volume_traded'])+""")"""

        #Execute the SQL
        cur.execute(sql)
conn.commit()


# ## Make sure that the insertion is successfull

# In[3]:


sql = """select * from cur_trad_hist"""
cur.execute(sql)
row = cur.fetchone()
for i in range(0,10):
    print(row)
    row = cur.fetchone()


# ## Fill in values from DB to the Chart values

# In[3]:


labels = []
values = []

#Aggregating average trade count by month and year
sql = """select to_char(time_open,'Mon') as mon,extract(year from time_open) as yyyy, avg("trades_count") as "trades_count" from cur_trad_hist where cur_id='BTC' group by 1,2"""
cur.execute(sql)
roww = cur.fetchone()
while roww is not None:
    print(roww)
    label = roww[0]+"_"+str(int(roww[1]))
    labels.append(label)
    value = round(roww[2],2)
    values.append(value)
    roww = cur.fetchone()


# ## Calling Flask App to send values and labels to the Chart

# In[ ]:


app = Flask(__name__)


colors = [
    "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA",
    "#ABCDEF", "#DDDDDD", "#ABCABC", "#4169E1",
    "#C71585", "#FF4500", "#FEDCBA", "#46BFBD"]

@app.route('/')
def bar():
    bar_labels=labels
    bar_values=values
    return render_template('chart.html', title='BTC Bitcoin Monthly Average Number of Trades', max=30000, labels=bar_labels, values=bar_values)

if __name__ == "__main__":
    app.run()

