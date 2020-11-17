# tunaiku_test

Task with python. for more details you can check Full Code in [tunaiku.ipnyb](https://github.com/jodiesamuel/tunaiku_test/blob/main/README.md) files.

## Dependecies

- pip
- psycopg2
- localhost PostgreSQL server
- Jupyter Notebook (training data)
- Pandas
- Numpy
- email
- threading
- google.cloud
- sys
- sqlalchemy

## Questions:

- read a config file having flags, and create metric def based on flag values
example config file : here
Based on flag values, create metrics for all the demo_codes and store them in a python dict
hint : use system modules and try to store the file data in a dict as a starting point

Import the dependencies and download the metrix then change it into json files so it's easy to save as python dict

```
import pandas

excel_data_df = pandas.read_excel('/home/jodie/Downloads/config_metric.xlsx')

json_str = excel_data_df.to_json()
```
![alt text](https://github.com/jodiesamuel/tunaiku_test/blob/main/pictures/Screenshot%20from%202020-11-17%2015-53-01.png)

## Questions:

- split a time interval into equal portions
Example input >> start_time:  2020-03-02 13:02:00 end_time : 2020-03-02 15:55:07

Input the begin & end date and devided into equal portions and devided into 2 tables with start_time and end_time
```
begin = '2020-03-02 13:02:00'
end = '2020-03-02 15:55:07'

def date_range(start, end, intv):
    from datetime import datetime
    start = datetime.strptime(start,"%Y-%m-%d %H:%M:%S")
    end = datetime.strptime(end,"%Y-%m-%d %H:%M:%S")
    diff = (end  - start ) / intv
    for i in range(intv):
        yield (start + diff * i).strftime("%Y-%m-%d %H:%M:%S")
    yield end.strftime("%Y-%m-%d %H:%M:%S")
    
start_time = list(date_range(begin, end, 6))
del start_time[-1]

end_time = list(date_range(begin, end, 6))
del end_time[0]

time = pd.Series(data=start_time,name='start_time').to_frame()
time['end_time'] = pd.Series(data=end_time,name='end_time').to_frame()
```
![alt text](https://github.com/jodiesamuel/tunaiku_test/blob/main/pictures/Screenshot%20from%202020-11-17%2015-56-00.png)

## Questions:

- for any input date(YYYY-MM-DD format), print week starting and weekend date, consider week starting as monday and week ending as sunday

Change the date to define if monday is "weekday" and sunday is "weekend"
```
import numpy as np

x = pd.date_range('2020-11-09', '2020-11-16').to_series()
x = x.dt.dayofweek.to_frame()
x = x.rename(columns={0: "week"})

x.loc[x['week'] == 0] = 'weekday'
x.loc[x['week'] == 6] = 'weekend'
x.loc[(x['week'] == 1)|(x['week'] == 2)|(x['week'] == 3)|(x['week'] == 4)|(x['week'] == 5)] = ''
```

![alt text](https://github.com/jodiesamuel/tunaiku_test/blob/main/pictures/Screenshot%20from%202020-11-17%2016-02-37.png)

## Questions:

- add a new column to a bigquery table, whenever there is a notification/email alert occurs, example : add column x to the bq table when a email alert comes with the email body mentioning the same

Adding column to BQ with body email notifications with example mail
```
import email

from email.parser import HeaderParser
with open('/home/jodie/Downloads/Mail1',"r",encoding="ISO-8859-1") as f:
    msg=email.message_from_file(f)
    print('message',msg.as_string())
    parser = HeaderParser()
    h = parser.parsestr(msg.as_string())
```
![alt text](https://github.com/jodiesamuel/tunaiku_test/blob/main/pictures/Screenshot%20from%202020-11-17%2016-05-25.png)

After that get only the body and update to the bigquery columns
```
x = h.get_payload() #get body
```
![alt text](https://github.com/jodiesamuel/tunaiku_test/blob/main/pictures/Screenshot%20from%202020-11-17%2016-08-12.png)

load to bigquery
```
from google.cloud import bigquery

client = bigquery.Client()
pandas_gbq.to_gbq(output_details,project_id="zz",destination_table="xx",  
                  if_exists='overwrite', credentials=client,
                 table_schema=[{'name': 'email_user', 'type': 'STRING'},
                               {'name': 'email', 'type': 'STRING'}
                              ]
                 )
```
## Questions:

- extract a .tz file containing multiple txt files and each txt file has 6 different table data(identified based on table_id) and load these in a destination table

extract tar zip file with multiple csv files and load into localhost postgres table 
```
import  tarfile
tar = tarfile.open("/home/jodie/Documents/tunaiku/test.tar.xz")

li=[]
for member in tar.getmembers():
    f=tar.extractfile(member)
    content = pd.read_csv(f, sep=',')
    li.append(content)
    
frame = pd.concat(li, axis=0, ignore_index=True)
```
![alt text](https://github.com/jodiesamuel/tunaiku_test/blob/main/pictures/Screenshot%20from%202020-11-17%2016-14-53.png)
```
from sqlalchemy import create_engine
connect = "postgresql+psycopg2://admin:admin@localhost:5432/postgres"
def to_alchemy(df):
    engine = create_engine(connect)
    frame.to_sql(
        'tunaiku_test', 
        con=engine, 
        index=False, 
        if_exists='replace'
    )
```
## Questions:

- share a simple python code that use parallel threading for a class/pkg, example: run 2 methods in a python class in parallel

running parallel threads using the files from the question before
```
import threading

def testForThread1():
    print('[Thread1]: ')
    test1 = pd.read_csv('/home/jodie/Documents/tunaiku/test.csv', sep=',')
    print(test1)

def testForThread2():
    print('[Thread2]: ')
    test2 = pd.read_csv('/home/jodie/Documents/tunaiku/test2.csv', sep=',')
    print(test2)

if __name__ == "__main__":
    t1 = threading.Thread(name="Test1", target=testForThread1) 
    t1.start()
    t2 = threading.Thread(name="Test2", target=testForThread2) 
    t2.start()
```
![alt text](https://github.com/jodiesamuel/tunaiku_test/blob/main/pictures/Screenshot%20from%202020-11-17%2016-18-13.png)
