# -*- coding: utf-8 -*-
"""
Created on Thu Nov 19 11:52:16 2020

@author: Sadanand
"""

import requests
import json
import pandas as pd
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import pymysql
from sqlalchemy import create_engine, Column, Integer, String
import MySQLdb


class get_api_data:
    
    def __init__(self,date,zip_code,line_up_id,date_time):
        self.date = date
        self.zip_code = zip_code
        self.date_time = date_time
        self.line_up_id = line_up_id
    

    def get_movies_playing_in_local_theaters(self):
        
        url = 'http://data.tmsapi.com/v1.1/movies/showings?startDate={date}&zip={zip_code}&api_key={api_key}'.\
            format(date=self.date,zip_code=self.zip_code,api_key=os.environ.get('api_key'))
            
        response = requests.get(url)
        data = response.json()
        df = pd.DataFrame.from_dict(data)
        
        return df
        
        
    def get_movies_airing_on_TV(self):
        
        url = 'http://data.tmsapi.com/v1.1/programs/newShowAirings?lineupId={line_up_id}&startDateTime={date_time}&api_key={api_key}'.\
            format(line_up_id=self.line_up_id,date_time=self.date_time,api_key=os.environ.get('api_key'))
            
        response = requests.get(url)
        data = response.json()
        df = pd.DataFrame.from_dict(data)
        
        return df



Base = declarative_base()

class create_table_theater_movies(Base):
        
    __tablename__ = 'theater_movies'
    
    id = Column(Integer,primary_key=True)
    title = Column(String(100))
    release_year = Column(Integer)
    genres = Column(String(50))
    description = Column(String(300))
    channel_name_theater_name = Column(String(50))
    
class create_table_TV_movies(Base):
        
    __tablename__ = 'tv_movies'
    
    id = Column(Integer,primary_key=True)
    title = Column(String(100))
    release_year = Column(Integer)
    genres = Column(String(50))
    description = Column(String(300))
    channel_name_theater_name = Column(String(50))
    

date = '2020-11-18'
zip_code = '78701'
line_up_id = 'USA-TX42500-X'
date_time = '2020-11-18T16:00Z'

obj = get_api_data(date,zip_code,line_up_id,date_time)
    
local_theaters_df = obj.get_movies_playing_in_local_theaters()

#Need Columns in Database.
df_table1 = local_theaters_df[['title','releaseYear','genres','longDescription','showtimes']]
df_table1['showtimes'] = df_table1['showtimes'].map(lambda x:x[0]['theatre']['name'])
df_table1.rename(columns={'showtimes':'channel_name_theater_name','longDescription':'description',
                          'releaseYear':'release_year'},inplace=True)
df_table1['genres'] = df_table1['genres'].map(lambda x:','.join(x) if str(x) != 'nan' else x)
df_table1 = df_table1[['title','release_year','genres','description','channel_name_theater_name']]

line_up_id_df = obj.get_movies_airing_on_TV()
df_table2 = line_up_id_df[['channels','program']]
df_table2['title'] = df_table2.apply(lambda x:x['program']['title'],axis=1)
df_table2['release_year'] = df_table2.apply(lambda x:x['program']['releaseYear'] if \
                                            'releaseYear' in x['program'].keys() != True else '',axis=1)
df_table2['genres'] = df_table2.apply(lambda x:x['program']['genres'] if \
                                     'genres' in x['program'].keys() != True else '' ,axis=1)

df_table2['genres'] = df_table2['genres'].map(lambda x:','.join(x) if str(x) != 'nan' else x)

df_table2['description'] = df_table2.apply(lambda x:x['program']['longDescription'] if \
                                     'longDescription' in x['program'].keys() != True else '' ,axis=1)

df_table2['channel_name_theater_name'] = df_table2['channels'].map(lambda x:','.join(x) if str(x) != 'nan' else x)
    
df_table2 = df_table2[['title','release_year','genres','description','channel_name_theater_name']]


cnx = create_engine('mysql+pymysql://root:Root@localhost:3306/api_data')
Session = sessionmaker(bind=cnx)
session = Session()
Base.metadata.create_all(cnx)

df_table1.to_sql('theater_movies',con=cnx,index=False,if_exists='replace',method='multi')
df_table2.to_sql('tv_movies',con=cnx,index=False,if_exists='replace',method='multi')




mysql_cn= MySQLdb.connect(host='localhost', 
                port=3306,user='root', passwd='Root', 
                db='api_data')
theater_movies = pd.read_sql('select * from theater_movies;', con=mysql_cn) 
theater_movies['genres'] = theater_movies['genres'].map(lambda x:x.split(',') if x is not None else '')
theater_movies = theater_movies.explode('genres')

tv_movies = pd.read_sql('select * from tv_movies;', con=mysql_cn) 
tv_movies['genres'] = tv_movies['genres'].map(lambda x:x.split(',') if x is not None else '')   
tv_movies = tv_movies.explode('genres')


merged_df = pd.concat([theater_movies,tv_movies])

res_data = merged_df.groupby('genres')['title'].count()

res_data = pd.DataFrame(res_data)
res_data = res_data.reset_index()
res_data.rename(columns={'title':'title_count'},inplace=True)
res_data = res_data.sort_values('title_count',ascending = False).head(5)
mysql_cn.close()

#=========================================================
#Top 5 genres are:
print (res_data)
    

