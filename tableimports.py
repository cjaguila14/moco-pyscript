#importing required modules
import numpy as np
import os
import pandas as pd
import psycopg2
import psycopg2.extras as extras

#Database URL used to connect to Heroku
DATABASE_URL = 'postgres://caeltaetxhemre:fce0691b7d6c3ac33a16fd72ec7a4009ad74b3410385a6b75c488dc3ce39c157@ec2-34-194-158-176.compute-1.amazonaws.com:5432/de09islg90c56t'

#reading in csv files as panda dataframes and adjusting columns
rpsTable = pd.read_csv('tf_recs.csv') #Change this line with correct file path for CSV 1
rpsTable.drop(rpsTable.columns[[0]], axis=1, inplace=True)
rpsTable['SSJC Comments'] = np.NaN
rpsTable.columns = ['action_id', 'focus_area', 'tf_rec', 'action', 'parties', 'progress', 'timeline', 'priority', 'ssjc_comments']
mpaaTable = pd.read_csv('mpaa.csv') # change this line with correct file path for CSV 2
mpaaTable['SSJC Comments'] = np.NaN
mpaaTable.columns = ['mpaa_id', 'focus_area', 'rps_rec', 'action', 'parties_responsible', 'progress', 'timeline', 'priority_level', 'ssjc_comments']
auditTable = pd.read_csv('audit.csv') #change this line with correct file path for CSV 3
auditTable['SSJC Comments'] = np.NaN
auditTable.columns = ['audit_id', 'action_id', 'focus_area', 'recommendations', 'action', 'parties_responsible', 'progress', 'timeline', 'priority_level', 'ssjc_comments']

#Function to populate tables in databases
def execute_values(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

#Establishing connection to database
conn = psycopg2.connect(DATABASE_URL, sslmode='require')
#creating tables if they do not already exist
cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS tf_recs (action_id serial PRIMARY KEY, focus_area text, tf_rec text, action text, parties text, progress text, timeline text, priority text, ssjc_comments text);")
cur.execute("CREATE TABLE IF NOT EXISTS mpaa (mpaa_id serial PRIMARY KEY, focus_area text, rps_rec text, action text, parties_responsible text, progress text, timeline text, priority_level text, ssjc_comments text);")
cur.execute("CREATE TABLE IF NOT EXISTS audit (audit_id serial PRIMARY KEY, action_id int, focus_area text, recommendations text, action text, parties_responsible text, progress text, timeline text, priority_level text, ssjc_comments text);")
cur.close()
#using the function to populate tables with corresponding data
execute_values(conn, rpsTable, 'tf_recs')
execute_values(conn, mpaaTable, 'mpaa')
execute_values(conn, auditTable, 'audit')
conn.commit()
conn.close()







