# -*- coding: utf-8 -*-
"""
Created on Fri Mar 30 13:30:46 2018

@author: Andrey
"""

import csv
import psycopg2

filename_in = 'test2.csv'
data=[]
fieldnames1=[]
item={}

with open(filename_in,'r') as csv_in:
    reader = csv.DictReader(csv_in)
    fieldnames1=reader.fieldnames
    for row in reader:
        item=dict(row)
        data.append(item)

connstr=''
connstr+=' host=79.165.88.213'
connstr+=' port=5432'
connstr+=' dbname=frod'
connstr+=' user=postgres'
connstr+=' password=Radius123'

conn = None
try:
    conn=psycopg2.connect(connstr)
    cur = conn.cursor()
    sql='DELETE FROM "Andrey"."A_Clients";'
    cur.execute(sql)
    conn.commit()
    sql='INSERT INTO "Andrey"."A_Clients"('
    sql+=' client_id, lastname, firstname, secondname, sex, "Birthday", "Addres_reg", email)' #, creation_date, last_update_date
    sql+='VALUES (%s, %s, %s, %s, %s, %s, %s, %s);' #, ?, ?
    for item in data:
        cur.execute(sql,
                    (item['client_id'],
                     item['lastname'],
                     item['firstname'],
                     item['secondname'],
                     item['sex'],
                     item['Birthday'],
                     item['Addres_reg'],
                     item['email'])
        )
    conn.commit()
    cur.close()
except psycopg2.DatabaseError as error:
    print('Error: ',error)
finally:
    if conn is not None:
        conn.close()
