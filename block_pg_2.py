# -*- coding: utf-8 -*-
"""
Created on Fri Mar 30 15:29:38 2018

@author: Andrey
"""

import urllib
import psycopg2
import time
import json
#collections

class Api():
    main_url = 'https://api.vk.com/method/'

    def method(self, method, params=None):
        if not params:
            params = {}
        full_url = self.generate_url(method, params)
        res_api = urllib.request.urlopen(full_url).read()
        time.sleep(2)
        return res_api

    def generate_url(self, method, params):
        m = self.main_url + method + "?" +urllib.parse.urlencode(params)
        return m

def next_n(i,n):
    i+=1
    if i>=n:
        i=0
    return i

api = Api()

access_token=[]
access_token.append('38307e5c81217431ce5ad708c39734573da0e481a05bff848f183324c4646d3cec0d256f676afe9b84ae6')
access_token.append('6ea788255843d2fe40aa74c5e8f165cd9661ad926ccf1e61d95bc878da6511f2d37a7575683fae2f00f06')
access_token.append('f47494d6ab026e9254f34e5a70637b0fa9a3a6672142ae02bc4b589c54f48d91fdb1915c43f612485503d')
c=-1

method=''
param={}
#aram.update({"access_token" : access_token[0]})

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
    sql='DELETE FROM "Andrey"."A_VK_Accounts";'
    cur.execute(sql)
    sql='select client_id, lastname, firstname, "Birthday" from "Andrey"."A_Clients";'
    cur.execute(sql)
    rows = cur.fetchall()
    n=0
    for row in rows:
        print(n, row)
        method='users.search'
        param={}
        param.update({"fields": "bdate,screen_name,sex,city,nickname,contacts,last_seen"})
        param.update({"count" : 100})
        param.update({"v" : "5.73"})
        cl_name=row[2]+" "+row[1]
        param.update({"q" : cl_name})
        param.update({"birth_year" : row[3].year})
        param.update({"birth_month" : row[3].month})
        param.update({"birth_day" : row[3].day})
        c=next_n(c,len(access_token))
        param.update({"access_token" : access_token[c]})
        res = api.method(method, param)
        res1 = json.loads(res)
        print(' cnt=', res1['response']['count'])
        sql='UPDATE "Andrey"."A_Clients" SET other=%s WHERE client_id=%s;'
        cur.execute(sql,(json.JSONEncoder().encode(res1),row[0]))
        for uid in res1['response']['items']:
            print('  id=', uid['id'])
            method='users.get'
            param={
                    "fields": "online,last_seen,city,bdate,sex,contacts,connections,has_photo,games,verified,blacklisted,wall_default,screen_name,nickname,followers_count",
                    "v" : "5.73"
                    }
            param.update({'user_ids':uid['id']})
            c=next_n(c,len(access_token))
            param.update({"access_token" : access_token[c]})
            res_u = api.method(method, param)
            res1_u = json.loads(res_u)
            res1_u['response'][0]['last_seen'].update({'time_str': time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(res1_u['response'][0]['last_seen']['time']))})

            print('   friends')
            method='friends.get'
            param={
                    'order': 'hints',
                    'fields': 'last_seen',
                    'v' : '5.73'
                    }
            param.update({'user_id':uid['id']})
            c=next_n(c,len(access_token))
            param.update({"access_token" : access_token[c]})
            res_f = api.method(method, param)
            res1_f = json.loads(res_f)

            print('   groups')
            method='groups.get'
            param={
                    'extended': 1,
                    'v' : '5.73'
                    }
            param.update({'user_id':uid['id']})
            c=next_n(c,len(access_token))
            param.update({"access_token" : access_token[c]})
            res_g = api.method(method, param)
            res1_g = json.loads(res_g)

            print('   wall')
            method='wall.get'
            param={
                    'filter':'owner',
                    'extended': 0,
                    'v' : '5.73'
                    }
            param.update({'owner_id':uid['id']})
            c=next_n(c,len(access_token))
            param.update({"access_token" : access_token[c]})
            res_w = api.method(method, param)
            res1_w = json.loads(res_w)

            sql='INSERT INTO "Andrey"."A_VK_Accounts"('
            sql+='account_id, client_id, trust_lvl, vk_user, vk_friends, vk_groups, vk_wall)' #, creation_date, last_update_date
            sql+='VALUES (%s, %s, %s, %s, %s, %s, %s);' #, ?, ?
            cur.execute(sql,
                        (uid['id'],
                         row[0],
                         1.0,
                         json.JSONEncoder().encode(res1_u),
                         json.JSONEncoder().encode(res1_f),
                         json.JSONEncoder().encode(res1_g),
                         json.JSONEncoder().encode(res1_w)
                         )
                        )
        conn.commit()
        n+=1
    cur.close()
except psycopg2.DatabaseError as error:
    print('Error: ',error)
finally:
    if conn is not None:
        conn.close()
