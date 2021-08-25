# -*- coding: utf-8 -*-
# @Time    : 2020/9/14 15:57
# @Author  : 莫再提

import os
import requests
import datetime
import psycopg2
from collections import Counter
from postSql import Sqlprocess
from config import config

# 获取所有文件
def get_filelist(dir, Filelist):
    newDir = dir
    if os.path.isfile(dir):
        Filelist.append(dir)

    elif os.path.isdir(dir):
        for s in os.listdir(dir):
            if s == "第1批":
                continue
            newDir=os.path.join(dir,s)
            get_filelist(newDir, Filelist)
    return Filelist

# 借贷全部更换为贷
def change_df(row):
    if row['借贷标志']=='借':
        return row['对方账号卡号'], row['查询账号']
    else:
        return row['查询账号'], row['对方账号卡号']

# id录入前预判
def if_only(id, only_id):
    if id in only_id:
        return 1
    else:
        only_id.add(id)
        return 0

# 借贷转换方法
def md3_dk(id_a, id_b, money, times):
    if id_a < id_b:
        return id_a+'#'+id_b, money, times
    else:
        return id_b+'#'+id_a, -money, times

# 切割借贷转换方法
def split_df(row):
    if row['all_money']>0:
        a,b = row['md3'].split('#')
        return a,b, row['all_money'], row['max_times']
    else:
        b,a = row['md3'].split('#')
        return a,b, abs(row['all_money']), row['max_times']


#  空值用零替换
def ret_or(x):
    if x!=None:
        return x
    else:
        return 0

def add_or(graph_db, start_node, end_node, relationship='资金流向'):
    if len(list(graph_db.match((start_node, end_node), r_type=relationship))) > 0:
        pass
    else:
        start_node['out_num'] = ret_or(start_node['out_num']) + 1
        end_node['in_num'] = ret_or(end_node['in_num']) + 1

def if_Mysql(idlist):
    conn = psycopg2.connect(database=config['database'], user=config['user'],
                            password=config['password'], host=config['host'],
                            port=config['port'])
    sqlpro = Sqlprocess(config['db_conn'])
    sqlpro.initialization()
    cur = self.conn.cursor()
    cur.execute("SELECT 唯一id FROM zmd_transaction_details LIMIT 50")
    rows = cur.fetchall()
    aa = '6228480479097095570$215500690$20210502041738$24021.0$16941.8,'
    list = []
    for i in rows:
        print(i)
        list.append(i[0])
    print(list)


# 关联时间天数限制
def timeIn(x, xmax, xmin):
    if x<=xmax and x>=xmin:
        reslut = 1
    else:
        reslut = 0
    return reslut


def dayPositive(x):
    try:
        x = datetime.datetime.strptime(x, '%Y%m%d')
    except:
        pass
    try:
        x = datetime.datetime.strptime(x, '%Y%m%d%H%M%S')
    except:
        pass
    delta = datetime.timedelta(days=30)
    x = x - delta
    x = x.strftime('%Y%m%d%H%M%S')
    return x

# 存在判别
def ifIn(x,s):
    if x in s:
        return 1
    else:
        return 0


def ifLen(s):
    if len(s)>10:
        return 1
    else:
        return 0


def iposit(ip):
    url = 'http://whois.pconline.com.cn/ipJson.jsp?json=true&ip=' + ip
    data = requests.get(url).json()
    return data['addr']


def prossip(iplist):
    ipdict = Counter(iplist)
    result = {}
    for i in ipdict.keys():
        county = iposit(i)
        if county in result.keys():
            result[county] = result[county] + ipdict[i]
        else:
            result[county] = ipdict[i]
    return result

