# -*- coding: utf-8 -*-
# @Time    : 2020/11/5 23:49
# @Author  : 莫再提
import re
import json
import pandas as pd
from config import config
from datetime import datetime
from postSql import readP2p, readUNData
from py2neo import Node, Relationship, Graph, Subgraph
from matchDef import dayPositive, ifIn, ifLen, timeIn

graph = Graph(config['graph_conn']['host'], username=config['graph_conn']['username'], password=config['graph_conn']['password'])

# 节点转换json
def nodesFormat(list_s, nodes, duplicate_set):
    for node_one in list_s:
        node_z = {}
        label = str(node_one.labels)[1:]
        name = node_one['name']
        bankId = node_one['bankId']
        node_z['labels'] = label
        node_z['name'] = name
        node_z['bankId'] = bankId
        if bankId not in duplicate_set:
            nodes.append(node_z)
            duplicate_set.add(bankId)
    return nodes, duplicate_set

# 关系转换json
def relFormat(r_list, links, reldup_set):
    for rel_one in r_list:
        rel_z = {}
        label = "资金流向"
        source_id = rel_one.start_node['bankId']
        target_id = rel_one.end_node['bankId']
        money = rel_one['money']
        times = rel_one['times']
        rel_z['labels'] = label
        rel_z['source_id'] = source_id
        rel_z['target_id'] = target_id
        rel_z['money'] = money
        rel_z['times'] = times
        if source_id + target_id not in reldup_set:
            links.append(rel_z)
            reldup_set.add(source_id + target_id)
    return links, reldup_set

# neo4j数据转换成json用于前端展示
def dfFormat(df, type_s):
    links = []
    nodes = []
    duplicate_set = set()
    reldup_set = set()
    if len(df)>0:
        if type_s == 0:
            n_list = df['n'].tolist()
            nodes, duplicate_set = nodesFormat(n_list, nodes, duplicate_set)
            b_list = df['b'].tolist()
            nodes, duplicate_set = nodesFormat(b_list, nodes, duplicate_set)
            r_list = df['r'].tolist()
            links, reldup_set = relFormat(r_list, links, reldup_set)
            neo4j_data = {'links': links, 'nodes': nodes}
        else:
            n_list = df['data'].tolist()
            for node_one in n_list:
                node_node = node_one.nodes
                nodes, duplicate_set = nodesFormat(node_node, nodes, duplicate_set)
                node_rel = node_one.relationships
                links, reldup_set = relFormat(node_rel, links, reldup_set)
            neo4j_data = {'links': links, 'nodes': nodes}
    else:
        neo4j_data = {'links': links, 'nodes': nodes}
    return neo4j_data



# 查询关系
def queryOne(cardNum, type_s, min_money, min_times):
    if type_s == '1':
        cql = "match (n)<-[r:资金流向]-(b) where n.bankId='{}' and r.money>{} and r.times>{} return n,r,b".format(cardNum, min_money, min_times)
    elif type_s == '2':
        cql = "match (n)-[r:资金流向]->(b) where n.bankId='{}' and r.money>{} and r.times>{} return n,r,b".format(cardNum, min_money, min_times)
    data = graph.run(cql)
    df = data.to_data_frame()
    neo4j_data = dfFormat(df, 0)
    return neo4j_data

# 查询关系
def queryTwo(depth, cardNumone, cardNumtwo, min_money, min_times):
    cql = "MATCH data=(n)-[r:资金流向 *1..{}]-(b) where n.bankId='{}' and b.bankId='{}' and all(r in relationships(data) where r.money>{} and r.times>{}) return data".format(depth, cardNumone, cardNumtwo, min_money, min_times)
    data = graph.run(cql)
    df = data.to_data_frame()
    neo4j_data = dfFormat(df, 1)
    return neo4j_data

# 功能一，异常群体发现功能
# 寻找资金下流节点
def findinGroup(type_s, grade, money=200000, in_num=4, min_times=7):
    if type_s == 'findDown':
        cql = "MATCH data=(n:person_{})<-[r:资金流向]-(b:person_{}) where n.in_money-coalesce(n.out_money,0)>{} and all(r in relationships(data) where r.times>{}) return n.bankId,n.name".format(grade, grade-1, money, min_times)
    elif type_s == 'findUp':
        cql = "MATCH data=(b:person_{})<-[r:资金流向]-(n:person_{}) where b.in_money-coalesce(b.out_money,0)>{} and all(r in relationships(data) where r.times>{}) return n.bankId,n.name".format(grade, grade+1, money, min_times)
    elif type_s == 'findClose':
        cql = "match (n:person_{}) where n.in_num>={} return n.name, n.bankId".format(grade, in_num)
    elif type_s == 'findAnd':
        cql = "MATCH data=(n:person_{})<-[r:资金流向]-(b:person_{}) where n.in_money-coalesce(n.out_money,0)>{} and n.in_num>={} and all(r in relationships(data) where r.times>{}) return n.bankId,n.name".format(grade, grade-1, money, in_num, min_times)
    else:
        print("类型错误，重新输入")
        cql = ''
    data = graph.run(cql)
    df = data.to_data_frame()
    dfUN = readUNData(config['db_conn'], config['tableName'])
    dataUN = set(dfUN['查询账号'].tolist())
    df['ifIN'] = df['n.bankId'].apply(lambda x: ifIn(x, dataUN))
    return df

# 资金链关系查询（与过桥账户最短关系网）
def find_relation(grade, bankId, money=200000, times=7):
    cql = "MATCH data=(n:person_{})<-[r:资金流向 *1..{}]-(b:person_1) where n.bankId='{}' and all(r in relationships(data) where r.money>{} and r.times>{}) return data".format(grade, grade-1, bankId, money, times)
    data = graph.run(cql)
    df = data.to_data_frame()
    return df

def find_chain(bankId, money=200000, times=7):
    grade = getGrade(bankId)
    df = find_relation(grade, bankId, money, times)
    neo4j_data = dfFormat(df, 1)
    return neo4j_data


# # 获取冻结人员名单
# def get_person(grade, in_money=200000, money=100000):
#     df = findin_group('寻找下游', grade, in_money)
#     s = set()
#     for index, row in df.iterrows():
#         try:
#             df1 = find_relation(grade, row['n.bankId'], money=money)
#             if len(df1)>0:
#                 s.add(row['n.bankId'])
#         except:
#             print(row['n.bankId'])
#     df['if_in'] = df['n.bankId'].apply(lambda x: if_in(x,s))
#     df = df[df['if_in']==1]
#     df['time'] = df['n.bankId'].apply(lambda x: get_time(x))
#     return df
def getGrade(bankId):
    cql = "match (n) where n.bankId = '{}' return n".format(bankId)
    data = graph.run(cql).data()
    grade = int(str(data[0]['n'].labels)[-1])
    return grade

def getName(bankId):
    cql = "match (n) where n.bankId = '{}' return n.name".format(bankId)
    df = graph.run(cql).to_data_frame()
    name = df['n.name'][0]
    return name

def chastime(i):
    try:
        return str(datetime.strptime(str(i), '%Y%m%d%H%M%S').hour) + ':00'
    except:
        return 'null'

def archiveTime(bankId):
    df = readP2p(config['db_conn'], config['tableName'], bankId)
    df = df[df['借贷标志']=='借']
    df['tm'] = df['交易时间'].apply(ifLen)
    df = df[df['tm'] == 1]
    df['交易时间'] = df['交易时间'].apply(lambda i: chastime(i))
    list_max = df['交易时间'].tolist()
    if len(list_max)>0:
        try:
            histogram = pd.value_counts(list_max)
            histogramK = histogram.keys().tolist()
            histogramV = histogram.values.tolist()
            maxlabel = max(list_max, key=list_max.count)

        except:
            maxlabel='null'
        if maxlabel!='null':
            cql = "match (n) where n.bankId = '{}' return n.name".format(bankId)
            data = graph.run(cql)
            df = data.to_data_frame()
            return maxlabel, df['n.name'][0], histogramK, histogramV
        else:
            return 'null', 'null', [], []
    else:
        return 'null', 'null', [], []


def archiveAll(bankId):
    df = readP2p(config['db_conn'], config['tableName'], bankId)
    df = df[df['借贷标志']=='借']
    df['tm'] = df['交易时间'].apply(ifLen)
    df = df[df['tm'] == 1]
    df['交易时间'] = df['交易时间'].apply(lambda i: chastime(i))
    list_max = df['交易时间'].tolist()
    cql = "match (n) where n.bankId = '{}' return n.name".format(bankId)
    data = graph.run(cql)
    dfname = data.to_data_frame()
    names = dfname['n.name'][0]
    if len(list_max)>0:
        try:
            maxlabel = max(list_max, key=list_max.count)
        except:
            maxlabel='null'
        if maxlabel!='null':
            return maxlabel, names
        else:
            return 'null', names
    else:
        return 'null', names
# 数据预处理
def clearn_df(df):
    df = df[['查询账号', '对方账号姓名', '对方账号卡号', '交易时间', '金额', '借贷标志']]
    df['交易时间'] = df['交易时间'].apply(lambda x: x[:8])
    df['金额'] = df['金额'].apply(lambda x: float(x.replace(',', '')[1:]))
    df = df["金额"].groupby([df['查询账号'], df['对方账号姓名'], df['对方账号卡号'], df['借贷标志'], df['交易时间']]).sum()
    df = df.reset_index()
    return df


# 处理nodes数据
# def get_reltable(grade, df, config=config):
#     dedupbankid_set = set()
#     nodeList = []
#     relaList = []
#     for i in range(grade):
#         locals()['l'+str(i)]=[]
#     for index, row in df.iterrows():
#         if len(row['data']) > 1:
#             for i in range(grade):
#                 dictNode = {}
#                 locals()['l' + str(grade-1)].append(row['data'].nodes[i]['bankId'])
#                 if row['data'].nodes[i]['bankId'] not in dedupbankid_set:
#                     dictNode['labels'] = str(row['data'].nodes[i].labels)[1:]
#                     dictNode['name'] = row['data'].nodes[i]['name']
#                     dictNode['bankId'] = row['data'].nodes[i]['bankId']
#                     nodeList.append(dictNode)
#                     dedupbankid_set.add(dictNode['bankId'])
#     df_result = pd.DataFrame()
#     for i in range(grade-1):
#         locals()['df'+str(i)] = readP2p(config['db_conn'],config['tableName'],locals()['l'+str(i)],locals()['l'+str(i+1)])
#         if len(locals()['df'+str(i)]) == 0:
#             return pd.DataFrame()
#         else:
#             df_result = pd.concat([df_result, locals()['df'+str(i)]])
#     df_result = df_result.drop_duplicates(['唯一id'])
#     for index, row in df_result.iterrows():
#         dictRela = {}
#         dictRela['labels'] = "资金流向"
#         dictRela['source_id'] = row['出钱方']
#         dictRela['target_id'] = row['收钱方']
#         dictRela['money'] = row['金额']
#         dictRela['timez'] = row['交易时间']
#         relaList.append(dictRela)
#     neo4j_data = {'links': relaList, 'nodes': nodeList}
#     return neo4j_data

####################################################################################
# # 处理nodes数据
# def get_reltable(grade, df, config=config):
#     dedupbankid_set = set()
#     nodeList = []
#     relaList = []
#     for i in range(grade):
#         locals()['l'+str(i)]=[]
#     for index, row in df.iterrows():
#         if len(row['data']) > 0:
#             for i in range(grade):
#                 dictNode = {}
#                 locals()['l' + str(i)].append(row['data'].nodes[i]['bankId'])
#                 if row['data'].nodes[i]['bankId'] not in dedupbankid_set:
#                     dictNode['labels'] = str(row['data'].nodes[i].labels)[1:]
#                     dictNode['name'] = row['data'].nodes[i]['name']
#                     dictNode['bankId'] = row['data'].nodes[i]['bankId']
#                     nodeList.append(dictNode)
#                     dedupbankid_set.add(dictNode['bankId'])
#     df_result = pd.DataFrame()
#     timeMax = '30181111020202'
#     timeMin = '10181111020202'
#     for i in range(grade-1):
#         locals()['df'+str(i)] = readP2p(config['db_conn'],config['tableName'],locals()['l'+str(i+1)], locals()['l'+str(i)])
#         if len(locals()['df'+str(i)]) == 0:
#             return pd.DataFrame()
#         locals()['df'+str(i)]['timesIf'] = locals()['df'+str(i)]['交易时间'].apply(lambda x: timeIn(x, timeMax, timeMin))
#         locals()['df'+str(i)] = locals()['df'+str(i)][locals()['df'+str(i)]['timesIf']==1]
#         timeMin = min(locals()['df'+str(i)]['交易时间'].tolist())
#         timeMax = max(locals()['df'+str(i)]['交易时间'].tolist())
#         timeMin = dayPositive(timeMin)
#         if len(locals()['df'+str(i)]) == 0:
#             return pd.DataFrame()
#         else:
#             df_result = pd.concat([df_result, locals()['df'+str(i)]])
#     df_result = df_result.drop_duplicates(['唯一id'])
#     for index, row in df_result.iterrows():
#         dictRela = {}
#         dictRela['labels'] = "资金流向"
#         dictRela['source_id'] = row['出钱方']
#         dictRela['target_id'] = row['收钱方']
#         dictRela['money'] = row['金额']
#         dictRela['timez'] = row['交易时间']
#         relaList.append(dictRela)
#     neo4j_data = {'links': relaList, 'nodes': nodeList}
#     return neo4j_data

# 处理nodes数据
def get_reltable(grade, df, config=config):
    dedupbankid_set = set()
    nodeList = []
    relaList = []
    for i in range(grade):
        locals()['l'+str(i)]=[]
    for index, row in df.iterrows():
        if len(row['data']) > 0:
            for i in range(grade):
                dictNode = {}
                try:
                    locals()['l' + str(i)].append(row['data'].nodes[i]['bankId'])
                    if row['data'].nodes[i]['bankId'] not in dedupbankid_set:
                        dictNode['labels'] = str(row['data'].nodes[i].labels)[1:]
                        dictNode['name'] = row['data'].nodes[i]['name']
                        dictNode['bankId'] = row['data'].nodes[i]['bankId']
                        nodeList.append(dictNode)
                        dedupbankid_set.add(dictNode['bankId'])
                except:
                    pass

    df_result = pd.DataFrame()
    for i in range(grade-1):
        locals()['df'+str(i)] = readP2p(config['db_conn'],config['tableName'],locals()['l'+str(i+1)], locals()['l'+str(i)])
        if len(locals()['df'+str(i)]) == 0:
            return pd.DataFrame()
        else:
            df_result = pd.concat([df_result, locals()['df'+str(i)]])
    df_result = df_result.drop_duplicates(['唯一id'])
    for index, row in df_result.iterrows():
        dictRela = {}
        dictRela['labels'] = "资金流向"
        dictRela['source_id'] = row['出钱方']
        dictRela['target_id'] = row['收钱方']
        dictRela['money'] = row['金额']
        dictRela['timez'] = row['交易时间']
        relaList.append(dictRela)
    neo4j_data = {'links': relaList, 'nodes': nodeList}
    return neo4j_data


if __name__ == "__main__":
    id = '6217001820049502994'
    df = find_relation(2, id, 10000, 0)
    z = get_reltable(2,df)
    print(z)
    # findinGroup('findDown',2, 1000, min_times=1)
    ##########################################################冻结
    # df = pd.read_excel(r'C:\Users\sky\Desktop\冻结.xlsx')
    # df['预测交易时间'], df['姓名'] = zip(*df.apply(lambda x:archiveAll(x['银行卡']),axis=1))
    # print(df)
    # df.to_excel(r'C:\Users\sky\Desktop\冻结2.xlsx')
    ##########################################################凭证

