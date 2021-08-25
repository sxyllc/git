# -*- coding: utf-8 -*-
# @Time    : 2020/11/5 12:48
# @Author  : 莫再提
import re
import pandas as pd
from tqdm import tqdm
from config import config
from datetime import datetime
from postSql import Sqlprocess
from py2neo import Node, Relationship, Graph, Subgraph
from matchDef import ret_or, split_df, md3_dk, add_or
from py2neo.matching import *
from matchDef import dayPositive, ifIn, ifLen, timeIn
import psycopg2


class GraphProcess(Sqlprocess):
    def __init__(self, post_config, graph_config):
        Sqlprocess.__init__(self, post_config)
        self.graphconfig = graph_config
        Sqlprocess.initialization(self)
        self.graph = Graph(self.graphconfig['host'], username=self.graphconfig['username'],
                      password=self.graphconfig['password'])

    # 节点唯一性设置
    def graphDeduplicate(self, df):
        for person in set(df['人物等级'].tolist()):
            cql = "create constraint on (n:{}) assert n.bankId is unique".format(person)
            try:
                self.graph.run(cql)
            except:
                print('本次执行已进行去重化操作')
        return True

    # 数据关系合并
    def relCombing(self, df):
        df = df.drop_duplicates(['收钱方', '出钱方', '金额', '交易时间'])
        df = df["金额"].groupby([df['收钱方'], df['出钱方']]).agg(['sum', 'count'])
        df = df.reset_index()
        df.columns = ['收钱方', '出钱方', '金额', '交易次数']
        dfR = pd.DataFrame()
        dfR['md3'], dfR['money'], dfR['times'] = zip(
            *df.apply(lambda row: md3_dk(row['收钱方'], row['出钱方'], row['金额'], row['交易次数']), axis=1))
        gp = dfR.groupby('md3')['money', 'times'].agg(
            {'money': {'all_money': sum}, 'times': {'max_times': max}}).reset_index()
        gpR = pd.DataFrame()
        gpR['a'], gpR['b'], gpR['money'], gpR['times'] = zip(*gp.apply(lambda row: split_df(row), axis=1))
        return gpR

    # 创建图数据库
    def mainGraph(self, data):
        node_dict = {}
        nodes_all = set()
        matcher = NodeMatcher(self.graph)

        if data == 1:
            idolDf = Sqlprocess.readData(self, 0)
            for index, row in idolDf.iterrows():
                n = matcher.match(bankId=row['银行卡']).first()
                if n is None:
                    a = Node("person_1", bankId=row['银行卡'], name=row['姓名'])
                    nodes_all.add(row['银行卡'])
                    node_dict[row['银行卡']] = a

            node_data = list(node_dict.values())
            node_lists = [node_data[i:i + 1000] for i in range(0, len(node_data), 1000)]
            for nodez in node_lists:
                nodes = Subgraph(nodez)
                self.graph.create(nodes)

        else:
            tableDf = Sqlprocess.readData(self, 2)
            tableDf['金额'] = tableDf['金额'].apply(lambda x: float(x.replace(',', '')[1:]))
            table_nodes_list = tableDf['对方账号卡号'].drop_duplicates().values.tolist()
            for i in table_nodes_list:
                n = matcher.match(bankId=i).first()
                if n is None:
                    index = tableDf[tableDf['对方账号卡号'] == i].index.tolist()[0]
                    a = Node(tableDf['人物等级'].loc[index], bankId=i, name=tableDf['对方账号姓名'].loc[index])
                    nodes_all.add(i)
                    node_dict[i] = a
        # for index, row in tableDf.iterrows():
        #     if row['对方账号卡号'] not in node_basen:
        #         a = Node(row['人物等级'], bankId=row['对方账号卡号'], name=row['对方账号姓名'])
        #         nodes_all.add(row['对方账号卡号'])
        #         node_dict[row['对方账号卡号']] = a
            try:
                self.graphDeduplicate(tableDf)
            except:
                pass
            # 先创建节点=============================================
            # 创建节点
            node_data = list(node_dict.values())
            node_lists = [node_data[i:i + 1000] for i in range(0, len(node_data), 1000)]
            for nodez in node_lists:
                nodes = Subgraph(nodez)
                self.graph.create(nodes)

            # 关系整理
            gpR = self.relCombing(tableDf)

            nodes_update_dict = {}
            re_update = []
            re_create = []
            bankId_list = list(set(gpR['a'].tolist() + gpR['b'].tolist()))
            for i in bankId_list:
                # matcher = NodeMatcher(self.graph)
                n = matcher.match(bankId=i).first()
                nodes_update_dict[i] = n
            for index, row in tqdm(gpR.iterrows()):
                try:
                    node1 = nodes_update_dict[row['a']]
                    node2 = nodes_update_dict[row['b']]
                except:
                    pass
    # 节点更新+++++++++++++++++++++++++++++
                node1['in_money'] = ret_or(node1['in_money']) + row['money']
                node2['out_money'] = ret_or(node2['out_money']) + row['money']
                add_or(self.graph, node2, node1, relationship='资金流向')

                # matcher = NodeMatcher(self.graph)
                # n1 = matcher.match(bankId=row['a']).first()
                # n2 = matcher.match(bankId=row['b']).first()
                rel_match = list(self.graph.match((node2, node1), r_type='资金流向'))
                if len(rel_match) > 0:
                    the_rel_match = rel_match[0]
                    the_rel_match['money'] = the_rel_match['money'] + row['money']
                    the_rel_match['times'] = the_rel_match['times'] + row['times']
                    re_update.append(the_rel_match)
                else:
                    sx = {"money": row['money'], 'times': row['times']}
                    rel_c = Relationship(node2, "资金流向", node1, **sx)
                    re_create.append(rel_c)

            nodes_update_lists = list(nodes_update_dict.values())
            nodes_update_list = [nodes_update_lists[i:i + 1000] for i in range(0, len(nodes_update_lists), 1000)]
            for x in nodes_update_list:
                sub = Subgraph(nodes=x)
                self.graph.push(sub)

            if re_create:
                res_create_list = [re_create[i:i + 500] for i in range(0, len(re_create), 500)]
                for y in res_create_list:
                    res_create = Subgraph(relationships=y)
                    self.graph.create(res_create)
            if re_update:
                res_update_list = [re_update[i:i + 500] for i in range(0, len(re_update), 500)]
                for z in res_update_list:
                    res_update = Subgraph(relationships=z)
                    self.graph.push(res_update)
        Sqlprocess.truncate(self)

        return True

    # 节点转换json
    def nodesFormat(self, list_s, nodes, duplicate_set):
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
    def relFormat(self, r_list, links, reldup_set):
        for rel_one in r_list:
            rel_z = {}
            label = "资金流向"
            rel_z['labels'] = label
            rel_z['source_id'] = rel_one.start_node['bankId']
            rel_z['target_id'] = rel_one.end_node['bankId']
            rel_z['money'] = rel_one['money']
            rel_z['times'] = rel_one['times']
            if rel_z['source_id'] + rel_z['target_id'] not in reldup_set:
                links.append(rel_z)
                reldup_set.add(rel_z['source_id'] + rel_z['target_id'])
        return links, reldup_set

    # neo4j数据转换成json用于前端展示
    def dfFormat(self, df, type_s):
        links = []
        nodes = []
        duplicate_set = set()
        reldup_set = set()
        if len(df) > 0:
            if type_s == 0:
                n_list = df['n'].tolist()
                nodes, duplicate_set = self.nodesFormat(n_list, nodes, duplicate_set)
                b_list = df['b'].tolist()
                nodes, duplicate_set = self.nodesFormat(b_list, nodes, duplicate_set)
                r_list = df['r'].tolist()
                links, reldup_set = self.relFormat(r_list, links, reldup_set)
                neo4j_data = {'links': links, 'nodes': nodes}
            else:
                n_list = df['data'].tolist()
                for node_one in n_list:
                    node_node = node_one.nodes
                    nodes, duplicate_set = self.nodesFormat(node_node, nodes, duplicate_set)
                    node_rel = node_one.relationships
                    links, reldup_set = self.relFormat(node_rel, links, reldup_set)
                neo4j_data = {'links': links, 'nodes': nodes}
        else:
            neo4j_data = {'links': links, 'nodes': nodes}
        return neo4j_data

    # 查询关系
    def queryOne(self, cardNum, type_s, min_money, min_times):
        if type_s == '1':
            cql = "match (n)<-[r:资金流向]-(b) where n.bankId='{}' and r.money>{} and r.times>{} return n,r,b".format(
                cardNum, min_money, min_times)
        elif type_s == '2':
            cql = "match (n)-[r:资金流向]->(b) where n.bankId='{}' and r.money>{} and r.times>{} return n,r,b".format(
                cardNum, min_money, min_times)
        data = self.graph.run(cql)
        df = data.to_data_frame()
        neo4j_data = self.dfFormat(df, 0)
        return neo4j_data

    # 查询关系
    def queryTwo(self, depth, cardNumone, cardNumtwo, min_money, min_times):
        cql = "MATCH data=(n)-[r:资金流向 *1..{}]-(b) where n.bankId='{}' and b.bankId='{}' and all(r in relationships(data) where r.money>{} and r.times>{}) return data".format(
            depth, cardNumone, cardNumtwo, min_money, min_times)
        data = self.graph.run(cql)
        df = data.to_data_frame()
        neo4j_data = self.dfFormat(df, 1)
        return neo4j_data

    # 功能一，异常群体发现功能
    # 寻找资金下流节点
    def findinGroup(self, type_s, grade, money=200000, in_num=4, min_times=7):
        if type_s == 'findDown':
            cql = "MATCH data=(n:person_{})<-[r:资金流向]-(b:person_{}) where n.in_money-coalesce(n.out_money,0)>{} and all(r in relationships(data) where r.times>{}) return n.bankId,n.name".format(
                grade, grade - 1, money, min_times)
        elif type_s == 'findUp':
            cql = "MATCH data=(b:person_{})<-[r:资金流向]-(n:person_{}) where b.in_money-coalesce(b.out_money,0)>{} and all(r in relationships(data) where r.times>{}) return n.bankId,n.name".format(
                grade, grade + 1, money, min_times)
        elif type_s == 'findClose':
            cql = "match (n:person_{}) where n.in_num>={} return n.name, n.bankId".format(grade, in_num)
        elif type_s == 'findAnd':
            cql = "MATCH data=(n:person_{})<-[r:资金流向]-(b:person_{}) where n.in_money-coalesce(n.out_money,0)>{} and n.in_num>={} and all(r in relationships(data) where r.times>{}) return n.bankId,n.name".format(
                grade, grade - 1, money, in_num, min_times)
        else:
            print("类型错误，重新输入")
            cql = ''
        data = self.graph.run(cql)
        df = data.to_data_frame()
        dfUN = self.readUNData()
        dataUN = set(dfUN['查询账号'].tolist())
        df['ifIN'] = df['n.bankId'].apply(lambda x: ifIn(x, dataUN))
        return df

    # 资金链关系查询（与过桥账户最短关系网）
    def find_relation(self, grade, bankId, money=200000, times=7):
        cql = "MATCH data=(n:person_{})<-[r:资金流向 *1..{}]-(b:person_1) where n.bankId='{}' and all(r in relationships(data) where r.money>{} and r.times>{}) return data".format(
            grade, grade - 1, bankId, money, times)
        data = self.graph.run(cql)
        df = data.to_data_frame()
        return df

    def find_chain(self, bankId, money=200000, times=7):
        grade = self.getGrade(bankId)
        df = self.find_relation(grade, bankId, money, times)
        neo4j_data = self.dfFormat(df, 1)
        return neo4j_data

    def getGrade(self, bankId):
        cql = "match (n) where n.bankId = '{}' return n".format(bankId)
        data = self.graph.run(cql).data()
        grade = int(str(data[0]['n'].labels)[-1])
        return grade

    def getName(self,bankId):
        cql = "match (n) where n.bankId = '{}' return n.name".format(bankId)
        df = self.graph.run(cql).to_data_frame()
        name = df['n.name'][0]
        return name

    def chastime(self, i):
        try:
            return str(datetime.strptime(str(i), '%Y%m%d%H%M%S').hour) + ':00'
        except:
            return 'null'

    def archiveTime(self, bankId):
        df = self.readP2p(bankId)
        df = df[df['借贷标志'] == '借']
        df['tm'] = df['交易时间'].apply(ifLen)
        df = df[df['tm'] == 1]
        df['交易时间'] = df['交易时间'].apply(lambda i: self.chastime(i))
        list_max = df['交易时间'].tolist()
        if len(list_max) > 0:
            try:
                histogram = pd.value_counts(list_max)
                histogramK = histogram.keys().tolist()
                histogramV = histogram.values.tolist()
                maxlabel = max(list_max, key=list_max.count)

            except:
                maxlabel = 'null'
            if maxlabel != 'null':
                cql = "match (n) where n.bankId = '{}' return n.name".format(bankId)
                data = self.graph.run(cql)
                df = data.to_data_frame()
                return maxlabel, df['n.name'][0], histogramK, histogramV
            else:
                return 'null', 'null', [], []
        else:
            return 'null', 'null', [], []

    def archiveAll(self, bankId):
        df = self.readP2p(bankId)
        df = df[df['借贷标志'] == '借']
        df['tm'] = df['交易时间'].apply(ifLen)
        df = df[df['tm'] == 1]
        df['交易时间'] = df['交易时间'].apply(lambda i: self.chastime(i))
        list_max = df['交易时间'].tolist()
        cql = "match (n) where n.bankId = '{}' return n.name".format(bankId)
        data = self.graph.run(cql)
        dfname = data.to_data_frame()
        names = dfname['n.name'][0]
        if len(list_max) > 0:
            try:
                maxlabel = max(list_max, key=list_max.count)
            except:
                maxlabel = 'null'
            if maxlabel != 'null':
                return maxlabel, names
            else:
                return 'null', names
        else:
            return 'null', names

    # 数据预处理
    def clearn_df(self, df):
        df = df[['查询账号', '对方账号姓名', '对方账号卡号', '交易时间', '金额', '借贷标志']]
        df['交易时间'] = df['交易时间'].apply(lambda x: x[:8])
        df['金额'] = df['金额'].apply(lambda x: float(x.replace(',', '')[1:]))
        df = df["金额"].groupby([df['查询账号'], df['对方账号姓名'], df['对方账号卡号'], df['借贷标志'], df['交易时间']]).sum()
        df = df.reset_index()
        return df

    # 处理nodes数据
    def get_reltable(self, grade, df):
        dedupbankid_set = set()
        nodeList = []
        relaList = []
        for i in range(grade):
            locals()['l' + str(i)] = []
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
        for i in range(grade - 1):
            locals()['df' + str(i)] = self.readP2p(locals()['l' + str(i + 1)], locals()['l' + str(i)])
            if len(locals()['df' + str(i)]) == 0:
                return pd.DataFrame()
            else:
                df_result = pd.concat([df_result, locals()['df' + str(i)]])
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
    gp = GraphProcess(config['db_conn'], config['graph_conn'])
    gp.mainGraph(2)
    # gp.findinGroup('findDown',3,10000)



























