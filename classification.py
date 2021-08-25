# -*- coding: utf-8 -*-
# @Time    : 2021/1/20 14:45
# @Author  : 莫再提
'''
--UPDATE transaction_details SET 借贷标志 = '贷' WHERE 借贷标志 = 'C';
--UPDATE transaction_details SET 借贷标志 = '借' WHERE 借贷标志 = 'D';
--UPDATE transaction_details SET 借贷标志 = '借' WHERE 借贷标志 = '2';
'''
import psycopg2
import numpy as np
import pandas as pd
from config import config
from multiprocessing import cpu_count, Pool
from functools import partial
from py2neo import Graph
from postSql import Sqlprocess

sqlpro = Sqlprocess(config['db_conn'])
sqlpro.initialization()


class Alltheory():

    def unreltion(self, row):
        if row['in_num'] < 3 and row['out_num'] < 3 and (
                row['out0'] + row['out1'] + row['out2'] + row['out3']) < 5 and (
                row['in0'] + row['in1'] + row['in2']) < 5 and row['in_money'] < 150000 and row['out_money'] < 150000 and \
                row['lable'] != 'person_1' and row['class'] == '':
            return '相关性较小人员'
        else:
            return row['class']

    def unchecked(self, row, dcry):
        if row['bankId'] in dcry:
            return '待查人员'
        else:
            return ''

    def unchecked2(self, row):
        if (row['out0'] + row['out1'] + row['out2'] + row['out3']) == 0 and (
                row['in0'] + row['in1'] + row['in2']) == 0 and (
                row['in_money'] > 300000 or row['out_money'] > 300000) and row['class'] == '':
            return '待查人员'
        else:
            return row['class']

    def gambler(self, row):
        try:
            outlv = row['out_money'] / row['out_num']
        except:
            outlv = 0
        if (row['out0'] + row['out1'] + row['out2'] + row['out3']) == 0 and (
                row['in0'] + row['in1'] + row['in2']) == 0 and row['class'] == '' and (
                outlv > 900 or row['in_money'] > 30000):
            return '码农'
        elif (row['out0'] + row['out1'] + row['out2'] + row['out3']) == 0 and (
                row['in0'] + row['in1'] + row['in2']) == 0 and row['class'] == '':
            return '赌客'
        else:
            return row['class']

    def wallet(self, row):
        if ((row['out0'] + row['out1'] + row['out2'] + row['out3']) > 0 or (
                row['in0'] + row['in1'] + row['in2']) > 0) and row['class'] == '' and row['obtainMax'] > 600000:
            return '钱包卡'
        elif ((row['out1'] + row['out2'] + row['out3']) >= (row['in0'] + row['in1'] + row['in2'])) and row[
            'class'] == '' and (row['out_num'] >= row['in_num']):
            return '大代理卡'
        elif ((row['out1'] + row['out2'] + row['out3']) < (row['in0'] + row['in1'] + row['in2'])) and row[
            'class'] == '' and (row['out_num'] < row['in_num']):
            return '码商卡'
        elif ((row['out0'] + row['out1'] + row['out2'] + row['out3']) > 0 or (
                row['in0'] + row['in1'] + row['in2']) > 0) and row['class'] == '':
            return '小代理卡'
        else:
            return row['class']

    def main(self, classdf, url):
        dfok = pd.read_excel(url)
        intervals = classdf['intervals'].tolist()
        intervals = [eval(i) for i in intervals]
        df1 = pd.DataFrame(intervals)
        intervals_in = classdf['intervals_in'].tolist()
        intervals_in = [eval(i) for i in intervals_in]
        df2 = pd.DataFrame(intervals_in)
        classdf['maxout'] = classdf['maxout'].apply(eval)
        classdf[['maxoutname', 'maxoutid', 'maxoutmoney']] = classdf['maxout'].apply(pd.Series, index=['col1', 'col2', 'col3'])
        classdf['maxin'] = classdf['maxin'].apply(eval)
        classdf[['maxinname', 'maxinid', 'maxinmoney']] = classdf['maxin'].apply(pd.Series, index=['col1', 'col2', 'col3'])
        df = pd.concat([classdf, df1, df2], axis=1)
        # df.to_excel(r'C:\Users\sky\Desktop\sql1.xlsx')
        df = df.fillna(0)
        dcry = set(dfok['bankId'].tolist())
        df['class'] = df.apply(lambda row: self.unchecked(row, dcry), axis=1)
        df['class'] = df.apply(lambda row: self.unreltion(row), axis=1)
        df['class'] = df.apply(lambda row: self.unchecked2(row), axis=1)
        df['class'] = df.apply(lambda row: self.gambler(row), axis=1)
        df['class'] = df.apply(lambda row: self.wallet(row), axis=1)




class CateGory:
    def __init__(self, graphconfig):

        self.graph = Graph(graphconfig['host'], username=graphconfig['username'],
                           password=graphconfig['password'])
        self.dfs = self.allnodes()
        #self.df_middle = self.parallelize(self.dfs, self.df_core)


    # 获取所有节点
    def allnodes(self):
        cql = "match (n) return n"
        data = self.graph.run(cql)
        df = data.to_data_frame()
        dflist = []
        for node in df['n']:
            nodejson = {}
            nodejson['lable'] = str(node.labels)[1:]
            nodejson['name'] = node['name']
            nodejson['bankId'] = node['bankId']
            nodejson['in_money'] = node['in_money']
            nodejson['out_money'] = node['out_money']
            nodejson['in_num'] = node['in_num']
            nodejson['out_num'] = node['out_num']
            dflist.append(nodejson)
        return pd.DataFrame(dflist)


def obtain(bankId):
    data = sqlpro.readP2p(bankId)
    if len(data) > 0:
        data['金额'] = data['金额'].apply(lambda x: float(x.replace(',', '')[1:]))
        data['余额'] = data['余额'].apply(lambda x: float(x.replace(',', '')[1:]))
        obtainMax = max(data['余额'].tolist())
        obtainMintime = sum([1 for i in data['余额'].tolist() if i < 1000]) / len(data['余额'].tolist())
        datain = data[data['借贷标志'] == '贷']
        dataout = data[data['借贷标志'] == '借']
        # out0 0~100, out1 100~10000, out2 10000~50000, out3 50000+
        if len(dataout) > 0:
            intervals = {'out0': 0, 'out1': 0, 'out2': 0, 'out3': 0}
            for money in dataout['金额'].tolist():
                if money < 100:
                    intervals['out0'] = intervals['out0'] + 1
                elif money < 10000:
                    intervals['out1'] = intervals['out1'] + 1
                elif money < 50100:
                    intervals['out2'] = intervals['out2'] + 1
                else:
                    intervals['out3'] = intervals['out3'] + 1
            dfout = dataout["金额"].groupby([dataout['对方账号姓名'], dataout['对方账号卡号']]).sum()
            dfout = dfout.reset_index()
            ind = dfout["金额"].idxmax()
            maxout = dfout.iloc[ind, :].tolist()
        else:
            intervals = {'out0': 0, 'out1': 0, 'out2': 0, 'out3': 0}
            maxout = ['', '', 0]
        if len(datain) > 0:
            # in0 0~10000, in1 10000~50000, in2 50000+
            intervals_in = {'in0': 0, 'in1': 0, 'in2': 0}
            for money in datain['金额'].tolist():
                if money < 10000:
                    intervals_in['in0'] = intervals_in['in0'] + 1
                elif money < 50100:
                    intervals_in['in1'] = intervals_in['in1'] + 1
                else:
                    intervals_in['in2'] = intervals_in['in2'] + 1
            dfin = datain["金额"].groupby([datain['对方账号姓名'], datain['对方账号卡号']]).sum()
            dfin = dfin.reset_index()
            ind = dfin["金额"].idxmax()
            maxin = dfin.iloc[ind, :].tolist()
        else:
            intervals_in = {'in0': 0, 'in1': 0, 'in2': 0}
            maxin = ['', '', 0]
        return pd.Series([obtainMax, obtainMintime, intervals, intervals_in, maxout, maxin])
    else:
        return pd.Series(
            ['', '', {'out0': 0, 'out1': 0, 'out2': 0, 'out3': 0}, {'in0': 0, 'in1': 0, 'in2': 0}, ['', '', 0],
             ['', '', 0]])



partitions = cpu_count()
def parallelize(df, func):
    """
    多cpu并行处理
    :param df: df数据
    :param func: 处理函数
    :return: 处理结果
    """
    data_split = np.array_split(df, partitions)
    pool = Pool(partitions)
    # chain.from_iterable
    data = pool.map(func, data_split)
    pool.close()
    pool.join()
    return data

def df_core(dfs):
    dfs[['obtainMax', 'obtainMintime', 'intervals', 'intervals_in', 'maxout', 'maxin']] = dfs['bankId'].apply(
            lambda x: obtain(x))
    return dfs




if __name__ == "__main__":
    cg = CateGory(config['graph_conn'])
    df_middle = parallelize(cg.dfs, df_core)
    dfs = pd.concat(df_middle)
    dfs.to_excel(r'C:\Users\DELL\Desktop\sql.xlsx')





