# -*- coding: utf-8 -*-
# @Time    : 2021/1/29 15:08
# @Author  : 莫再提
import os
import pandas as pd
from config import config
from creatGraph import GraphProcess


gp = GraphProcess(config['db_conn'], config['graph_conn'])

class Proof():

    def desktopPath(self):
        return os.path.join(os.path.expanduser("~"), 'Desktop')

    def nodeschange(self, list_s, node_rel):
        nodes = []
        relations = []
        for node_one in list_s:
            label = str(node_one.labels)[1:]
            name = node_one['name']
            bankId = node_one['bankId']
            z = "(" + name + "是" + label + "级卡，他的卡号为" + bankId + ")"
            nodes.append(z)
        for rel_one in node_rel:
            money = str(rel_one['money'])
            times = str(rel_one['times'])
            z = '[' + '交易次数：' + times + '，总资金流出：' + money + ']'
            relations.append(z)
        result = ''
        for i in range(len(nodes), 0, -1):
            if i != 1:
                result = result + nodes[i - 1] + "->资金流向->" + relations[i - 2]
            else:
                result = result + "->资金流向->" + nodes[i - 1]
        return result

    def dfchange(self, grade,bankId):
        try:
            df = gp.find_relation(grade, bankId, 50000, 1)
        except:
            df = gp.find_relation(grade, bankId, 1000, 0)
        try:
            n_list = df['data'].tolist()
        except:
            n_list = []
        resultz = []
        for node_one in n_list:
            resultz.append(self.nodeschange(node_one.nodes, node_one.relationships))
        return pd.DataFrame({'Capital_flow': resultz})

    def get_detailed(self, grade, df):
        result = []
        for i in range(grade):
            locals()['l' + str(i)] = []
        for index, row in df.iterrows():
            if len(row['data']) > 0:
                for i in range(grade):
                    dictNode = {}
                    try:
                        locals()['l' + str(i)].append(row['data'].nodes[i]['bankId'])
                    except:
                        pass

        df_result = pd.DataFrame()
        for i in range(grade - 1):
            locals()['df' + str(i)] = gp.readP2p(locals()['l' + str(i + 1)], locals()['l' + str(i)])
            locals()['df' + str(i)]['查询账号姓名'] = locals()['df' + str(i)]['查询账号'].apply(gp.getName)
            result.append(locals()['df' + str(i)])
        return result

    def pro_main(self, grade, bankId):
        try:
            url = self.desktopPath()
            df = self.dfchange(grade, bankId)
            names = bankId + gp.getName(bankId) + '.xlsx'
            urls = os.path.join(url, names)
            dfz = gp.find_relation(grade, bankId, money=100000, times=1)
            dfres = self.get_detailed(grade, dfz)
            writer = pd.ExcelWriter(urls)
            df.to_excel(writer, "汇总")
            for i in range(len(dfres)):
                dfres[i].to_excel(writer, str(i))
            writer.save()
        except:
            print("导出文件错误")


if __name__ == "__main__":

    pf = Proof()
    print(pf.dfchange("6217002870090896364"))









