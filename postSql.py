# -*- coding: utf-8 -*-
# @Time    : 2020/9/15 16:18
# @Author  : 莫再提
import psycopg2
import pandas as pd
from config import config

# 数据库的存储及查询模块
class Sqlprocess():
    def __init__(self, config):
        self.config = config
        self.columnsTable = "收钱方, 出钱方, 查询账号, 对方账号姓名, 对方账号卡号, 金额, 余额, 借贷标志, 交易类型, 交易结果, 交易时间, 交易开户行, 交易网点名称, 交易流水号, 凭证号, 终端号, 现金标志, 交易摘要, 商户名称, IP地址, MAC地址, 唯一id, 人物等级"
        self.columnsTs = (len(self.columnsTable.split(","))*"%s, ")[:-2]

    # 初始化处理函数
    def initialization(self):
        try:
            self.conn = psycopg2.connect(database=self.config['database'], user=self.config['user'],
                                    password=self.config['password'], host=self.config['host'],
                                    port=self.config['port'])

        except:
            print("数据库连接失败")

    def readCardnum(self, currentPage, pageSize, cardNum=None):
        data_jump = (currentPage - 1) * pageSize
        if cardNum is None:
            sql_command: str = "select * from {} order by 人物等级 limit {} offset {}".format(self.config['tableName'], pageSize, data_jump)
            sql_data: str = "select count(*) from {}".format(self.config['tableName'])
        else:
            sql_command: str = "select * from {} where 查询账号 = '{}' limit {} offset {}".format(self.config['tableName'], cardNum, pageSize, data_jump)
            sql_data: str = "select count(*) from {} where 查询账号 = '{}'".format(self.config['tableName'], cardNum)
        data = pd.read_sql(sql_command, self.conn)
        nums = pd.read_sql(sql_data, self.conn)
        totals = nums['count'][0]
        return data, totals


    # 获取全部数据
    def readData(self, types):
        if types == 0:
            sql_command: str = "select * from {}".format(self.config['cacheName'])
        else:
            sql_command: str = "select * from {} order by 人物等级".format(self.config['partName'])
        data = pd.read_sql(sql_command, self.conn)
        return data


    # 获取全部已查询数据
    def readUNData(self):
        sql_command = "select 查询账号 from {}".format(self.config['tableName'])
        data = pd.read_sql(sql_command, self.conn)
        return data

    # 数据唯一性检测
    def dfuniq(self, df_z):
        try:
            sql_command = "SELECT 唯一id FROM zmd_transaction_details"
            rows = pd.read_sql(sql_command, self.conn)
            data = rows['唯一id'].tolist()
            if data:
                id_list = []
                for i in data:
                    id_list.append(i)
                inter = list(set(df_z['唯一id']).intersection(id_list))
                if inter:
                    df_z = df_z[~df_z.isin([inter])]
            return df_z
        except:
            print('2错误')
            return True

    # 插入数据库的方法
    def insertRow(self, strings, type_s=0):
        # 这里就不需要遍历了，因为executemany接受
        # for index in range(len(rows)):
        try:
            cur = self.conn.cursor()
            if type_s == 0:
                sql: str = "INSERT INTO {}(银行卡, 姓名) VALUES(%s,%s) ON CONFLICT (银行卡) DO UPDATE SET 姓名 = EXCLUDED.姓名".format(self.config['idolName'])
                sql1: str = "INSERT INTO {}(银行卡, 姓名) VALUES(%s,%s) ON CONFLICT (银行卡) DO UPDATE SET 姓名 = EXCLUDED.姓名".format(self.config['cacheName'])
            else:
                sql: str = "INSERT INTO {}({}) VALUES({}) on CONFLICT (唯一id) DO UPDATE SET 人物等级 = EXCLUDED.人物等级".format(self.config['tableName'], self.columnsTable, self.columnsTs)
                sql1: str = "INSERT INTO {}({}) VALUES({}) on CONFLICT (唯一id) DO UPDATE SET 人物等级 = EXCLUDED.人物等级".format(self.config['partName'], self.columnsTable, self.columnsTs)
            cur.executemany(sql1, strings)
            cur.executemany(sql, strings)
            self.conn.commit()

        except:
            print('3错误')
        #self.conn.close()

    # 清空数据库
    def truncate(self):
        try:
            cur = self.conn.cursor()
            sql: str = 'TRUNCATE table {},{}'.format(self.config['partName'], self.config['cacheName'])
            cur.execute(sql)
            self.conn.commit()
        except:
            print('1错误')

    # 查询人的数据
    def readP2p(self, bankId, bankId2=None):
        if bankId2 is None:
            sql_command = "select * from {} where 查询账号 = '{}'".format(self.config['tableName'], bankId)
        else:
            if len(bankId) == 1 and len(bankId2) == 1:
                sql_command = "select * from {} where 出钱方='{}' and 收钱方='{}'".format(self.config['tableName'], bankId[0], bankId2[0])
            if len(bankId) > 1 and len(bankId2) == 1:
                tup_bankId = tuple(bankId)
                sql_command = "select * from {} where 出钱方 in {} and 收钱方='{}'".format(self.config['tableName'], tup_bankId, bankId2[0])
            if len(bankId) == 1 and len(bankId2) > 1:
                tup_bankId2 = tuple(bankId2)
                sql_command = "select * from {} where 出钱方='{}' and 收钱方 in {}".format(self.config['tableName'], bankId[0], tup_bankId2)
            if len(bankId) > 1 and len(bankId2) > 1:
                tup_bankId = tuple(bankId)
                tup_bankId2 = tuple(bankId2)
                sql_command = "select * from {} where 出钱方 in {} and 收钱方 in {}".format(self.config['tableName'], tup_bankId,
                                                                                      tup_bankId2)
        try:
            data = pd.read_sql(sql_command, self.conn)
        except:
            print("查询数据错误：错误命令<<<{}>>>".format(sql_command))
            data = pd.DataFrame()
        return data

    # 随机获取一个查询账号
    def randomNode(self):
        sql_command = "select 查询账号 from {} order by 人物等级 desc limit 1".format(self.config['tableName'])
        data = pd.read_sql(sql_command, self.conn)
        return data['查询账号'][0]






if __name__ == "__main__":
    #数据测试
    sqlpro = Sqlprocess(config['db_conn'])
    sqlpro.initialization()
    sqlpro.insertRow([['6212262102017439227', '9912900000153300', '6212262102017439227', '支付宝（中国）网络技术有限公司', '9912900000153300', 29.91, '12335.59', '贷', '银联入账', '成功', '20201229125831', '', '工行广西南宁市佛子岭支行', '00000001140', '', '01080209', '其他', '正常', '', '', '', '6212262102017439227$9912900000153300$20201229125831$29.91$12335.59', 'person_2']], type_s=1)
    # z = sqlpro.readData(0)
    # df = pd.read_excel(r'C:\Users\DELL\Desktop\Desktop\二级卡\6212262102017439227吴小罗.xlsx')
    # df = df[['银行卡', '姓名']]
    # df = df.values.tolist()
    # sqlpro.insertRow(df)
    # print(df1)


