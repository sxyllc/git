# -*- coding: utf-8 -*-

import os
import shutil
import pandas as pd
from config import config
from postSql import Sqlprocess
from matchDef import change_df, if_only, get_filelist



# 数据初始化
class DataProcess(Sqlprocess):

    def __init__(self, config):
        Sqlprocess.__init__(self, config)
        Sqlprocess.initialization(self)


    # 数据清洗(完表数据)
    def dfclearn(self, grade, paths):
        dfs = pd.DataFrame()
        filelist = os.listdir(paths)
        fileslist = [os.path.join(paths, files) for files in filelist]
        for files in fileslist:
            df = pd.read_excel(files, converters={"对方账号卡号": str, '查询账号': str, '金额': str})
            dfs = pd.concat([dfs, df])

        dfs['对方账号卡号'] = dfs['对方账号卡号'].apply(lambda x: str(x))
        dfs['查询账号'] = dfs['查询账号'].apply(lambda x: str(x))
        dfs['人物等级'] = 'person_' + str(grade)
        df_z = dfs[(dfs['对方账号卡号'].notnull()) & (dfs['对方账号卡号'] != '-') & (dfs['对方账号卡号'] != 'nan')]
        df_z = df_z[(df_z['借贷标志'].notnull()) & (df_z['借贷标志'] != '-') & (df_z['金额'].notnull()) & (df_z['金额'] != '-')]
        df_z['对方账号姓名'] = df_z['对方账号姓名'].fillna('')
        df_z['金额'] = df_z['金额'].apply(lambda x: abs(float(x)))
        df_z['收钱方'], df_z['出钱方'] = zip(*df_z.apply(lambda row: change_df(row), axis=1))
        df_z = df_z.reset_index()
        df_z['交易时间'] = df_z['交易时间'].apply(lambda x: str(x))
        df_z["唯一id"] = df_z.apply(
            lambda x: x['查询账号'] + '$' + x['对方账号卡号'] + '$' + x['交易时间'] + '$' + str(x['金额']) + '$' + str(x['余额']), axis=1)
        df_z = df_z.drop_duplicates(['唯一id'])  # 去除重复数据例如a与b转账的同时，b的转账关系中也存在
        df_z = df_z.fillna("")
        df_z = Sqlprocess.dfuniq(self, df_z)
        return df_z

    # 数据插入(完表数据)
    def dfinsert(self, grade, paths):
        try:
            df_z = self.dfclearn(grade, paths)
            df_z = df_z[[name.strip() for name in self.columnsTable.split(",")]]
            df_z = df_z.values.tolist()
            Sqlprocess.insertRow(self, df_z, type_s=1)
            try:
                shutil.rmtree(paths)
            except:
                print('文件夹不存在')
            os.mkdir(paths)
            return True
        except:
            print('数据录入出错')

    # 一级数据清洗
    def dfOneclearn(self, paths):
        dfs = pd.DataFrame()
        filelist = os.listdir(paths)
        fileslist = [os.path.join(paths, files) for files in filelist]
        for files in fileslist:
            df = pd.read_excel(files, converters={"银行卡": str, '姓名': str})
            df = df[['银行卡', '姓名']]
            dfs = pd.concat([dfs, df])

        dfs['银行卡'] = dfs['银行卡'].apply(lambda x: x.replace(' ', ''))
        dfs['姓名'] = dfs['姓名'].apply(str)
        dfs['姓名'] = dfs['姓名'].apply(lambda x: x.replace(' ', ''))
        dfs = dfs[['银行卡', '姓名']]
        dfs = dfs.fillna("")
        dfs = dfs.values.tolist()
        try:

            Sqlprocess.insertRow(self, dfs)
            return True
        except:
            print("一级数据录入错误")



if __name__ == "__main__":
    # 数据库初始化
    dp = DataProcess(config['db_conn'])
    # dp.dfOneclearn(r'C:\Users\DELL\Desktop\Desktop\一级卡')
    # dp.dfinsert(2, r'C:\Users\sky\Desktop\二级卡')
    dp.dfinsert(2, r'C:\Users\DELL\Desktop\资金分析\驻马店一期\数据\二级卡')
