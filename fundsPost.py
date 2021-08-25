# -*- coding: utf-8 -*-

import os
import json
import pandas as pd
import win32api, win32gui
from config import config
from flask import Flask, request, Response
from dataEntry import DataProcess
from postSql import Sqlprocess
from creatGraph import GraphProcess
from detailedlist import Proof
from werkzeug.utils import secure_filename
from flask_cors import *


app = Flask(__name__)
CORS(app, supports_credentials=True)

ct = win32api.GetConsoleTitle()
hd = win32gui.FindWindow(0,ct)
win32gui.ShowWindow(hd,0)

# app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = config['pFiles']
app.config['UPLOAD_FIR'] = config['entryFirst']
app.config['UPLOAD_URL'] = config['entryUrl']


# 初始化程序
sqlpro = Sqlprocess(config['db_conn'])
sqlpro.initialization()
dp = DataProcess(config['db_conn'])
gp = GraphProcess(config['db_conn'], config['graph_conn'])
pf = Proof()

# 只接受post访问
# 数据上传
@app.route('/analysis/fileloader', methods = ['GET', 'POST'])
def fileloader():
   if request.method == 'POST':
       data = int(request.form['level'])
       files = request.files.getlist('files')
       if data>1:
           for f in files:
               filesUrl = os.path.join(app.config['UPLOAD_URL'],secure_filename(f.filename))
               f.save(filesUrl)
           dp.dfinsert(data, app.config['UPLOAD_URL'])
           gp.mainGraph(data)
       if data == 1:
           for f in files:
               filesUrl = os.path.join(app.config['UPLOAD_FIR'],secure_filename(f.filename))
               f.save(filesUrl)
           dp.dfOneclearn(app.config['UPLOAD_FIR'])
           try:
               gp.mainGraph(data)
           except:
               print('一级数据创建图失败，请检测是否存在其他级别数据')
       return 'files uploader success'



# 数据查询功能
'''
输入数据：页码，每页限制量，是否查询，
建议currentPage：int
pageSize int
cardNum str
'''
@app.route("/analysis/enigo", methods=["post"])
def check():
    # 默认返回内容
    return_dict = {'return_uid': '200', 'return_info': '处理成功', 'result': False, 'totals':False}
    # 判断入参是否为空
    data = request.get_data()
    json_data = json.loads(data.decode('utf-8'))
    currentPage = json_data['currentPage']
    pageSize = json_data['pageSize']
    cardNum = json_data['cardNum']
    # 获取传入的params参数
    if len(cardNum) == 0:
        get_data, totals = dp.readCardnum(currentPage, pageSize)

    else:
        get_data, totals = dp.readCardnum(currentPage, pageSize, cardNum)
    get_data.columns = ["Payee", "Payer", "Inquiry_account", "name", "name_account", "money", "balance", "sign", "Transaction_type", "Transaction_result", "Transaction_time", "Transaction_bank", "Transaction_webname",
                        "Transaction_serial", "Transaction_certificate", "Transaction_terminal", "Transaction_cash", "Transaction_summary", "Transaction_name", "Transaction_ip", "Transaction_mac", "onlyid", "grade"]
    get_data = get_data.to_json(orient='records', force_ascii=False)
    return_dict['result'] = eval(get_data)
    return_dict['totals'] = str(totals)
    return json.dumps(return_dict, ensure_ascii=False)

# 资金流查询
'''
type_s控制流入或流出查询，1为流入查询，2为流出查询
cardStart：字符串
cardEnd：字符串，当其为空则进行单个查询，否则进行多个查询
moneyMin：int
timesMin：int
'''
@app.route("/analysis/query", methods=["post"])
def checs():
    # 默认返回内容
    return_dict = {'return_uid': '200', 'return_info': '处理成功', 'result': False}
    # 判断入参是否为空
    data = request.get_data()
    json_data = json.loads(data.decode('utf-8'))
    cardStart = json_data['cardStart']
    cardEnd = json_data['cardEnd']
    type_s = json_data['type_s']
    moneyMin = json_data['moneyMin']
    timesMin = json_data['timesMin']
    if cardStart == '':
        cardStart = dp.randomNode()
    else:
        pass
    if len(cardEnd)==0:
        neo4j_data = gp.queryOne(cardStart, type_s, moneyMin, timesMin)
    else:
        neo4j_data = gp.queryTwo(3, cardStart, cardEnd, moneyMin, timesMin)
    return_dict['result'] = neo4j_data
    return json.dumps(return_dict, ensure_ascii=False)



# 异常群体发现
'''
type_s: 群体发现类型
grade: 等级
money: 金额
relevance:与上级人员紧密程度
times: 交易次数限制
'''
@app.route("/analysis/group", methods=["post"])
def community():
    # 默认返回内容
    return_dict = {'return_uid': '200', 'return_info': '处理成功', 'nodes': False}
    # 判断入参是否为空
    data = request.get_data()
    json_data = json.loads(data.decode('utf-8'))
    type_s = json_data['type_s']
    grade = json_data['grade']
    money = json_data['money']
    times = json_data['times']
    relevance = json_data['relevance']
    df = gp.findinGroup(type_s, grade, money, relevance, times)
    nodes = []
    duplicate_set = set()
    for index, row in df.iterrows():
        node_dict = {}
        bankId = row['n.bankId']
        node_dict['name'] = row['n.name']
        node_dict['bankId'] = bankId
        node_dict['ifIN'] = row['ifIN']
        if bankId not in duplicate_set:
            nodes.append(node_dict)
            duplicate_set.add(bankId)
    return_dict['nodes'] = nodes
    return json.dumps(return_dict, ensure_ascii=False)


# 冻结时间预测
'''
id 传入单个参数id
输出最佳冻结时间
及整体交易时间统计图
'''
@app.route("/analysis/archive", methods=["post"])
def pred_time():
    # 默认返回内容
    return_dict = {'return_uid': '200', 'return_info': '处理成功', 'data': False, 'histogramK': False, 'histogramV':False}
    # 判断入参是否为空
    data = request.get_data()
    json_data = json.loads(data.decode('utf-8'))
    bankId = json_data['id']
    if bankId =='':
        bankId = dp.randomNode()
    data = {}
    data['times'],data['name'], return_dict['histogramK'], return_dict['histogramV'] = gp.archiveTime(bankId)
    data['bankId'] = bankId
    data = [data]
    return_dict['data'] = data

    return_dict = json.dumps(return_dict, ensure_ascii=False)
    return return_dict


# 批量冻结时间预测

@app.route('/analysis/uploader', methods = ['GET', 'POST'])
def uploader():
   return_dict = {'return_uid': '200', 'return_info': '处理成功', 'data': False}
   if request.method == 'POST':
       f = request.files['file']
       filesUrl = os.path.join(app.config['UPLOAD_FOLDER'],secure_filename(f.filename))
       f.save(filesUrl)
       df = pd.read_excel(filesUrl)
       if  '银行卡' in df.columns.values.tolist() and len(df)>0:
           df['预测交易时间'], df['姓名'] = zip(*df.apply(lambda x:gp.archiveAll(x['银行卡']),axis=1))
       else:
           df = pd.DataFrame()
       df = df[['银行卡', '预测交易时间', '姓名']]
       df.columns = ['bankId', 'times', 'name']
       df = df.to_json(orient='records', force_ascii=False)
       return_dict['data'] = eval(df)
   return json.dumps(return_dict, ensure_ascii=False)



# 资金往来
@app.route("/analysis/chain", methods=["post"])
def pred_chain():
    # 默认返回内容
    return_dict = {'return_uid': '200', 'return_info': '处理成功', 'result':False}
    # 判断入参是否为空
    data = request.get_data()
    json_data = json.loads(data.decode('utf-8'))
    bankId = json_data['id']
    if bankId == '':
        bankId = dp.randomNode()
    else:
        pass
    money = json_data['money']
    times = json_data['times']
    return_dict['result'] = gp.find_chain(bankId, money, times)
    return json.dumps(return_dict, ensure_ascii=False)



# 交易证据链
@app.route("/analysis/detailedChain", methods=["post"])
def predChain():
    # 默认返回内容
    return_dict = {'return_uid': '200', 'return_info': '处理成功', 'result':False, 'outEx':False}
    # 判断入参是否为空
    data = request.get_data()
    json_data = json.loads(data.decode('utf-8'))
    bankId = json_data['id']
    if bankId == '':
        bankId = dp.randomNode()
    else:
        pass
    money = json_data['money']
    times = json_data['times']
    grade = gp.getGrade(bankId)
    df = gp.find_relation(grade, bankId, money, times)
    return_dict['result'] = gp.get_reltable(grade,df)
    get_data = pf.dfchange(grade, bankId)
    get_data = get_data.to_json(orient='records', force_ascii=False)
    return_dict['outEx'] = eval(get_data)
    return json.dumps(return_dict, ensure_ascii=False)
#
#
# # # 交易证据链
# # @app.route("/zokomfenxi", methods=["post"])
# # def zjfx():
# #     # 默认返回内容
# #     return_dict = {'return_uid': '200', 'return_info': '处理成功', 'result': False}
# #     # 判断入参是否为空
# #     data = request.get_data()
# #     json_data = json.loads(data.decode('utf-8'))
# #     bankId = json_data['id']
# #     if bankId == '':
# #         bankId = '6228480178320188877'
# #     # 获取传入的params参数
# #     get_data = dfchange(bankId)
# #     get_data = get_data.to_json(orient='records', force_ascii=False)
# #     return_dict['result'] = eval(get_data)
# #     return json.dumps(return_dict, ensure_ascii=False)

if __name__ == "__main__":
    # app.run(host='192.168.3.174', port=5000, debug=True)
    # app.run(host='192.168.3.51',port=5000, debug=True)
    app.run(host='localhost', port=5000, debug=True)
    # app.run(host='192.168.2.156', port=5000, debug=True)