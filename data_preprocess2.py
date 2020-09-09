# -*- coding: utf-8 -*-
"""
Created on Sun Sep  6 20:16:15 2020

@author: cyc
"""
import numpy as np 
import pandas as pd
import os

hangye=pd.read_csv('股票所属行业.csv')
result=[]
for i in hangye['stock_id'].values:
    result.append('C{}'.format(i.split('.')[0]))
hangye['stock_id']=result
hangye=hangye.rename(columns={'stock_id':'上市公司代码_Comcd'})
print(hangye)
#deta2
def filter_extreme_MAD(series,n): #MAD
  median = series.quantile(0.5)
  new_median = ((series - median).abs()).quantile(0.50)
  max_range = median + n*new_median
  min_range = median - n*new_median
  return np.clip(series,min_range,max_range)
#z-score
def standard_z_score(series):
    std = series.std()
    mean = series.mean()
    return (series-mean)/std
def Industry_neutrality(df,t,c):
    try:
      df=pd.merge(df,hangye)
    except:
       return df
    d = df[[t, c, 'industry']]
    mean_d = d.groupby(by=['industry', t]).mean().reset_index()
    mean_d=mean_d.rename(columns={c: 'mean'})
    std_d = d.groupby(by=['industry',t]).std().reset_index()
    std_d = std_d.rename(columns={c: 'std'})
    df=pd.merge(df,mean_d)
    df=pd.merge(df,std_d)
    df[c]=(df[c]-df['mean'])/df['std']
    return df

#StandardDeviationOfDailyReturn120
StandardDeviationOfDailyReturn120=pd.DataFrame(columns=['上市公司代码_Comcd','最新股票名称_Lstknm','日期_Date','日收益标准差_120日移动平均_Dstd120'])
for i in range(1,14):
    file='../data/deta2/StandardDeviationOfDailyReturn120/{}.csv'.format(i)
    data=pd.read_csv(file,encoding='GBK')
    for i in data.columns:
        if data[i].count() == 0:
            data.drop(labels=i, axis=1, inplace=True)
    StandardDeviationOfDailyReturn120=StandardDeviationOfDailyReturn120.append(data)

StandardDeviationOfDailyReturn120['日收益标准差_120日移动平均_Dstd120']=standard_z_score(filter_extreme_MAD(StandardDeviationOfDailyReturn120['日收益标准差_120日移动平均_Dstd120'],5))
StandardDeviationOfDailyReturn120=Industry_neutrality(StandardDeviationOfDailyReturn120,'日期_Date','日收益标准差_120日移动平均_Dstd120')
StandardDeviationOfDailyReturn120.to_csv('../preprocess_data/deta2/StandardDeviationOfDailyReturn120.csv',index=False,encoding='GBK')

def preprocess1(csv_name):
    try:
      data=pd.read_csv('../data/deta2/{}'.format(csv_name))
    except:
        data = pd.read_csv('../data/deta2/{}'.format(csv_name),encoding='GBK')
    for i in data.columns:  

        if data[i].count() == 0:
            data.drop(labels=i, axis=1, inplace=True)
    data=data.dropna(axis=0, how='any')
    data[data.columns[-1]] = standard_z_score(filter_extreme_MAD(data[data.columns[-1]], 5))
    data=Industry_neutrality(data,data.columns[-2],data.columns[-1])
    data=data.fillna(0)
    data.to_csv('../preprocess_data/deta2/{}'.format(csv_name),index=False,encoding='GBK')
def preprocess2(csv_name):
    try:
      data=pd.read_csv('../data/deta3/{}'.format(csv_name))
    except:
        data = pd.read_csv('../data/deta3/{}'.format(csv_name),encoding='GBK')
    for i in data.columns:  
        if data[i].count() == 0:
            data.drop(labels=i, axis=1, inplace=True)
    data=data.dropna(axis=0, how='any')
    data[data.columns[-1]] = standard_z_score(filter_extreme_MAD(data[data.columns[-1]], 5))
    data = Industry_neutrality(data,data.columns[-2], data.columns[-1])
    data = data.fillna(0)
    data.to_csv('../preprocess_data/deta3/{}'.format(csv_name),index=False,encoding='GBK')
def flatten(a):
    if not isinstance(a, (list, )):
        return [a]
    else:
        b = []
        for item in a:
            b += flatten(item)
    return b
def preprocess3(csv_name):
    hangye = pd.read_csv('股票所属行业.csv')
    new_data=pd.DataFrame()
    try:
      data=pd.read_csv('../data/因子数据1/{}'.format(csv_name),index_col=0)
    except:
        data = pd.read_csv('../data/因子数据1/{}'.format(csv_name),encoding='GBK',index_col=0)
    code=[]
    d=[]
    t=[]
    for i in data.columns: 
       code.append([i for j in range(data.shape[0])])
       d.append(data[i].values)
       t.append(data.index.values)
    code=np.array(code).reshape(-1)
    d=np.array(d).reshape(-1)
    t=np.array(t).reshape(-1)
    new_data['stock_id']=code
    new_data[csv_name.split('.')[0]]=d
    new_data['date']=t
    new_data=new_data.fillna(0)
    new_data[csv_name.split('.')[0]] = standard_z_score(filter_extreme_MAD(new_data[csv_name.split('.')[0]], 5))
    new_data=pd.merge(new_data,hangye)
    d = new_data[['date', new_data.columns[-3], 'industry']]
    mean_d = d.groupby(by=['industry', 'date']).mean().reset_index()
    mean_d=mean_d.rename(columns={new_data.columns[-3]: 'mean'})
    std_d = d.groupby(by=['industry','date']).std().reset_index()
    std_d = std_d.rename(columns={new_data.columns[-3]: 'std'})
    new_data=pd.merge(new_data,mean_d)
    new_data=pd.merge(new_data,std_d)
    print(new_data)
    new_data[new_data.columns[-5]]=(new_data[new_data.columns[-5]]-new_data['mean'])/new_data['std']
    new_data.to_csv('../preprocess_data/因子数据/{}'.format(csv_name),index=False,encoding='GBK')

def name(file_path):
    file_name = []
    a = os.listdir(file_path)
    for j in a:
        if os.path.splitext(j)[1] == '.csv':
            file_name.append(j)
    return file_name
file_name=name(r'../data/deta2')
print(file_name)
for i in file_name:
   print(i)
   preprocess1(i)

file_name=name(r'../data/因子数据1')
for i in file_name:
   print(i)
   preprocess3(i)

file_name=name(r'../data/deta3')
print(file_name)
for i in file_name:
   print(i)
   preprocess2(i)
##DailyReturnVolatlity20
DailyReturnVolatlity20=pd.DataFrame(columns=['上市公司代码_Comcd','最新股票名称_Lstknm','日期_Date','波动率_20日简单移动平均()_Sma20'])
for i in range(1,14):
    file='../data/deta3/DailyReturnVolatlity20/{}.csv'.format(i)
    data=pd.read_csv(file)
    for i in data.columns:
        if data[i].count() == 0:
            data.drop(labels=i, axis=1, inplace=True)
    DailyReturnVolatlity20=DailyReturnVolatlity20.append(data)
DailyReturnVolatlity20['波动率_20日简单移动平均()_Sma20']=standard_z_score(filter_extreme_MAD(DailyReturnVolatlity20['波动率_20日简单移动平均()_Sma20'],5))
DailyReturnVolatlity20=Industry_neutrality(DailyReturnVolatlity20,'日期_Date','波动率_20日简单移动平均()_Sma20')
DailyReturnVolatlity20=DailyReturnVolatlity20.fillna(0)
print(DailyReturnVolatlity20)
DailyReturnVolatlity20.to_csv('../preprocess_data/deta3/DailyReturnVolatlity20.csv',index=False,encoding='GBK')

##DailyReturnVolatlity60
DailyReturnVolatlity60=pd.DataFrame(columns=['上市公司代码_Comcd','最新股票名称_Lstknm','日期_Date','波动率_60日简单移动平均()_Sma60'])
for i in range(1,14):
    file='../data/deta3/DailyReturnVolatlity60/{}.csv'.format(i)
    data=pd.read_csv(file)
    for i in data.columns:
        if data[i].count() == 0:
            data.drop(labels=i, axis=1, inplace=True)
    DailyReturnVolatlity60=DailyReturnVolatlity60.append(data)
DailyReturnVolatlity60['波动率_60日简单移动平均()_Sma60']=standard_z_score(filter_extreme_MAD(DailyReturnVolatlity60['波动率_60日简单移动平均()_Sma60'],5))
DailyReturnVolatlity60=Industry_neutrality(DailyReturnVolatlity60,'日期_Date','波动率_60日简单移动平均()_Sma60')
DailyReturnVolatlity60=DailyReturnVolatlity60.fillna(0)
print(DailyReturnVolatlity60)

DailyReturnVolatlity60.to_csv('../preprocess_data/deta3/DailyReturnVolatlity60.csv',index=False,encoding='GBK')

##DailyReturnVolatlity120
DailyReturnVolatlity120=pd.DataFrame(columns=['上市公司代码_Comcd','最新股票名称_Lstknm','日期_Date','波动率_120日简单移动平均()_Sma120'])
for i in range(1,14):
    file='../data/deta3/DailyReturnVolatlity120/{}.csv'.format(i)
    data=pd.read_csv(file)
    for i in data.columns:
        if data[i].count() == 0:
            data.drop(labels=i, axis=1, inplace=True)
    DailyReturnVolatlity120=DailyReturnVolatlity120.append(data)
DailyReturnVolatlity120['波动率_120日简单移动平均()_Sma120']=standard_z_score(filter_extreme_MAD(DailyReturnVolatlity120['波动率_120日简单移动平均()_Sma120'],5))
#print(DailyReturnVolatlity120)
DailyReturnVolatlity120=Industry_neutrality(DailyReturnVolatlity120,'日期_Date','波动率_120日简单移动平均()_Sma120')
DailyReturnVolatlity120=DailyReturnVolatlity120.fillna(0)
DailyReturnVolatlity120.to_csv('../preprocess_data/deta3/DailyReturnVolatlity120.csv',index=False,encoding='GBK')
###next_rtn
month_return=pd.read_csv('../data/因子数据1/## 股票月度收益率.csv',index_col=0)
print(month_return)
month_next_return=month_return.shift(-1)
month_next_return=month_next_return.fillna(0)
print(month_next_return)
month_next_return.to_csv('../preprocess_data/因子数据/next_rtn.csv')
