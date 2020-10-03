# -*- coding: utf-8 -*-
"""
Created on Fri Sep 11 16:57:37 2020

@author: Wang yidi
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
#舍弃数据完全缺失公司，并将数据写入preprocess/deta5中
def drop_films4(data_name):
    df=pd.read_csv('../preprocess_data/deta4/{}'.format(data_name), encoding='GBK')
    df1=df.pivot(index='日期_Date', columns='上市公司代码_Comcd', values=data_name[0:-4])
    print(data_name[0:-4]+'\n')
    df2=df1.dropna(how='all',axis = 1)
    df2=df2.fillna(0)
    if len(df1.columns)==len(df2.columns):
        print('no drop\n')
    else:
        print('drop number='+str(len(df1.columns)-len(df2.columns))+'\n')
    df=df[df.上市公司代码_Comcd.apply(lambda x:x in df2.columns)]
    print(df)
    df.to_csv('../preprocess_data/deta5/dropna_{}'.format(data_name),index=False,encoding='GBK')

def drop_films1(data_name):
    try:
        df=pd.read_csv('../data/deta2/{}'.format(data_name))
    except:
        df=pd.read_csv('../data/deta2/{}'.format(data_name), encoding='GBK')
    df.drop([df.columns[1],df.columns[2]],axis=1,inplace=True)
    df=df.drop_duplicates(subset=[df.columns[0], df.columns[1]], keep='first')
    df1=df.pivot(index=df.columns[1], columns=df.columns[0], values=df.columns[2])
    print(df.columns[2]+'\n')
    df2=df1.dropna(how='all',axis = 1)
    df2=df2.fillna(0)
    if len(df1.columns)==len(df2.columns):
        print('no drop\n')
    else:
        print('drop number='+str(len(df1.columns)-len(df2.columns))+'\n')
    df=df[df.iloc[:,0].apply(lambda x:x in df2.columns)]
    print(df)
    df.to_csv('../preprocess_data/deta6/{}'.format(data_name),index=False,encoding='GBK')

def drop_films2(data_name):
    try:
        df=pd.read_csv('../data/deta3/{}'.format(data_name))
    except:
        df=pd.read_csv('../data/deta3/{}'.format(data_name), encoding='GBK')
    df.drop([df.columns[1]],axis=1,inplace=True)
    df=df.drop_duplicates(subset=[df.columns[0], df.columns[1]], keep='first')
    df1=df.pivot(index=df.columns[1], columns=df.columns[0], values=df.columns[2])
    print(df.columns[2]+'\n')
    df2=df1.dropna(how='all',axis = 1)
    df2=df2.fillna(0)
    if len(df1.columns)==len(df2.columns):
        print('no drop\n')
    else:
        print('drop number='+str(len(df1.columns)-len(df2.columns))+'\n')
    df=df[df.iloc[:,0].apply(lambda x:x in df2.columns)]
    print(df)
    df.to_csv('../preprocess_data/deta7/{}'.format(data_name),index=False,encoding='GBK')

#数据处理deta2
def preprocess1(csv_name):
    data = pd.read_csv('../preprocess_data/deta6/{}'.format(csv_name),encoding='GBK')
    for i in data.columns:  
        if data[i].count() == 0:
            data.drop(labels=i, axis=1, inplace=True)
        else:
            continue
    data=data.dropna(axis=0, how='any')
    data[data.columns[-1]]=standard_z_score(filter_extreme_MAD(data[data.columns[-1]], 5))
    data=Industry_neutrality(data,data.columns[-2],data.columns[-1])
    data=data.fillna(0)
    data.to_csv('../preprocess_data/deta2/{}'.format(csv_name),index=False,encoding='GBK')
#数据处理deta3
def preprocess2(csv_name):
    data = pd.read_csv('../preprocess_data/deta7/{}'.format(csv_name),encoding='GBK')
    for i in data.columns:  
        if data[i].count() == 0:
            data.drop(labels=i, axis=1, inplace=True)
    data=data.dropna(axis=0, how='any')
    data[data.columns[-1]] = standard_z_score(filter_extreme_MAD(data[data.columns[-1]], 5))
    data = Industry_neutrality(data,data.columns[-2], data.columns[-1])
    data = data.fillna(0)
    data.to_csv('../preprocess_data/deta3/{}'.format(csv_name),index=False,encoding='GBK')
#对deta4中的数据进行处理，并写入prepocess/deta5中
def preprocess4(csv_name):
    data=pd.read_csv('../preprocess_data/deta5/{}'.format(csv_name),encoding='GBK')
    if csv_name[7:-4]=='StandardDeviationOfDailyReturn120':
        data['StandardDeviationOfDailyReturn120']=standard_z_score(filter_extreme_MAD(data['StandardDeviationOfDailyReturn120'],5))
        data=Industry_neutrality(data,'日期_Date','StandardDeviationOfDailyReturn120')
    if csv_name[7:-4]=='DailyReturnVolatlity20':
        data['DailyReturnVolatlity20']=standard_z_score(filter_extreme_MAD(data['DailyReturnVolatlity20'],5))
        data=Industry_neutrality(data,'日期_Date','DailyReturnVolatlity20')
    if csv_name[7:-4]=='DailyReturnVolatlity60':
        data['DailyReturnVolatlity60']=standard_z_score(filter_extreme_MAD(data['DailyReturnVolatlity60'],5))
        data=Industry_neutrality(data,'日期_Date','DailyReturnVolatlity60')
    if csv_name[7:-4]=='DailyReturnVolatlity120':
        data['DailyReturnVolatlity120']=standard_z_score(filter_extreme_MAD(data['DailyReturnVolatlity120'],5))
        data=Industry_neutrality(data,'日期_Date','DailyReturnVolatlity120')
    data=data.fillna(0)
    print(data)
    data.to_csv('../preprocess_data/deta5/{}'.format(csv_name),index=False,encoding='GBK')
def flatten(a):
    if not isinstance(a, (list, )):
        return [a]
    else:
        b = []
        for item in a:
            b += flatten(item)
    return b

def name(file_path):
    file_name = []
    a = os.listdir(file_path)
    for j in a:
        if os.path.splitext(j)[1] == '.csv':
            file_name.append(j)
    return file_name
#填充na值
def filldata4(csv_name):
    df=pd.read_csv('../preprocess_data/deta5/{}'.format(csv_name),encoding='GBK')
    df1=df.pivot(index=df.columns[2],columns=df.columns[0],values=df.columns[3])
    df1=df1.fillna(method='ffill')
    print(df1)
    df1.to_csv('../preprocess_data/deta/data_{}'.format(csv_name),encoding='GBK')
    df2=df.pivot(index=df.columns[2],columns=df.columns[0],values='mean')
    df2=df2.fillna(method='ffill')
    df2.to_csv('../preprocess_data/deta/mean_{}'.format(csv_name),encoding='GBK')
    df3=df.pivot(index=df.columns[2],columns=df.columns[0],values='std')
    df3=df2.fillna(method='ffill')
    df3.to_csv('../preprocess_data/deta/std_{}'.format(csv_name),encoding='GBK')

def filldata1(csv_name):
    df=pd.read_csv('../preprocess_data/deta2/{}'.format(csv_name),encoding='GBK')
    df1=df.pivot(index=df.columns[1],columns=df.columns[0],values=df.columns[2])
    df1=df1.fillna(method='ffill')
    print(df1)
    df1.to_csv('../preprocess_data/deta/data_{}'.format(csv_name),encoding='GBK')
    df2=df.pivot(index=df.columns[1],columns=df.columns[0],values='mean')
    df2=df2.fillna(method='ffill')
    df2.to_csv('../preprocess_data/deta/mean_{}'.format(csv_name),encoding='GBK')
    df3=df.pivot(index=df.columns[1],columns=df.columns[0],values='std')
    df3=df2.fillna(method='ffill')
    df3.to_csv('../preprocess_data/deta/std_{}'.format(csv_name),encoding='GBK')

def filldata2(csv_name):
    df=pd.read_csv('../preprocess_data/deta3/{}'.format(csv_name),encoding='GBK')
    df1=df.pivot(index=df.columns[1],columns=df.columns[0],values=df.columns[2])
    df1=df1.fillna(method='ffill')
    print(df1)
    df1.to_csv('../preprocess_data/deta/data_{}'.format(csv_name),encoding='GBK')
    try:
        df2=df.pivot(index=df.columns[1],columns=df.columns[0],values='mean')
        df2=df2.fillna(method='ffill')
        df2.to_csv('../preprocess_data/deta/mean_{}'.format(csv_name),encoding='GBK')
        df3=df.pivot(index=df.columns[1],columns=df.columns[0],values='std')
        df3=df2.fillna(method='ffill')
        df3.to_csv('../preprocess_data/deta/std_{}'.format(csv_name),encoding='GBK')
    except:
        return
def changename(csv_name):
    df=pd.read_csv('../preprocess_data/deta/{}'.format(csv_name),encoding='GBK')
    df.columns=pd.Series(df.columns).apply(lambda x:str(x)[1:])
    print(df)
    df.to_csv('../preprocess_data/deta/{}'.format(csv_name),encoding='GBK')
    
#将数据合并到dataframe中，并写入prepocess/deta4文件
#StandardDeviationOfDailyReturn120
StandardDeviationOfDailyReturn120=pd.DataFrame(columns=['上市公司代码_Comcd','最新股票名称_Lstknm','日期_Date','日收益标准差_120日移动平均_Dstd120'])
for i in range(1,14):
    file='../data/deta2/StandardDeviationOfDailyReturn120/{}.csv'.format(i)
    data=pd.read_csv(file,encoding='GBK')
    for i in data.columns:
        if data[i].count() == 0:
            data.drop(labels=i, axis=1, inplace=True)
    StandardDeviationOfDailyReturn120=StandardDeviationOfDailyReturn120.append(data)
StandardDeviationOfDailyReturn120=StandardDeviationOfDailyReturn120.rename(columns={'日收益标准差_120日移动平均_Dstd120':'StandardDeviationOfDailyReturn120'})
StandardDeviationOfDailyReturn120.to_csv('../preprocess_data/deta4/StandardDeviationOfDailyReturn120.csv',index=False,encoding='GBK')
##DailyReturnVolatlity20
DailyReturnVolatlity20=pd.DataFrame(columns=['上市公司代码_Comcd','最新股票名称_Lstknm','日期_Date','波动率_20日简单移动平均()_Sma20'])
for i in range(1,14):
    file='../data/deta3/DailyReturnVolatlity20/{}.csv'.format(i)
    data=pd.read_csv(file)
    for i in data.columns:
        if data[i].count() == 0:
            data.drop(labels=i, axis=1, inplace=True)
    DailyReturnVolatlity20=DailyReturnVolatlity20.append(data)
DailyReturnVolatlity20=DailyReturnVolatlity20.rename(columns={'波动率_20日简单移动平均()_Sma20':'DailyReturnVolatlity20'})
DailyReturnVolatlity20.to_csv('../preprocess_data/deta4/DailyReturnVolatlity20.csv',index=False,encoding='GBK')

##DailyReturnVolatlity60
DailyReturnVolatlity60=pd.DataFrame(columns=['上市公司代码_Comcd','最新股票名称_Lstknm','日期_Date','波动率_60日简单移动平均()_Sma60'])
for i in range(1,14):
    file='../data/deta3/DailyReturnVolatlity60/{}.csv'.format(i)
    data=pd.read_csv(file)
    for i in data.columns:
        if data[i].count() == 0:
            data.drop(labels=i, axis=1, inplace=True)
    DailyReturnVolatlity60=DailyReturnVolatlity60.append(data)
DailyReturnVolatlity60=DailyReturnVolatlity60.rename(columns={'波动率_60日简单移动平均()_Sma60':'DailyReturnVolatlity60'})
DailyReturnVolatlity60.to_csv('../preprocess_data/deta4/DailyReturnVolatlity60.csv',index=False,encoding='GBK')
##DailyReturnVolatlity120
DailyReturnVolatlity120=pd.DataFrame(columns=['上市公司代码_Comcd','最新股票名称_Lstknm','日期_Date','波动率_120日简单移动平均()_Sma120'])
for i in range(1,14):
    file='../data/deta3/DailyReturnVolatlity120/{}.csv'.format(i)
    data=pd.read_csv(file)
    for i in data.columns:
        if data[i].count() == 0:
            data.drop(labels=i, axis=1, inplace=True)
    DailyReturnVolatlity120=DailyReturnVolatlity120.append(data)
DailyReturnVolatlity120=DailyReturnVolatlity120.rename(columns={'波动率_120日简单移动平均()_Sma120':'DailyReturnVolatlity120'})
DailyReturnVolatlity120.to_csv('../preprocess_data/deta4/DailyReturnVolatlity120.csv',index=False,encoding='GBK')
#数据筛选
print('数据筛选:\n')
file_name=name(r'../data/deta2')
print(file_name)
for i in file_name:
   print(i)
   drop_films1(i)
file_name=name(r'../data/deta3')
print(file_name)
for i in file_name:
   print(i)
   drop_films2(i)
file_name=name(r'../preprocess_data/deta4')
print(file_name)
for i in file_name:
   drop_films4(i)
#数据处理
print('数据处理：\n')
file_name=name(r'../preprocess_data/deta5')
for i in file_name:
    print(i)
    preprocess4(i)
    filldata4(i)
file_name=name(r'../preprocess_data/deta6')
for i in file_name:
    print(i)
    preprocess1(i)
file_name=name(r'../preprocess_data/deta7')
for i in file_name:
    print(i)
    preprocess2(i)
print('数据填充：\n')
#数据填充
file_name=name(r'../preprocess_data/deta2')
for i in file_name:
    print(i)
    filldata1(i)
file_name=name(r'../preprocess_data/deta3')
for i in file_name:
    print(i)
    filldata2(i)
file_name=name(r'../preprocess_data/deta')
for i in file_name:
    changename(i)
    





