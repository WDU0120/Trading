# -*- coding: utf-8 -*-
"""
Created on Sat Sep 26 19:31:58 2020

@author: Cyc
"""

import numpy as np
import pandas as pd
import os
import seaborn as sns
import matplotlib.pyplot as plt

df_ic = pd.read_excel('./Data/dfFactor.xlsx',index_col =0)

from pylab import mpl
mpl.rcParams['font.sans-serif'] = ['SimHei'] 
mpl.rcParams['axes.unicode_minus'] = False 

dfData = df_ic.corr()
plt.subplots(figsize=(9, 9)) # 设置画面大小
sns.heatmap(dfData, annot=True, vmax=1, square=True, cmap="Blues")

df_per = pd.read_excel('./Data/dfPerformance.xlsx')

df_per

alpha = 0.068278/(0.068278+0.067585)
df1 = pd.read_csv('./Data/净利润同比增长率.csv',index_col=0)
df2= pd.read_csv('./Data/营业利润同比增长率.csv',index_col=0)

assert len(df1.columns)== len(df2.columns)
df = pd.DataFrame(index = df1.index)

df1.columns[0]

def bind(df1,df2,alpha):
    for i in df1.columns:
        df[i] = pd.Series(alpha*df1[i].values+(1-alpha)*df2[i].values,index= df1.index)
    return df
df =bind(df1,df2,alpha)

df.to_csv('营业利润+净利润.csv')