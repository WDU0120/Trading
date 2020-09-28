# -*- coding: utf-8 -*-
"""
Created on Tue Sep 22 21:26:02 2020

@author: Lenovo
"""

import pandas as pd
import numpy as np
import os

wd = r"E:\Education\Education Material\09.22 Quant\数据预处理\Data"

os.chdir(wd)

nxtrtn = pd.read_csv("next_rtn.csv")

stockname = nxtrtn.columns[1:]

dates = nxtrtn["Date"]

CashRet = pd.read_csv("经营活动产生现金流增长率.csv")

CashRet["Date"] == dates

all(CashRet.columns[1:]==nxtrtn.columns[1:])

#################################################################################


def anal_ICs(ret,factorname):
    ICs = []
    
    factor = pd.read_csv(factorname+".csv")
    
    if all(factor["Date"] == dates) & all(factor.columns[1:] == stockname):
        print("Condition is satisfied!")
    else:
        print("Condition is not satisfied!")
    for i in range(len(dates)):
        cur_bool1 = pd.notna(ret.iloc[i,:])
        cur_bool2 = pd.notna(factor.iloc[i,:])
        cur_bool = cur_bool1 & cur_bool2
        if sum(cur_bool) > 0:
            tempret = ret.iloc[i,1:][cur_bool].astype(float)
            tempfactor = factor.iloc[i,1:][cur_bool].astype(np.float)
            ICs.append(np.corrcoef(tempfactor,tempret)[0,1])
    
    ICs = pd.Series(ICs)
    IC_mean = np.mean(ICs)
    IC_abs_mean = np.mean(abs(ICs))
    IC_std = np.std(ICs)
    IC_greater_zero = (sum(ICs > 0)/len(ICs) - 0.5)
    IC_t = IC_mean/IC_std * np.sqrt(len(ICs) - 1)
    
    res = {"ICs":ICs,"IC_mean":IC_mean,"IC_abs_mean":IC_abs_mean,"IC_std":IC_std,\
            "IC_greater_zero":IC_greater_zero,"IC_t":IC_t}
    dfres = pd.DataFrame(res)
    dfres.iloc[1:,1:] = np.nan
    dfres.index = dates
    dfres.to_excel(factorname+'.xlsx')
    
    
    return dfres


#def anal_rankICs(data_df,factor_name,rtn_name,date_name):
#    ICs = []
#    dates = np.unique(data_df[date_name])
#    dates = sorted(dates)
#    for date in dates: 
#        cur_df = data_df[data_df.date == date]
#        cur_bool1 = pd.notna(cur_df[factor_name])
#        cur_bool2 = pd.notna(cur_df[rtn_name])
#        cur_bool = cur_bool1 & cur_bool2
#        if sum(cur_bool) > 0:
#            cur_df = cur_df[cur_bool]
#            s1 = get_rank(cur_df[factor_name])
#            s2 = get_rank(cur_df[rtn_name])
#            ICs.append(np.corrcoef(s1,s2)[0,1])
#    
#    ICs = pd.Series(ICs)
#    IC_mean = np.mean(ICs)
#    IC_abs_mean = np.mean(abs(ICs))
#    IC_std = np.std(ICs)
#    IC_greater_zero = (sum(ICs > 0)/len(ICs) - 0.5)
#    IC_t = IC_mean/IC_std * np.sqrt(len(ICs))
#    
#    return {"rankICs":ICs,"rankIC_mean":IC_mean,"rankIC_abs_mean":IC_abs_mean,"rankIC_std":IC_std,\
#            "rankIC_greater_zero":IC_greater_zero,"rankIC_t":IC_t}
#    

anal_ICs(nxtrtn,"经营活动产生现金流增长率")



factor_list = ['净利润同比增长率',
 '净利率',
 '现金比率',
 '经营活动产生现金流增长率',
 '经营活动产生现金流增长率',
 '营业利润同比增长率',
 '营业收入历史增长率',
 '营业收入同比增长',
 '营业收入预期短期增长率',
 '营业收入预期长期增长率',
 '融资活动产生现金流增长率']

dfFactor = pd.DataFrame(dates)
dfFactor.index = dates

dfPerformance = pd.DataFrame(columns=["IC_mean","IC_abs_mean","IC_std",\
            "IC_greater_zero","IC_t"])

for factorname in factor_list:
    tempdf = anal_ICs(nxtrtn,factorname)
    dfFactor[factorname]=pd.Series(tempdf.ICs)

    dfPerformance=dfPerformance.append(tempdf.iloc[0,1:])


dfPerformance.index=factor_list

dfFactor = dfFactor.drop(columns="Date")
dfFactor.to_excel("dfFactor.xlsx")
dfPerformance.to_excel("dfPerformance.xlsx")
    