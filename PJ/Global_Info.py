# -*- coding:utf-8 -*-
# Copyright GDP Group
# Created by songzhichen and ruchuang gao on July 8 2017

# Global Info

from Function import factor_weight
from Function import position_method


class Global_Info:
    # 基本参数
    start = '2008-01-02'
    end = '2017-04-26'
    benchmark = "000300"  # “000300”,"000906","000905","000016"
    universe = 'HS300'  # “HS300”,"ZZ800","ZZ500","SZ50" None:A股（部分功能不可使用，待维护#todo:维护A股功能）
    freq = 'M'  # "M","Q"（D：日频率，不推荐使用，部分功能不可使用，待维护#todo：维护季度频率功能）
    weightType = 1  # 因子打分权重算法（0是用户主动输入方法，1是普通加权，2是主成分分析，3是回归）
    n = 10  # 调仓日选出的股票总只数
    industry_neutral = 0  # 1：选股维持和标的指数的行业市值中性(采用中信行业一级分类），0：不维持行业市值中性，正常模式（
    positionType = 1  # 资产组合构成算法，1是普通加权，2是30天股价波动率加权， 3是市值加权
    backperiod = 20  # 因子权重计算过程中，使用多少过去的数据来计算调仓日的因子打分权重
    hedgemethod = 0  # 对冲的模式，0不进行对冲，1是指数简单对冲（不考虑保证金，只计算超额收益），2是使用标的的期货品种进行对冲，保证金为margin
    margin = 0.1  # 对冲标的的保证金比率
    tradecost=0.0001 # 股票交易成本

    # score_weight
    factor_weight = {1: factor_weight.equal_weight, 2: factor_weight.pca, 3: factor_weight.regression}
    # position weight
    position_weight = {1: position_method.equal_weight, 2: position_method.vol_weight, 3: position_method.ev_weight}
