# -*- coding:utf-8 -*-
# Copyright GDP Group
# Created by songzhichen and ruchuang gao on July 8 2017

# 输入持仓表和行情表，返回每日净值

# import pickle
# import sqlite3
# conn = sqlite3.connect('F:\project_gdp\GD.db')
import pandas as pd
import numpy as np


# 输入最终每日实际持仓表（考虑涨跌停等限制后），返回策略的净值表现
# 增加交易成本的考虑
# def get_portfolio(position, price):
#     position = pd.merge(position, price, on=['time', 'stkcd'])
#     # 得到每日的收益状况
#     daily_return = pd.DataFrame()
#     i = 0
#     for name, group in position.groupby('time'):
#         daily_return.loc[i, 'time'] = name
#         daily_return.loc[i, 'value'] = (group.loc[:, 'weight'] * np.log(group.loc[:, 'closep'])).sum() - \
#                                        (group.loc[:, 'weight'] * np.log(group.loc[:, 'preclosep'])).sum()
#         i += 1
#     # 得到净值表现
#     portfolio = daily_return.copy()
#     portfolio.loc[:, "value"] = (daily_return.loc[:, 'value'] + 1).cumprod()
#     return daily_return, portfolio

def get_cost(all_tradedate_position):
    j = 0
    trade_detail = pd.DataFrame(columns=['time', 'stkcd', 'delta_weight', 'direction'])
    yesterday_position = pd.DataFrame(columns=['time', 'stkcd', 'weight'])
    cost = pd.DataFrame()
    for name, group in all_tradedate_position.groupby("time"):
        today_position = group.copy()
        union = pd.merge(today_position, yesterday_position, how='outer', on='stkcd',
                         suffixes=['', '_yesterday'])
        difference = union[union.weight != union.weight_yesterday].fillna(0)
        difference.loc[:, 'direction'] = (difference.weight > difference.weight_yesterday)
        difference.loc[:, 'delta_weight'] = abs(difference.loc[:, 'weight'] - difference.loc[:, 'weight_yesterday'])
        trade_detail = trade_detail.append(difference.loc[:, ['time', 'stkcd', 'delta_weight', 'direction']])
        if len(difference) == 0:
            cost.loc[j, 'time'] = name
            cost.loc[j, 'cost'] = 0
        else:
            cost.loc[j, 'time'] = name
            cost.loc[j, 'cost'] = abs(difference.loc[:, 'weight'] - difference.loc[:, 'weight_yesterday']).sum()
        j += 1
        yesterday_position = today_position.copy()
    return trade_detail, cost



def get_portfolio(all_tradedate_position, all_trading_data, benchmark, hedgemethod, margin):
    # 取得参数的deepcopy 防止污染参数数据
    all_tradedate_position = all_tradedate_position.copy()
    all_trading_data = all_trading_data.copy()

    # 两张表merge
    all_tradedate_position = pd.merge(all_tradedate_position, all_trading_data, on=['time', 'stkcd'])
    # 计算得的股票在每天的收益（可能使用其他的计算方法）
    all_tradedate_position.loc[:, "return"] = (np.log(all_tradedate_position.loc[:, "closep"]) - np.log(
        all_tradedate_position.loc[:, "preclosep"])) * all_tradedate_position.loc[:, "weight"]
    all_tradedate_position = all_tradedate_position.loc[:, ["time", "return"]]

    # 得到每日净值变化
    def sum_all(df):
        df = df.copy()
        df.loc[:, 'value'] = np.sum(df.loc[:, "return"])
        return df.head(1)

    daily_return = all_tradedate_position.groupby('time').apply(sum_all).reset_index(drop=True).loc[:,
                   ["time", "value"]]

    # 对冲
    if hedgemethod == 1:
        benchmark_return = all_trading_data[all_trading_data.stkcd == benchmark]
        benchmark_return.loc[:, 'return'] = np.log(benchmark_return.closep) - np.log(benchmark_return.preclosep)
        daily_return_hedged = pd.merge(daily_return, benchmark_return, on='time')
        daily_return_hedged.loc[:, 'value_hedged'] = daily_return_hedged['value'] - daily_return_hedged['return']
        daily_return_hedged = daily_return_hedged.loc[:, ['time', 'value_hedged']]

    # 得到净值表现
    portfolio = daily_return_hedged.copy()
    portfolio.loc[:, "value_hedged"] = np.cumprod((daily_return_hedged.loc[:, 'value_hedged'] + 1))
    return portfolio

# def get_transaction(indicator_matrix):
#     pass
#
#
# # 输入净值矩阵，返回最大回撤
# def get_maxdrawdown(value):
#     get_max = value.expanding().max()
#     drawdown = (get_max - value) * 1.0 / get_max
#     return drawdown.max()


# # 输入每日交易盈亏和无风险利率，返回夏普比率
# def get_sharperatio(annual_return, annual_volatility, r):
#     return (annual_return - r) / annual_volatility
#
#
# # 输入每日交易盈亏，返回波动率
# def get_volatility(final):
#     return np.std(final, axis=0) * np.sqrt(250)
#
#
# # 输入每日交易盈亏，返回年化收益率
# def get_annual_return(final):
#     return (1 + np.mean(final, axis=0)) ** 250 - 1
#
#
# # 组合beta
# def get_alpha_beta(final, base_return):
#     slope, intercept, r_value, p_value, slope_std_error \
#         = stats.linregress(final, base_return)
#     return intercept, slope
#
#
# # 输入组合每日日交易盈亏和基准每日交易盈亏，返回信息比率
# def get_IR(final, base_return):
#     alpha, beta = get_alpha_beta(final, base_return)
#     theta = final - beta * base_return
#     return ((1 + np.mean(theta, axis=0)) ** 250 - 1) / (np.std(theta, axis=0) * np.sqrt(250))
