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

# 获得每日交易成本和每日股票买卖点
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
        difference.loc[:, 'time'] = name
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


def get_portfolio(all_tradedate_position, trade_detail, daily_cost, all_trading_data, benchmark, hedgemethod, margin,
                  tradecost):
    # 取得参数的deepcopy 防止污染参数数据
    all_tradedate_position = all_tradedate_position.copy()
    all_trading_data = all_trading_data.copy()
    trade_detail = trade_detail.copy()
    daily_cost = daily_cost.copy()

    # 处理daily_cost
    daily_cost.loc[:, "daily_cost"] = daily_cost["cost"] * tradecost

    # 计算每日每只股票的收益率
    # 交易日实际持仓表和行情表进行merge,得到带行情的持仓表
    all_tradedate_position = pd.merge(all_tradedate_position, all_trading_data, on=['time', 'stkcd'])
    # 带行情的持仓表和trade_detail表进行merge
    all_tradedate_position = pd.merge(all_tradedate_position, trade_detail, on=["time", "stkcd"], how="left")
    # 股票交易状态为四种：增仓、新增、减仓、踢出和不变

    # direction等于True的，即今日进行增仓或新增的股票。
    # 增加的仓位使用close-open，保留的仓位使用close-preclose
    all_tradedate_position.loc[all_tradedate_position.direction == True, "return"] = \
        (np.log(all_tradedate_position.loc[:, "closep"]) - np.log(all_tradedate_position.loc[:, "preclosep"])) \
        * (all_tradedate_position.loc[:, "weight"] - all_tradedate_position.loc[:, "delta_weight"]) + \
        (np.log(all_tradedate_position.loc[:, "closep"]) - np.log(all_tradedate_position.loc[:, "openp"])) \
        * (all_tradedate_position.loc[:, "delta_weight"])
    # direction等于False的，即今日进行减仓的股票。
    # 减仓部分使用open-preclose，保留的部分使用close-preclose
    all_tradedate_position.loc[all_tradedate_position.direction == False, "return"] = \
        (np.log(all_tradedate_position.loc[:, "closep"]) - np.log(all_tradedate_position.loc[:, "preclosep"])) \
        * (all_tradedate_position.loc[:, "weight"]) + \
        (np.log(all_tradedate_position.loc[:, "openp"]) - np.log(all_tradedate_position.loc[:, "preclosep"])) \
        * (all_tradedate_position.loc[:, "delta_weight"])
    # direction等于NA，即今日股票未交易。使用close-preclose
    all_tradedate_position.loc[
        (all_tradedate_position.direction != True) & (all_tradedate_position.direction != False), "return"] = \
        (np.log(all_tradedate_position.loc[:, "closep"]) - np.log(all_tradedate_position.loc[:, "preclosep"])) \
        * (all_tradedate_position.loc[:, "weight"])

    # 和dailycost合并，汇总一次daily_return,作为all_tradedate_position_summary。再单独处理踢出的股票
    all_tradedate_position_summary = pd.merge(all_tradedate_position, daily_cost, on=['time']).loc[:,
                                     ["time", "return", "daily_cost"]]

    def sum_all(df):
        df = df.copy()
        # 汇总当日return并减去当日的交易成本
        df.loc[:, 'value'] = np.sum(df.loc[:, "return"]) - df.loc[:, "daily_cost"].iloc[0]
        return df.head(1)

    daily_return = all_tradedate_position_summary.groupby('time').apply(sum_all).reset_index(drop=True).loc[:,
                   ["time", "value"]]
    # 在trade_detail里的direction等于false，但股票不在对应交易日的all_tradedate_position里，即今日踢出的股票
    # 使用open-preclose
    # 将trade_detail和all_trading_data进行合并
    trade_detail = pd.merge(trade_detail, all_trading_data, on=["time", "stkcd"], how="left")
    for time, group in trade_detail.groupby("time"):
        group = group.copy()
        # 对应time的当日持仓表all_tradedate_position
        time_stk_list = list(all_tradedate_position.loc[all_tradedate_position.time == time].stkcd)
        # 关心的标的
        ind = (group.direction == False) & (-group.stkcd.isin(time_stk_list))
        group.loc[ind, "return"] = (np.log(group.loc[:, "openp"]) - np.log(group.loc[:, "preclosep"])) \
                                   * (group.loc[:, "delta_weight"])
        # 修改daily_return表即可,增加当日踢出股票带来的收益
        daily_return.loc[daily_return.time == time, "value"] = daily_return.loc[daily_return.time == time, "value"] + \
                                                               group.loc[ind, "return"].sum()

    # # 计算得的股票在每天的收益（可能使用其他的计算方法）
    # all_tradedate_position.loc[:, "return"] = (np.log(all_tradedate_position.loc[:, "closep"]) - np.log(
    #     all_tradedate_position.loc[:, "preclosep"])) * all_tradedate_position.loc[:, "weight"]
    # all_tradedate_position = all_tradedate_position.loc[:, ["time", "return"]]
    #
    # # 得到每日净值变化
    #
    # def sum_all(df):
    #     df = df.copy()
    #     df.loc[:, 'value'] = np.sum(df.loc[:, "return"])
    #     return df.head(1)
    #
    # daily_return = all_tradedate_position.groupby('time').apply(sum_all).reset_index(drop=True).loc[:,
    #                ["time", "value"]]

    # 对冲
    # todo:是否按照当日实际仓位对冲
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
# 计算最大回撤率
def get_maxdrawdown(portfolio):
    max_return = np.fmax.accumulate(portfolio)
    return np.nanmin((portfolio - max_return) / max_return)

# 输入每日交易盈亏，返回年化收益率
def get_annual_return(portfolio):
    return (1 + np.mean(portfolio, axis=0)) ** 250 - 1

# 输入每日交易盈亏，返回年化波动率
def get_volatility(final):
    return np.std(final, axis=0) * np.sqrt(250)


# # 输入每日交易盈亏和无风险利率，返回夏普比率
# def get_sharperatio(annual_return, annual_volatility, r):
#     return (annual_return - r) / annual_volatility
#
#

#
#

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
