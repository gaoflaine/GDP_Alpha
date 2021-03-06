# -*- coding:utf-8 -*-
# Copyright GDP Group
# Created by songzhichen and ruchuang gao on July 8 2017

import pandas as pd
import numpy as np
import empyrical as em


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
    all_tradedate_position = pd.merge(all_tradedate_position, trade_detail, on=["time", "stkcd"], how="outer")
    # 股票交易状态为四种：增仓、减仓、踢出和不变

    # direction等于True的，即今日进行增仓或新增的股票。
    # 增加的仓位使用close-open，保留的仓位使用close-preclose
    all_tradedate_position.loc[all_tradedate_position.direction == True, "return"] = \
        (np.log(all_tradedate_position.loc[:, "closep"]) - np.log(all_tradedate_position.loc[:, "preclosep"])) \
        * (all_tradedate_position.loc[:, "weight"] - all_tradedate_position.loc[:, "delta_weight"]) + \
        (np.log(all_tradedate_position.loc[:, "closep"]) - np.log(all_tradedate_position.loc[:, "openp"])) \
        * (all_tradedate_position.loc[:, "delta_weight"])

    # direction等于False同时今日有行情，即今日进行减仓的股票。
    # 减仓部分使用open-preclose，保留的部分使用close-preclose
    all_tradedate_position.loc[
        (all_tradedate_position.direction == False) & (-all_tradedate_position.closep.isnull()), "return"] = \
        (np.log(all_tradedate_position.loc[:, "closep"]) - np.log(all_tradedate_position.loc[:, "preclosep"])) \
        * (all_tradedate_position.loc[:, "weight"]) + \
        (np.log(all_tradedate_position.loc[:, "openp"]) - np.log(all_tradedate_position.loc[:, "preclosep"])) \
        * (all_tradedate_position.loc[:, "delta_weight"])

    # direction等于False同时今日无行情，即今日进行踢出的股票。
    # 踢出的部分使用openp-preclose
    # 由于被踢出的股票，没有行情数据，表all_tradedate_position_tickout加入了踢出股票的行情数据
    all_tradedate_position_tickout = all_tradedate_position.loc[:, ["time", "stkcd", "delta_weight"]]
    all_tradedate_position_tickout = pd.merge(all_tradedate_position_tickout, all_trading_data, on=["time", "stkcd"])
    # all_tradedate_position_tickout和all_tradedate_position只在行情数据上不一致
    all_tradedate_position.loc[
        (all_tradedate_position.direction == False) & (all_tradedate_position.closep.isnull()), "return"] = \
        (np.log(all_tradedate_position_tickout.loc[:, "openp"]) - np.log(
            all_tradedate_position_tickout.loc[:, "preclosep"])) \
        * (all_tradedate_position_tickout.loc[:, "delta_weight"])

    # direction等于NA，即今日股票未交易。使用close-preclose
    all_tradedate_position.loc[-all_tradedate_position.direction.isin([True, False]), "return"] = \
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
    # # 在trade_detail里的direction等于false，但股票不在对应交易日的all_tradedate_position里，即今日踢出的股票
    # # 使用open-preclose
    # # 将trade_detail和all_trading_data进行合并
    # trade_detail = pd.merge(trade_detail, all_trading_data, on=["time", "stkcd"], how="left")
    # for time, group in trade_detail.groupby("time"):
    #     group = group.copy()
    #     # 对应time的当日持仓表all_tradedate_position
    #     time_stk_list = list(all_tradedate_position.loc[all_tradedate_position.time == time].stkcd)
    #     # 关心的标的
    #     ind = (group.direction == False) & (-group.stkcd.isin(time_stk_list))
    #     group.loc[ind, "return"] = (np.log(group.loc[:, "openp"]) - np.log(group.loc[:, "preclosep"])) \
    #                                * (group.loc[:, "delta_weight"])
    #     # 修改daily_return表即可,增加当日踢出股票带来的收益
    #     daily_return.loc[daily_return.time == time, "value"] = daily_return.loc[daily_return.time == time, "value"] + \
    #                                                            group.loc[ind, "return"].sum()

    # 计算基准收益
    benchmark_return = all_trading_data[all_trading_data.stkcd == benchmark]
    benchmark_return.loc[:, 'return'] = np.log(benchmark_return.closep) - np.log(benchmark_return.preclosep)
    benchmark_return = benchmark_return.loc[:, ["time", "return"]]

    # 对冲
    # todo:是否按照当日实际仓位对冲
    if hedgemethod == 0:
        daily_return_hedged = daily_return
        daily_return_hedged.columns = ["time", "value_hedged"]
        benchmark_return = benchmark_return.loc[benchmark_return.time.isin(daily_return_hedged.time), :]

    if hedgemethod == 1:
        daily_return_hedged = pd.merge(daily_return, benchmark_return, on='time')
        daily_return_hedged.loc[:, 'value_hedged'] = daily_return_hedged['value'] - daily_return_hedged['return']
        daily_return_hedged = daily_return_hedged.loc[:, ['time', 'value_hedged']]

    # 得到最终的daily_return表现
    daily_return_hedged = daily_return_hedged.set_index("time")

    # 得到基准的daily_return表现
    benchmark_return = benchmark_return.set_index("time")

    # 得到最终的累计净值表现
    portfolio = em.stats.cum_returns(daily_return_hedged, 1)

    return portfolio, daily_return_hedged, benchmark_return


def get_indicator(daily_return_hedged, benchmark_return, risk_free_rate, hedgemethod, cutoff):
    # 取得参数的deepcopy 防止污染参数数据 同时转化为series
    daily_return_hedged = daily_return_hedged.value_hedged.copy()
    benchmark_return = benchmark_return.loc[:, "return"].copy()

    # 计算组合净值表现
    portfolio = em.stats.cum_returns(daily_return_hedged, 1)
    # 计算组合最大回撤
    max_drawdown = em.stats.max_drawdown(daily_return_hedged)
    # 计算组合年化收益率
    annual_return = em.stats.annual_return(daily_return_hedged)
    # 计算组合年化波动率
    annual_volatility = em.stats.annual_volatility(daily_return_hedged)
    # 计算组合calmar比率
    calmar_ratio = em.stats.calmar_ratio(daily_return_hedged)
    # 计算组合omega比率
    #
    # 计算组合sharpe比率
    sharpe_ratio = em.stats.sharpe_ratio(daily_return_hedged, risk_free=risk_free_rate)
    # 计算组合sortino比率
    #
    # 计算组合downside_risk
    #
    # 计算组合tail_ratio
    tail_ratio = em.stats.tail_ratio(daily_return_hedged)
    # 计算组合VaR和CVaR
    VaR = em.stats.value_at_risk(daily_return_hedged, cutoff)
    CVaR = em.stats.conditional_value_at_risk(daily_return_hedged, cutoff)

    # 计算组合超额sharpe比率、alpha\beta值和capture比率
    if hedgemethod == 0:
        # 超额夏普比率
        excess_sharpe_ratio = em.stats.excess_sharpe(daily_return_hedged, benchmark_return)
        # 组合alpha和beta值
        alpha, beta = em.stats.alpha_beta(daily_return_hedged, benchmark_return, risk_free=risk_free_rate)
        up_alpha, up_beta = em.stats.up_alpha_beta(daily_return_hedged, benchmark_return, risk_free=risk_free_rate)
        down_alpha, down_beta = em.stats.down_alpha_beta(daily_return_hedged, benchmark_return,
                                                         risk_free=risk_free_rate)
        # capture
        capture = em.stats.capture(daily_return_hedged, benchmark_return)
        # up\down capture
        up_capture = em.stats.up_capture(daily_return_hedged, benchmark_return)
        down_capture = em.stats.down_capture(daily_return_hedged, benchmark_return)

# # 计算最大回撤率
# def get_maxdrawdown(portfolio):
#     max_return = np.fmax.accumulate(portfolio)
#     return np.nanmin((portfolio - max_return) / max_return)
#
# # 输入每日交易盈亏，返回年化收益率
# def get_annual_return(portfolio):
#     return (1 + np.mean(portfolio, axis=0)) ** 250 - 1
#
# # 输入每日交易盈亏，返回年化波动率
# def get_volatility(final):
#     return np.std(final, axis=0) * np.sqrt(250)

# # 输入每日交易盈亏和无风险利率，返回夏普比率
# def get_sharperatio(annual_return, annual_volatility, r):
#     return (annual_return - r) / annual_volatility
#
# # 组合beta
# def get_alpha_beta(final, base_return):
#     slope, intercept, r_value, p_value, slope_std_error \
#         = stats.linregress(final, base_return)
#     return intercept, slope
#
# # 输入组合每日日交易盈亏和基准每日交易盈亏，返回信息比率
# def get_IR(final, base_return):
#     alpha, beta = get_alpha_beta(final, base_return)
#     theta = final - beta * base_return
#     return ((1 + np.mean(theta, axis=0)) ** 250 - 1) / (np.std(theta, axis=0) * np.sqrt(250))
