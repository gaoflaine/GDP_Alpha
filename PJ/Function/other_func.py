# -*- coding:utf-8 -*-
# Copyright GDP Group
# Created by songzhichen and ruchuang gao on July 8 2017

# other useful function

from datetime import datetime as dt
import pandas as pd


# 根据月份返回季度
def quater(month):
    if month in ["01", "02", "03"]:
        return 1
    elif month in ["04", "05", "06"]:
        return 2
    elif month in ["07", "08", "09"]:
        return 3
    elif month in ["10", "11", "12"]:
        return 4


# 得到数据库内所有的交易日序列
def get_all_tradedate(conn):
    strSQL = "select distinct time from PE"
    return pd.read_sql_query(strSQL, conn)


# 根据起始日期，频率，从数据库中获取进行建仓调仓的日期序列
def tradeday(start, end, freq, all_tradingday):
    # 取得参数的deepcopy 防止污染参数数据
    all_tradingday = all_tradingday.copy()

    all_tradingday = all_tradingday.query("time  >='{}' and time <='{}'".format(start, end))
    all_tradingday.loc[:, "year"] = [i[:4] for i in all_tradingday.loc[:, "time"]]
    all_tradingday.loc[:, "month"] = [i[5:7] for i in all_tradingday.loc[:, "time"]]
    all_tradingday.loc[:, "day"] = [i[8:] for i in all_tradingday.loc[:, "time"]]
    # 根据freq，输出调仓日
    if freq == "M":
        result = pd.DataFrame()
        for (name1, name2), group in all_tradingday.groupby(["year", "month"]):
            result = result.append(group.head(1))
        return result.drop(["day", "month", "year"], axis=1)
    elif freq == "Q":
        # 得到每年4个quanter的月份
        result = pd.DataFrame()
        all_tradingday.loc[:, "quarter"] = [quater(i) for i in all_tradingday.loc[:, "month"]]
        for (name1, name2), group in all_tradingday.groupby(["year", "quarter"]):
            result = result.append(group.head(1))
        return result.drop(["day", "month", "year", "quarter"], axis=1)
    else:
        return all_tradingday.drop(["day", "month", "year"], axis=1)


# 根据开始日期和回归时间，返回数据获取的开始时间
def back_date(start, back_period, all_tradingday):
    # 取得参数的deepcopy 防止污染参数数据
    all_tradingday = all_tradingday.copy()

    strSQL = "select distinct time from PE"
    daylist = list(all_tradingday.time)
    return daylist[daylist.index(start) - back_period]


def get_id():
    date = dt.today()
    id = str(date.year) + str(date.month) + str(date.day) + str(date.hour) + str(date.minute) + str(date.second)
    return id


def get_tradefactor(all_factor_data, change_position_day):
    # 取得参数的deepcopy 防止污染参数数据
    all_factor_data = all_factor_data.copy()
    change_position_day = change_position_day.copy()

    return pd.merge(change_position_day, all_factor_data, on="time", how="left")


def revise_start(start, all_tradingday):
    # 取得参数的deepcopy 防止污染参数数据
    all_tradingday = all_tradingday.copy()

    while True:
        if start in all_tradingday:
            return start
        else:
            start = str(pd.date_range(start=start, periods=2, freq='D')[-1])[:10]


def revise_end(end, all_tradingday):
    # 取得参数的deepcopy 防止污染参数数据
    all_tradingday = all_tradingday.copy()
    while True:
        if end in all_tradingday:
            return end
        else:
            end = str(pd.date_range(end=end, periods=2, freq='D')[0])[:10]


# 输入调仓日的position表和调仓日序列、全部交易日序列，返回全部交易日的持仓表
# resample错误
# def position_extension(position, all_tradedate, freq):
#     if freq == "D":
#         return position
#     else:
#         position.time = [dt.strptime(i, '%Y-%m-%d') for i in position.time]
#         position = position.set_index("time")
#         result = pd.DataFrame()
#         for name, group in position.groupby("stkcd"):
#             result = result.append(group.resample('D').ffill())
#         result = result.reset_index()
#         result.time = [dt.strftime(i, '%Y-%m-%d') for i in result.time]
#         return result[result.time.isin(all_tradedate.time)]

# def position_extension(CPD_position, all_trading_data, freq, trade_status):
#     # 取得参数的deepcopy 防止污染参数数据
#     CPD_position = CPD_position.copy()
#     all_trading_data = all_trading_data.copy()
#
#     if freq == 'D':
#         return CPD_position
#     if freq == 'M':
#         # CPD_position 的time格式是timestamp
#         CPD_position.loc[:, 'YYMM'] = [i[:4] + i[5:7] for i in CPD_position.time]
#         # all_trading_date 的time格式是字符串
#         all_trading_data.loc[:, 'YYMM'] = [i[:4] + i[5:7] for i in all_trading_data.time]
#         result = pd.merge(all_trading_data, CPD_position, how='inner',
#                           on=['stkcd', 'YYMM'], suffixes=['', '_huancang'])
#         return result[['time', 'stkcd', "weight"]]



# 在position_extension里得到no_space_tobuy
def get_no_space_tobuy(weight_left, buy_available, no_space_tobuy):
    '''
        具体算法为以下这个函数，
        对buy_available逐行检查，若该行的weight小于weight_left，则可以买入，直接检查下一行
        若该行的weight大于weight_left,则把该股票记入no_space_tobuy里，再检查下一行
    '''
    if len(buy_available) == 1:
        if round(buy_available['weight'].iloc[0], 6) > round(weight_left, 6):
            no_space_tobuy.append(buy_available['stkcd'].iloc[0])
        return None
    if buy_available['weight'].iloc[0] < weight_left:
        return get_no_space_tobuy(weight_left - buy_available['weight'].iloc[0],
                                  buy_available.iloc[1:, :], no_space_tobuy)
    else:
        no_space_tobuy.append(buy_available['stkcd'].iloc[0])
        return get_no_space_tobuy(weight_left, buy_available.iloc[1:, :], no_space_tobuy)


def position_extension(CPD_position, all_trading_data,
                       all_tradingday, change_position_day, freq,
                       trade_status, trade_limit):
    # 取得参数的deepcopy 防止污染参数数据
    CPD_position = CPD_position.copy()
    all_trading_data = all_trading_data.copy()
    all_tradingday = all_tradingday.copy()
    change_position_day = change_position_day.copy()
    trade_status = trade_status.copy()
    trade_limit = trade_limit.copy()

    # 把停牌和涨跌停合并到一张表里
    status_limit = pd.merge(trade_status, trade_limit, on=['time', 'stkcd'])

    # 找到每个持仓周期的时间list
    change_position_day = list(change_position_day.time)
    all_tradingday = list(all_tradingday.time)
    position_period = []
    for i in range(0, len(change_position_day) - 1):
        starttime = change_position_day[i]
        endtime = change_position_day[i + 1]
        position_period.append(
            all_tradingday[all_tradingday.index(starttime):all_tradingday.index(endtime)])
    position_period.append(
        all_tradingday[all_tradingday.index(endtime): all_tradingday.index(all_trading_data.time.max()) + 1])

    # 循环前的准备变量
    yesterday = 0
    yesterday_position = pd.DataFrame(columns=['time', 'stkcd', 'score', 'weight'])
    final_result = pd.DataFrame()
    for (name, group), period in zip(CPD_position.groupby('time'), position_period):
        # copy数据，防止数据污染
        group = group.copy()
        for today in period:
            # if today == '2011-04-01':
            #     print(1)
            #     pass
            today_position = pd.DataFrame(columns=['time', 'stkcd', 'weight'])  # 重置数据
            if today == name:
                # 换仓日，仓位保持与上个月一样
                today_position = yesterday_position.copy()
                today_position['time'] = today
                # 并记录上个月的position
                last_month_position = today_position.copy()
            elif yesterday == name:
                # 换仓日第二天
                # 先确定准备买入和卖出的股票，selling和buying
                # selling包括两部分，一部分是上个月持有的，这月要全部卖完的，另一部分是上个月持有的，但是要减仓的
                # buying包括两部分，一部分是上个月没有的，这个月要买入的，另一部分是上个月持有的，但是要加仓的

                # selling和buying的第一部分
                selling = last_month_position[-last_month_position.stkcd.isin(group.stkcd)]
                buying = group[-group.stkcd.isin(last_month_position.stkcd)]

                # selling和buying的第二部分，算法是merge上个月和这个月的持仓，根据weight变化方向记录下这部分股票，记为pluslist和minuslist
                adjust_weight = pd.merge(group, last_month_position, on=['time', 'stkcd'], suffixes=['', '_yesterday'])
                adjust_weight['weight'] = adjust_weight['weight'] - adjust_weight['weight_yesterday']
                pluslist = list(adjust_weight.query("weight > 0").stkcd)
                minuslist = list(adjust_weight.query("weight < 0").stkcd)

                # 把这第二部分加到selling和buying里
                buying = buying.append(adjust_weight.query("weight > 0").loc[:, ['time', 'stkcd', 'score', 'weight']])
                selling = selling.append(adjust_weight.query("weight < 0").loc[:, ['time', 'stkcd', 'score', 'weight']])
                selling['weight'] = abs(selling['weight'])  # weight统一为正

                # 若buying为空，直接填充到月底或季度底
                if len(buying) == 0:
                    today_position = group.copy()
                    for i in period[period.index(today):]:
                        today_position['time'] = i
                        final_result = final_result.append(today_position)
                        last_month_position = today_position.copy()
                    break

                # 查找buying和selling的交易状态
                selling = pd.merge(selling, status_limit[status_limit.time == today], on='stkcd')
                buying = pd.merge(buying, status_limit[status_limit.time == today], on='stkcd')

                # 确定tosell
                tosell = selling[(selling.status == 'Suspension') | (selling.LimitUD == -1)][
                    ['stkcd', 'score', 'weight']]
                toselllist = list(tosell.stkcd)

                # 算出仓位不够买入的股票
                ## 对于上个月和这个月都持有的股票，取二者中weight较小的作为底部持仓
                adjust_weight.loc[adjust_weight['weight'] >= 0, 'weight_plus'] = adjust_weight['weight_yesterday']
                adjust_weight.loc[adjust_weight['weight'] < 0, 'weight_plus'] = adjust_weight['weight_yesterday'] + \
                                                                                adjust_weight['weight']

                # 计算还可以买进的权重上限weight_left
                weight_left = 1 - (adjust_weight['weight_plus'].sum() + tosell.weight.sum())

                # 算出可买的股票
                buy_available = buying[(buying.status != 'Suspension') & (buying.LimitUD != 1)]. \
                    sort_values('score', ascending=False)

                # 在可买的股票里计算出哪些仓位不够买入，记作no_space_tobuy
                no_space_tobuy = []
                get_no_space_tobuy(weight_left, buy_available, no_space_tobuy)

                # 待买的，两部分，一部分是不可交易的，另一部分是仓位不够买入的
                tobuylist = list(buying[(buying.status == 'Suspension') | (buying.LimitUD == 1)].stkcd)
                tobuylist.extend(no_space_tobuy)
                tobuy = buying[buying.stkcd.isin(tobuylist)][['stkcd', 'score', 'weight']]

                # 算今天的实际仓位，即保留待卖的，删去待买的
                # 第一步，删去tobuylist的股票，但是可能会把加仓的股票也删掉
                today_position = group[-group.stkcd.isin(tobuylist)]
                if tobuy.stkcd.isin(pluslist).any():
                    # 如果tobuylist有加仓的股票，则要在上个月的持仓中找到该股票以及权重，添加进来
                    today_position = today_position.append(
                        last_month_position[last_month_position.stkcd.isin(tobuylist)])

                # 第二步，添加tosellist的股票，但是可能会把减仓的股票也添加进来，导致有两行股票代码一样
                if tosell.stkcd.isin(minuslist).any():
                    # 如果toselllist有减仓的股票，则要先把该股票扣除，以避免重复
                    # 获取toselllist中属于减仓的股票
                    tosell_in_minuslist = list(tosell.stkcd.isin(minuslist))
                    today_position = today_position[-today_position.stkcd.isin(tosell_in_minuslist)]
                today_position = today_position.append(last_month_position[last_month_position.stkcd.isin(toselllist)])

                # 统一日期和顺序
                today_position['time'] = today
                today_position = today_position.sort_values("stkcd")

            else:
                # 普通交易日
                if len(tobuylist) == 0:
                    # 若tobuy为空，直接填充到月底或季度底
                    today_position = group.copy()
                    for i in period[period.index(today):]:
                        today_position['time'] = i
                        final_result = final_result.append(today_position)
                        yesterday_position = today_position.copy()
                    break
                else:
                    selling = pd.merge(tosell, status_limit[status_limit.time == today], on='stkcd')
                    tosell = selling[(selling.status == 'Suspension') | (selling.LimitUD == -1)][
                        ['stkcd', 'score', 'weight']]
                    toselllist = list(tosell.stkcd)
                    # 计算出卖掉的权重
                    weight_sell = selling[-selling.stkcd.isin(toselllist)].weight.sum()

                    buying = pd.merge(tobuy, status_limit[status_limit.time == today], on='stkcd')

                    if ((buying.status == 'Suspension') | (buying.LimitUD == 1)).all():
                        pass
                    else:
                        # 计算还可以买进的权重上限weight_left
                        weight_left = 1 - (yesterday_position.weight.sum() - weight_sell)

                        # 算出可买的股票
                        buy_available = buying[(buying.status != 'Suspension') & (buying.LimitUD != 1)]. \
                            sort_values('score', ascending=False)

                        # 在可买的股票里计算出哪些仓位不够买入，记作no_space_tobuy
                        no_space_tobuy = []
                        get_no_space_tobuy(weight_left, buy_available, no_space_tobuy)

                        # 待买的，两部分，一部分是不可交易的，另一部分是仓位不够买入的
                        tobuylist = list(buying[(buying.status == 'Suspension') | (buying.LimitUD == 1)].stkcd)
                        tobuylist.extend(no_space_tobuy)
                        tobuy = buying[buying.stkcd.isin(tobuylist)][['stkcd', 'score', 'weight']]

                # 算今天的实际仓位，即保留待卖的，删去待买的
                # 第一步，删去tobuylist的股票，但是可能会把加仓的股票也删掉
                today_position = group[-group.stkcd.isin(tobuylist)]
                if tobuy.stkcd.isin(pluslist).any():
                    # 如果tobuylist有加仓的股票，则要在上个月的持仓中找到该股票以及权重，添加进来
                    today_position = today_position.append(
                        last_month_position[last_month_position.stkcd.isin(tobuylist)])

                # 第二步，添加tosellist的股票，但是可能会把减仓的股票也添加进来，导致有两行股票代码一样
                if tosell.stkcd.isin(minuslist).any():
                    # 如果toselllist有减仓的股票，则要先把该股票扣除，以避免重复
                    # 获取toselllist中属于减仓的股票
                    tosell_in_minuslist = list(tosell.stkcd.isin(minuslist))
                    today_position = today_position[-today_position.stkcd.isin(tosell_in_minuslist)]
                today_position = today_position.append(last_month_position[last_month_position.stkcd.isin(toselllist)])

                # 统一日期和顺序
                today_position['time'] = today
                today_position = today_position.sort_values("stkcd")
            # 每天最后把今日持仓写入final_result
            final_result = final_result.append(today_position)
            # 把今天赋值给昨天
            yesterday = today
            yesterday_position = today_position.copy()
    return final_result


def weighttoweight(change_position_day, factor_input_weight, factor):
    # 取得参数的deepcopy 防止污染参数数据
    change_position_day = change_position_day.copy()
    factor_input_weight = factor_input_weight.copy()

    final = change_position_day.copy()
    for i in range(len(factor_input_weight)):
        final.loc[:, factor[i]] = factor_input_weight[i]
    return final


def exclude_suspension(CPD_factor, trade_status):
    # 取得参数的deepcopy 防止污染参数数据
    CPD_factor = CPD_factor.copy()
    trade_status = trade_status.copy()

    result = pd.merge(CPD_factor, trade_status, on=["time", "stkcd"], how="left")
    result = result.dropna()
    result = result[result.status != "Suspension"]
    return result.drop("status", axis=1)
