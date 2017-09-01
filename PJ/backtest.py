# -*- coding:utf-8 -*-
# Copyright GDP Group
# Created by songzhichen and ruchuang gao on July 8 2017


# backtest class

from Function import other_func
from Function import score_method
from Global_Info import Global_Info
from MySQLite.MySQLite_Manager import MySQLite
from Performance import perform
from Performance import output


class BackTest:
    def __init__(self, kwargs):
        # 初始化策略id
        self.id = other_func.get_id()
        # 传参
        self.global_info = Global_Info()
        self.path = kwargs.get("path")
        self.factor = kwargs.get("factor")
        self.start = kwargs.get("start", self.global_info.start)
        self.end = kwargs.get("end", self.global_info.end)
        self.benchmark = kwargs.get("benchmark", self.global_info.benchmark)
        self.universe = kwargs.get("universe", self.global_info.universe)
        self.freq = kwargs.get("freq", self.global_info.freq)
        self.weightType = kwargs.get("weightType", self.global_info.weightType)
        self.n = kwargs.get("n", self.global_info.n)
        self.industry_neutral = kwargs.get("industry_neutral", self.global_info.industry_neutral)
        self.positionType = kwargs.get("positionType", self.global_info.positionType)
        self.backperiod = kwargs.get("backperiod", self.global_info.backperiod)
        self.hedgemethod = kwargs.get("hedgemethod", self.global_info.hedgemethod)
        self.margin = kwargs.get("margin", self.global_info.margin)
        self.tradecost = kwargs.get("tradecost", self.global_info.tradecost)

        # 连接数据库
        self.connection = MySQLite(self.path)

        # 进行传参检验
        if self.weightType in [1, 2]:
            self.factor_direction = kwargs.get("factor_direction")
            if len(self.factor) != len(self.factor_direction):
                print("facot and factor_direction must be the same length!")
                assert False
        if self.weightType == 0:
            self.factor_input_weight = kwargs.get("factor_weight")
            if len(self.factor) != len(self.factor_input_weight):
                print("facot and factor_weight must be the same length!")
                assert False

    # 获得因子表和行情表
    def get_data(self):
        # 获得数据库内所有交易日的时间序列
        self.all_tradingday = other_func.get_all_tradedate(self.connection.conn)

        # 根据交易日序列，修改start和end
        self.start = other_func.revise_start(self.start, list(self.all_tradingday.time))
        self.end = other_func.revise_end(self.end, list(self.all_tradingday.time))

        # 得到起止日期之间的调仓日期序列
        self.change_position_day = other_func.tradeday(self.start, self.end, self.freq, self.all_tradingday)

        back_date = other_func.back_date(self.start, self.backperiod, self.all_tradingday)

        # 得到起止日期之间的因子表
        self.all_factor_data = self.connection.get_factors(back_date, self.end, self.factor, self.universe)

        # 得到需要日期的close和pre_close表
        self.all_trading_data = self.connection.get_trading(back_date, self.end, self.benchmark, self.universe)

        # ev/vol表
        self.ev = 0
        self.vol = 0
        if self.positionType == 2:
            back_date_vol = other_func.back_date(self.start, 20, self.all_tradingday)
            self.vol = self.connection.get_factors(back_date_vol, self.end, ["volatility"], self.universe)
        elif self.positionType == 3:
            self.ev = self.connection.get_factors(self.start, self.end, ["marketvalue"], self.universe)

        # 是否维持行业中性，进行n的输入判断
        if self.industry_neutral == 1:
            if self.n < 29:
                print("输入的持有股票个数n应大于29！请重新输入！")
            if self.universe == None:
                print("无法在A股范围内进行行业市值中性！请重新输入universe！")
            assert self.n >= 29 and self.universe != None
            # 得到对应指数的行业占比
            self.CPD_stk_inudstry, self.CPD_inudstry_weight = self.connection.get_industry(self.change_position_day,
                                                                                           self.universe)
        # 得到交易状态表
        self.trade_status = self.connection.get_factors(self.start, self.end, ["status"], self.universe)
        # 获取涨跌停数据(规则：开盘价相比于昨日收盘价上涨大于等于9.5%：1；开盘价相比于昨日收盘价下跌大于等于9.5%：-1；其他：0)
        self.trade_limit = self.connection.get_factors(self.start, self.end, ["LimitUD"], self.universe)

    # 输入因子表和行情表，返回调仓日的因子权重表
    def factor_weight(self):
        # 用户主动输入因子权重
        if self.weightType == 0:
            self.weight = other_func.weighttoweight(self.change_position_day, self.factor_input_weight, self.factor)

        # 等权重和PCA方法
        elif self.weightType in [1, 2]:
            self.weight = self.global_info.factor_weight[self.weightType](self.all_factor_data, self.all_trading_data,
                                                                          self.change_position_day, self.backperiod,
                                                                          self.all_tradingday, self.benchmark)
            # 根据用户输入方向进行调整
            for i in range(self.weight.shape[1] - 1):
                self.weight.iloc[:, i + 1] = self.weight.iloc[:, i + 1] * self.factor_direction[i]
        # 回归方法的到因子权重
        else:
            self.weight = self.global_info.factor_weight[self.weightType](self.all_factor_data, self.all_trading_data,
                                                                          self.change_position_day, self.backperiod,
                                                                          self.all_tradingday, self.benchmark)

    # 输入因子表、因子权重序列，获得每日持仓;使用scorepercent方式进行选股排序，得到调仓日的持仓表
    # n：股票个数,top:  搞个接口来做这个东西
    def get_position(self, condition="top"):
        # 得到调仓日的factor data
        self.CPD_factor = other_func.get_tradefactor(self.all_factor_data, self.change_position_day)
        # 剔除调仓日停牌的股票（假设停牌时间具有延续性）
        self.CPD_factor = other_func.exclude_suspension(self.CPD_factor, self.trade_status)

        # 不进行行业市值中性处理
        if self.industry_neutral == 0:
            # 根据限制后的调仓日因子表、因子权重进行交易日的选股
            self.symbol = score_method.scorepercent(self.CPD_factor, self.weight, self.n, condition=condition)
            # 根据positionType进行仓位分配，获得调仓日的仓位表
            self.CPD_position = self.global_info.position_weight[self.positionType](symbol=self.symbol, ev=self.ev,
                                                                                    vol=self.vol)
        # 进行行业市值中性处理(暂时默认仓位分配为等权重处理#todo：加入positionType，多种权重方式)
        elif self.industry_neutral == 1:
            self.CPD_position = score_method.scorepercent_industry(self.CPD_factor, self.weight,
                                                                   self.CPD_inudstry_weight,
                                                                   self.CPD_stk_inudstry, self.n, condition=condition)

    # 输入每日选股持仓表，得到全部交易日的持仓表
    def position_restrict(self):
        # 根据调仓日的实际持仓表，展期到全部的相关交易日，确定为当日的持仓
        # 处理了涨跌停、停牌等限制，返回当日的实际持仓表
        self.all_tradedate_position = other_func.position_extension(self.CPD_position,
                                                                    self.all_trading_data,
                                                                    self.all_tradingday,
                                                                    self.change_position_day,
                                                                    self.freq, self.trade_status,
                                                                    self.trade_limit)

    # 输入全部交易日的实际持仓表（考虑涨跌停等限制后），返回策略的净值表现
    def get_portfolio(self):
        # 根据每日的实际持仓，计算每日交易成本表和股票具体买卖点
        self.trade_detail, self.cost = perform.get_cost(self.all_tradedate_position)

        # 输入每日持仓表、每日交易成本、股票具体买卖点、行情数据表、对冲方法和基准标的，返回净值表现
        self.portfolio = perform.get_portfolio(self.all_tradedate_position, self.trade_detail, self.cost,
                                               self.all_trading_data,
                                               self.benchmark, self.hedgemethod, self.margin, self.tradecost)

    # 把每日的实际持仓和净值表现写入txt，存储在Backtest_Position和Backtest_Graph文件夹里
    def output_result(self):
        output.store_position(self.all_tradedate_position, self.id, para_dict)
        output.store_graph(self.portfolio, self.id, para_dict)

    def run(self):
        self.get_data()
        self.factor_weight()
        self.get_position()
        self.position_restrict()
        self.get_portfolio()
        self.output_result()


if __name__ == "__main__":
    para_dict = {
        # "path": 'D:\strategy\GDP\GD.db',
        "path": 'F:\project_gdp\GD.db',
        "factor": ["pb"],
        # "weightType": 3,
        "n": 100,
        "industry_neutral": 1,
        "weightType": 1,
        "positionType": 2,
        "factor_direction": [1],
        # "weightType": 0,
        # "factor_weight": [0.5, -0.5],
        "start": "2010-01-01",
        "end": "2011 -01-01",
        "backperiod": 20,
        "hedgemethod": 1
    }

    backtest = BackTest(para_dict)
    backtest.run()

    # backtest.trade_position.to_csv("position_trade.csv")
    # backtest.final_position.to_csv("position_final.csv")
    # backtest.all_trading_data.to_csv("all_trading_data.csv")
    # backtest.all_tradedate_position.to_csv("all_tradedate_position.csv")
    # backtest.CPD_position.to_csv("position.csv")
    # backtest.portfolio.to_csv("portfolio.csv")
    # backtest.CPD_position.to_csv("CPD_position.csv")
    # backtest.all_tradedate_position.to_csv("all_tradedate_position.csv")
    # print(backtest.daily_return)
    print(backtest.portfolio)
    # print(backtest.cost)
    # print(backtest.trade_detail)
    # import pickle
    # pickle.dump(backtest.trade_status,open("trade_status","wb"))
    # pickle.dump(backtest.factor_data, open("factor_data", "wb"))
    # pickle.dump(backtest.all_tradedate,open("all_tradedate","wb"))
    # pickle.dump(backtest.tradeday,open("tradeday","wb"))
    # pickle.dump(backtest.all_factor_data, open("all_factor_data", "wb"))
    # pickle.dump(backtest.all_trading_data, open("all_trading_data", "wb"))
    # pickle.dump(backtest.weight, open("weight", "wb"))
    # pickle.dump(backtest.factor_data, open("factor_data", "wb"))
    # pickle.dump(backtest.trade_position, open("trade_position", "wb"))
    # pickle.dump(backtest.final_position, open("final_position", "wb"))
    # pickle.dump(backtest.daily_return, open("daily_return", "wb"))
    # pickle.dump(backtest.portfolio, open("portfolio", "wb"))
