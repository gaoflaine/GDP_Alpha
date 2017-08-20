# -*- coding:utf-8 -*-
# Copyright GDP Group
# Created by songzhichen and ruchuang gao on July 8 2017


# database manager class
import sqlite3
import sys
import pandas as pd


class MySQLite:
    def __init__(self, path):
        try:
            self.conn = sqlite3.connect(path)
            self.cur = self.conn.cursor()
        except:
            info = sys.exc_info()
            print(info[0], ":", info[1])

    # factorID:因子序列,起止日期和选股池！！！
    def get_factors(self, start, end, factorID, universe=None):
        factorlist = []
        for name in factorID:
            if universe is None:
                SQL = '''
                    select t1.time, t1.stkcd, t1.value as {name}
                    from {table} as t1
                    where t1.time between '{start}' and '{end}'
                    order by t1.time asc
                '''.format(name=name, table=name, start=start, end=end)
            else:
                SQL = '''  
                    select t1.time, t1.stkcd, t1.value as {name}
                    from {table} as t1
                    join indexweight as t2
                    on t1.time = t2.time and t1.stkcd = t2.stkcd
                    where t2.indexname = '{universe}' and t1.time between '{start}' and '{end}'
                '''.format(name=name, table=name, start=start, end=end, universe=universe)
            data = pd.read_sql_query(SQL, self.conn)
            factorlist.append(data)
        factor_merge = factorlist[0]
        for df in factorlist[1:]:
            factor_merge = factor_merge.merge(df, on=['time', 'stkcd'])
            # print(factor_merge)
        return factor_merge

    # 返回所有股票和基准标的收盘价和昨日收盘价
    def get_trading(self, start, end, benchmark, universe=None):
        # 指数的开盘数据不需要，暂时使用收盘数据代替 #todo：修改为真正的开盘数据
        if universe is None:
            SQL = '''
            select time, stkcd, closep, preclosep, openp
            from BasicInfo
            where time between '{start}' and '{end}' 
            union all
            select time, stkcd, closep, preclosep, closep as openp
            from indexprice
            where time between '{start}' and '{end}' 
            and stkcd = '{benchmark}'
            '''.format(start=start, end=end, benchmark=benchmark)
        else:
            SQL = '''
            select t1.time, t1.stkcd, t1.closep, t1.preclosep, t1.openp
            from Basicinfo as t1
            join indexweight as t2
            on t1.time = t2.time and t1.stkcd = t2.stkcd
            where t2.indexname = '{universe}' and t1.time between '{start}' and '{end}'
            union all
            select time, stkcd, closep, preclosep, closep as openp
            from indexprice
            where time between '{start}' and '{end}'
            and stkcd = '{benchmark}'
            '''.format(start=start, end=end, benchmark=benchmark, universe=universe)

            # print(pd.read_sql_query(SQL, self.conn))
        return pd.read_sql_query(SQL, self.conn)

    # 返回调仓日的universe各行业权重和调仓日股票的行业分类表
    def get_industry(self, change_position_day, universe):
        SQL1 = '''
            select t1.time, t1.stkcd, t2.industry as industry
            from indexweight as t1
            join industry as t2
            on t1.stkcd = t2.stkcd
            where t1.time in {period} and t1.indexname = '{universe}'
            '''.format(period='(\'' + '\',\''.join(list(change_position_day.time)) + '\')', universe=universe)
        SQL2 = '''
            select t1.time, t1.industry as industry, t1.weight,t1.num
            from industryweight as t1
            where t1.time in {period}
            and t1.indexname = '{universe}' 
            '''.format(period='(\'' + '\',\''.join(list(change_position_day.time)) + '\')', universe=universe)
        CPD_stk_inudstry = pd.read_sql_query(SQL1, self.conn)
        CPD_inudstry_weight = pd.read_sql_query(SQL2, self.conn)
        return CPD_stk_inudstry, CPD_inudstry_weight
