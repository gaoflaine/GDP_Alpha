# -*- coding:utf-8 -*-
# Copyright GDP Group
# Created by songzhichen and ruchuang gao on July 8 2017
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt


def store_position(all_tradedate_position, id, para_dict):
    all_tradedate_position = all_tradedate_position.copy()
    path = './Backtest_result/Position/' + id + '.txt'
    df = all_tradedate_position.loc[:, ['time', 'stkcd', 'weight']]
    df.to_csv(path, sep='\t', index=True)
    f = open(path, 'r+')
    lines1 = f.readlines()
    lines2 = ['{}: {}\n'.format(k, v) for k, v in para_dict.items()]
    lines2.insert(0, "INPUT:\n")
    lines2.append('\n')
    lines1.insert(0, "OUTPUT:\n")
    lines2.extend(lines1)
    f.seek(0, 0)
    f.writelines(lines2)
    f.close()


def store_graph(portfolio, id, hedgemethod):
    portfolio = portfolio.copy()
    path = './Backtest_result/Graph/' + id + '.jpg'
    portfolio['time'] = [dt.datetime.strptime(i, '%Y-%m-%d') for i in portfolio.index]
    plt.plot(portfolio.time, portfolio.value_hedged)
    plt.savefig(path)
