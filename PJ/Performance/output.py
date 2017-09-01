# -*- coding:utf-8 -*-
# Copyright GDP Group
# Created by songzhichen and ruchuang gao on July 8 2017
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
def store_position(all_tradedate_position, id, para_dict):
    path = './Backtest_Position/'+ id +'.txt'
    all_tradedate_position = all_tradedate_position.copy()
    df = all_tradedate_position.loc[:, ['time', 'stkcd', 'weight']]
    df.to_csv(path, sep='\t', index=True)
    f = open(path, 'r+')
    lines = f.readlines()
    for k, v in para_dict.items():
        lines.insert(0, '{}:{} \n'.format(k, v))
    f.seek(0, 0)
    f.writelines(lines)
    f.close()

def store_graph():

    pass