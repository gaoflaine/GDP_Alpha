import pickle
import pandas as pd
def get_no_space_tobuy(weight_left, buy_available, no_space_tobuy):
    if len(buy_available) == 1:
        if round(buy_available['weight'].iloc[0], 6) > round(weight_left, 6):
            no_space_tobuy.append(buy_available['stkcd'].iloc[0])
        return None
    if buy_available['weight'].iloc[0] < weight_left:
        return get_no_space_tobuy(weight_left - buy_available['weight'].iloc[0], buy_available.iloc[1:, :], no_space_tobuy)
    else:
        no_space_tobuy.append(buy_available['stkcd'].iloc[0])
        return get_no_space_tobuy(weight_left, buy_available.iloc[1:, :], no_space_tobuy)
weight_left = 0.291840332377573
buy_available = pickle.load(open('buy_available.txt', 'rb'))
no_space_tobuy = []
get_no_space_tobuy(weight_left, buy_available, no_space_tobuy)
print(no_space_tobuy)
