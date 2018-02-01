# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
class Feature:

    df = None
    f =  {}   # 符合 one-hot 规则
    cp = {}
    f_cols_names=None
    # 单个数据特征 : 均值 卷积 均方差->衡量卷积框

    # 联合特征 :  相关系数 交错相关系数

    def __init__(self,df,f_cols_names=None):
        if f_cols_names==None:
            self.df = df
        else:
            self.df = df.loc[:,f_cols_names]
        self.f['E'] = np.mean(self.df)

    def compare(self,ft):

        if type(ft) != Feature or ft.f_cols_names!=self.f_cols_names:
            return None
        return self.cp

    def loop_max_compare(self):
        return 0 , self.cp

    def update(self):
        return self.f

    def commit(self):
        return None
    pass