#!/usr/bin/env python
# coding:utf-8

# **********************************************************
# * Author        : xfzheng
# * Email         : 329472010@qq.com
# * Create time   : 2019-03-29 11:30
# * Last modified : 2019-03-29 11:30
# * Filename      : class_template.py
# * Description   :
# **********************************************************
from BaseDag import BaseDag


class Mzsaleinvst20172019(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP TABLE belle_sh.mz_sale_inv_st_2017_2019;


CREATE TABLE belle_sh.mz_sale_inv_st_2017_2019 AS
SELECT *
FROM belle_sh.mzj_pmt_sku_store_avg_st_17
UNION ALL
SELECT *
FROM belle_sh.mz_sku_store_avg_st_18
UNION ALL
SELECT *
FROM belle_sh.mzj_pmt_sku_store_avg_st_19"""
        self.call()

if __name__ == '__main__':
    Mzsaleinvst20172019().run_command()
