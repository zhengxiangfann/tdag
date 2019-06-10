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


class Optransbyskufinal20190421spr(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.op_trans_by_sku_final_20190421_spr;
CREATE TABLE belle_sh.op_trans_by_sku_final_20190421_spr AS
SELECT *
FROM belle_sh.yl_trans_by_sku_final_20190421_spr_v10
WHERE label_sku_logic_v2 NOT IN (2,3)
UNION ALL
SELECT *
FROM belle_sh.xw_trans_by_sku_final_20190421_spr_v11a
WHERE label_sku_logic_v2 IN (2,3)
;"""
        self.call()

if __name__ == '__main__':
    Optransbyskufinal20190421spr().run_command()
