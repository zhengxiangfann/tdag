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

class Optransbyskufinalspr(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.op_trans_by_sku_final_{invsunday}_spr;
CREATE TABLE belle_sh.op_trans_by_sku_final_{invsunday}_spr AS
SELECT
a.*
FROM
(
SELECT *
FROM belle_sh.yl_trans_by_sku_final_{invsunday}_spr_vt12
WHERE label_sku_logic_v2 NOT IN (2,3,6,8)
UNION ALL
SELECT *
FROM belle_sh.xw_trans_by_sku_final_{invsunday}_spr_vt12
WHERE label_sku_logic_v2 IN (2,3,6,8)
) a
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Optransbyskufinalspr().run_command()
