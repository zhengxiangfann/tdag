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


class Yltransbyskuadnoouttestv10(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_ad_no_out_test_v10;
CREATE TABLE belle_sh.yl_trans_by_SKU_ad_no_out_test_v10 AS
SELECT
a.*
,sum(least(inv_end_220_store,inv_220_from_score)*nvl(greatest(ind_last_week,ind_no_out),0)) over(PARTITION BY a.product_code) AS new_sum_inv_220_no_out
,sum(least(inv_end_225_store,inv_225_from_score)*nvl(greatest(ind_last_week,ind_no_out),0)) over(PARTITION BY a.product_code) AS new_sum_inv_225_no_out
,sum(least(inv_end_230_store,inv_230_from_score)*nvl(greatest(ind_last_week,ind_no_out),0)) over(PARTITION BY a.product_code) AS new_sum_inv_230_no_out
,sum(least(inv_end_235_store,inv_235_from_score)*nvl(greatest(ind_last_week,ind_no_out),0)) over(PARTITION BY a.product_code) AS new_sum_inv_235_no_out
,sum(least(inv_end_240_store,inv_240_from_score)*nvl(greatest(ind_last_week,ind_no_out),0)) over(PARTITION BY a.product_code) AS new_sum_inv_240_no_out
,sum(least(inv_end_245_store,inv_245_from_score)*nvl(greatest(ind_last_week,ind_no_out),0)) over(PARTITION BY a.product_code) AS new_sum_inv_245_no_out
FROM belle_sh.yl_trans_by_SKU_dist1_20190421_spr_test_v10 a
;"""
        self.call()

if __name__ == '__main__':
    Yltransbyskuadnoouttestv10().run_command()
