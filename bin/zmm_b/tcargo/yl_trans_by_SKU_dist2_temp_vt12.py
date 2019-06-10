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

class Yltransbyskudist2tempvt12(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_dist2_temp_vt12;
CREATE TABLE belle_sh.yl_trans_by_SKU_dist2_temp_vt12 AS
SELECT
a.*
,nvl(lag(cum_220_inv_left,1) over(PARTITION BY product_code ORDER BY nvl(ind_priority_inv,0)*nvl(inv_priority_220,0) DESC,if(days_no_sale_4w=28,1,0),final_score_rank),inv_end_220_total-new_sum_inv_220_no_out) AS cum_220_inv_left_lag1
,nvl(lag(cum_225_inv_left,1) over(PARTITION BY product_code ORDER BY nvl(ind_priority_inv,0)*nvl(inv_priority_225,0) DESC,if(days_no_sale_4w=28,1,0),final_score_rank),inv_end_225_total-new_sum_inv_225_no_out) AS cum_225_inv_left_lag1
,nvl(lag(cum_230_inv_left,1) over(PARTITION BY product_code ORDER BY nvl(ind_priority_inv,0)*nvl(inv_priority_230,0) DESC,if(days_no_sale_4w=28,1,0),final_score_rank),inv_end_230_total-new_sum_inv_230_no_out) AS cum_230_inv_left_lag1
,nvl(lag(cum_235_inv_left,1) over(PARTITION BY product_code ORDER BY nvl(ind_priority_inv,0)*nvl(inv_priority_235,0) DESC,if(days_no_sale_4w=28,1,0),final_score_rank),inv_end_235_total-new_sum_inv_235_no_out) AS cum_235_inv_left_lag1
,nvl(lag(cum_240_inv_left,1) over(PARTITION BY product_code ORDER BY nvl(ind_priority_inv,0)*nvl(inv_priority_240,0) DESC,if(days_no_sale_4w=28,1,0),final_score_rank),inv_end_240_total-new_sum_inv_240_no_out) AS cum_240_inv_left_lag1
,nvl(lag(cum_245_inv_left,1) over(PARTITION BY product_code ORDER BY nvl(ind_priority_inv,0)*nvl(inv_priority_245,0) DESC,if(days_no_sale_4w=28,1,0),final_score_rank),inv_end_245_total-new_sum_inv_245_no_out) AS cum_245_inv_left_lag1
FROM belle_sh.yl_trans_by_SKU_dist2_{invsunday}_spr_test_vt12 a
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Yltransbyskudist2tempvt12().run_command()
