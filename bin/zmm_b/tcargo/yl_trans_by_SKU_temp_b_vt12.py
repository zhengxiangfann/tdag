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

class Yltransbyskutempbvt12(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_temp_b_vt12;
CREATE TABLE belle_sh.yl_trans_by_SKU_temp_b_vt12 AS
SELECT
a.*
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,nvl(ind_priority_inv,0)*nvl(inv_priority_220,0),inv_220_from_score_b,days_no_sale_4w,inv_220_from_score_b-inv_end_220_store,sum_qty_2week_store DESC,final_normed_avg_sr_score DESC) AS rank_2nd_round_220_b
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,nvl(ind_priority_inv,0)*nvl(inv_priority_225,0),inv_225_from_score_b,days_no_sale_4w,inv_225_from_score_b-inv_end_225_store,sum_qty_2week_store DESC,final_normed_avg_sr_score DESC) AS rank_2nd_round_225_b
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,nvl(ind_priority_inv,0)*nvl(inv_priority_230,0),inv_230_from_score_b,days_no_sale_4w,inv_230_from_score_b-inv_end_230_store,sum_qty_2week_store DESC,final_normed_avg_sr_score DESC) AS rank_2nd_round_230_b
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,nvl(ind_priority_inv,0)*nvl(inv_priority_235,0),inv_235_from_score_b,days_no_sale_4w,inv_235_from_score_b-inv_end_235_store,sum_qty_2week_store DESC,final_normed_avg_sr_score DESC) AS rank_2nd_round_235_b
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,nvl(ind_priority_inv,0)*nvl(inv_priority_240,0),inv_240_from_score_b,days_no_sale_4w,inv_240_from_score_b-inv_end_240_store,sum_qty_2week_store DESC,final_normed_avg_sr_score DESC) AS rank_2nd_round_240_b
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,nvl(ind_priority_inv,0)*nvl(inv_priority_245,0),inv_245_from_score_b,days_no_sale_4w,inv_245_from_score_b-inv_end_245_store,sum_qty_2week_store DESC,final_normed_avg_sr_score DESC) AS rank_2nd_round_245_b
FROM belle_sh.yl_trans_by_SKU_b_dist2_{invsunday}_spr_size5_vt12 a
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Yltransbyskutempbvt12().run_command()
