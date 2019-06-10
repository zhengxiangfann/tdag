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

class Xwtransbyskutempbvt12(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.xw_trans_by_SKU_temp_b_vt12;
CREATE TABLE belle_sh.xw_trans_by_SKU_temp_b_vt12 AS
SELECT
a.*
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,nvl(ind_priority_inv,0)*nvl(inv_priority_220,0),if(inv_end_store >0,0,1), if(inv_end_220_store - inv_220_from_score_b >0, 0,1) , if(days_no_sale <= 56, 0,1), sum_qty_2week_store,inv_220_from_score_b,if(cnt_main_size4<3,0,1),final_normed_avg_sr_score DESC) AS rank_2nd_round_220_b
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,nvl(ind_priority_inv,0)*nvl(inv_priority_225,0),if(inv_end_store >0,0,1), if(inv_end_225_store - inv_225_from_score_b >0, 0,1) , if(days_no_sale <= 56, 0,1), sum_qty_2week_store,inv_225_from_score_b,if(cnt_main_size4<3,0,1),final_normed_avg_sr_score DESC) AS rank_2nd_round_225_b
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,nvl(ind_priority_inv,0)*nvl(inv_priority_230,0),if(inv_end_store >0,0,1), if(inv_end_230_store - inv_230_from_score_b >0, 0,1) , if(days_no_sale <= 56, 0,1), sum_qty_2week_store,inv_230_from_score_b,if(cnt_main_size4<3,0,1),final_normed_avg_sr_score DESC) AS rank_2nd_round_230_b
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,nvl(ind_priority_inv,0)*nvl(inv_priority_235,0),if(inv_end_store >0,0,1), if(inv_end_235_store - inv_235_from_score_b >0, 0,1) , if(days_no_sale <= 56, 0,1), sum_qty_2week_store,inv_235_from_score_b,if(cnt_main_size4<3,0,1),final_normed_avg_sr_score DESC) AS rank_2nd_round_235_b
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,nvl(ind_priority_inv,0)*nvl(inv_priority_240,0),if(inv_end_store >0,0,1), if(inv_end_240_store - inv_240_from_score_b >0, 0,1) , if(days_no_sale <= 56, 0,1), sum_qty_2week_store,inv_240_from_score_b,if(cnt_main_size4<3,0,1),final_normed_avg_sr_score DESC) AS rank_2nd_round_240_b
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,nvl(ind_priority_inv,0)*nvl(inv_priority_245,0),if(inv_end_store >0,0,1), if(inv_end_245_store - inv_245_from_score_b >0, 0,1) , if(days_no_sale <= 56, 0,1), sum_qty_2week_store,inv_245_from_score_b,if(cnt_main_size4<3,0,1),final_normed_avg_sr_score DESC) AS rank_2nd_round_245_b
FROM belle_sh.xw_trans_by_SKU_b_dist2_{invsunday}_spr_size5_vt12 a
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Xwtransbyskutempbvt12().run_command()
