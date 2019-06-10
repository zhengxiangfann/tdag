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


class Yltransbyskutempv10(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_temp_v10;
CREATE TABLE belle_sh.yl_trans_by_SKU_temp_v10 AS
SELECT
a.*
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,ind_no_in,nvl(ind_priority_inv,0)*nvl(inv_priority_220,0),ind_i014st_from_score_220,ind_main_size_6plus,if(days_no_sale_4w=28,1,0),ind_dist_0,sum_qty_2week_store DESC,final_normed_avg_sr_score DESC) AS rank_2nd_round_220
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,ind_no_in,nvl(ind_priority_inv,0)*nvl(inv_priority_225,0),ind_i014st_from_score_225,ind_main_size_6plus,if(days_no_sale_4w=28,1,0),ind_dist_0,sum_qty_2week_store DESC,final_normed_avg_sr_score DESC) AS rank_2nd_round_225
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,ind_no_in,nvl(ind_priority_inv,0)*nvl(inv_priority_230,0),ind_i014st_from_score_230,ind_main_size_6plus,if(days_no_sale_4w=28,1,0),ind_dist_0,sum_qty_2week_store DESC,final_normed_avg_sr_score DESC) AS rank_2nd_round_230
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,ind_no_in,nvl(ind_priority_inv,0)*nvl(inv_priority_235,0),ind_i014st_from_score_235,ind_main_size_6plus,if(days_no_sale_4w=28,1,0),ind_dist_0,sum_qty_2week_store DESC,final_normed_avg_sr_score DESC) AS rank_2nd_round_235
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,ind_no_in,nvl(ind_priority_inv,0)*nvl(inv_priority_240,0),ind_i014st_from_score_240,ind_main_size_6plus,if(days_no_sale_4w=28,1,0),ind_dist_0,sum_qty_2week_store DESC,final_normed_avg_sr_score DESC) AS rank_2nd_round_240
,row_number() over(PARTITION BY product_code ORDER BY ind_clear,ind_no_in,nvl(ind_priority_inv,0)*nvl(inv_priority_245,0),ind_i014st_from_score_245,ind_main_size_6plus,if(days_no_sale_4w=28,1,0),ind_dist_0,sum_qty_2week_store DESC,final_normed_avg_sr_score DESC) AS rank_2nd_round_245
FROM belle_sh.yl_trans_by_SKU_dist2_temp_1st_v10 a
;"""
        self.call()

if __name__ == '__main__':
    Yltransbyskutempv10().run_command()
