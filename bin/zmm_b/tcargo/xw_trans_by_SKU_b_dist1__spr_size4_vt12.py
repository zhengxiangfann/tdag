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

class Xwtransbyskubdist1sprsize4vt12(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.xw_trans_by_SKU_b_dist1_{invsunday}_spr_size4_vt12;
CREATE TABLE belle_sh.xw_trans_by_SKU_b_dist1_{invsunday}_spr_size4_vt12 AS
SELECT
 a.product_code
,b.size_no
,a.store_no
,a.ind_clear
,a.ind_priority_inv
,a.ind_no_out
,a.sum_qty_1week_store
,a.inv_end_225_total AS inv_end_size4_total
,a.sum_qty_2week_225 AS sum_qty_2week_size4
,a.inv_end_225_store AS inv_end_size4_store
,a.inv_end_225_i0215 AS inv_end_size4_i0215
,a.sum_inv_225_no_out AS sum_inv_size4_no_out
,a.inv_priority_225 AS inv_priority_size4
,a.inv_225_from_score AS inv_size4_from_score
,a.inv_225_from_score_in AS inv_size4_from_score_in
,a.new_sum_inv_225_no_out AS new_sum_inv_size4_no_out
,a.cum_225_inv_left AS cum_size4_inv_left
,a.ind_i014st_from_score_225 AS ind_i014st_from_score_size4
,a.cnt_225_require_score_3 AS cnt_size4_require_score
,a.cum_in_225_3 AS cum_in_size4
,a.cum_in_225_lag1_3 AS cum_in_size4_lag1
,a.sum_out_225_clear AS sum_out_size4_clear
,a.cum_out_225_available AS cum_out_size4_available
,a.inv_extra_225_store AS inv_extra_size4_store
,a.cum_out_noneed_225_available AS cum_out_noneed_size4_available
,a.inv_noneed_225_store AS inv_noneed_size4_store
,a.rank_out_225 AS rank_out_size4
,a.cum_out_extra_225 AS cum_out_extra_size4
,a.cum_out_extra_225_lag1 AS cum_out_extra_size4_lag1
,a.cum_out_extra_225_available AS cum_out_extra_size4_available
,a.cum_out_225 AS cum_out_size4
,a.cum_out_225_lag1 AS cum_out_size4_lag1
,a.cum_noneed_225 AS cum_noneed_size4
,a.cum_noneed_225_lag1 AS cum_noneed_size4_lag1
,a.cnt_main_size3
,a.ind_mainsize_require
FROM belle_sh.xw_trans_by_SKU_b_spr_prepare2_{invsunday}_size3_vt12 a
LEFT JOIN belle_sh.yl_trans_by_SKU_size_rank_{invsunday}_spr_vt12 b
ON a.product_code=b.product_code
WHERE rank_size=4 AND size_no=225
UNION ALL
SELECT
 a.product_code
,b.size_no
,a.store_no
,a.ind_clear
,a.ind_priority_inv
,a.ind_no_out
,a.sum_qty_1week_store
,a.inv_end_230_total AS inv_end_size4_total
,a.sum_qty_2week_230 AS sum_qty_2week_size4
,a.inv_end_230_store AS inv_end_size4_store
,a.inv_end_230_i0215 AS inv_end_size4_i0215
,a.sum_inv_230_no_out AS sum_inv_size4_no_out
,a.inv_priority_230 AS inv_priority_size4
,a.inv_230_from_score AS inv_size4_from_score
,a.inv_230_from_score_in AS inv_size4_from_score_in
,a.new_sum_inv_230_no_out AS new_sum_inv_size4_no_out
,a.cum_230_inv_left AS cum_size4_inv_left
,a.ind_i014st_from_score_230 AS ind_i014st_from_score_size4
,a.cnt_230_require_score_3 AS cnt_size4_require_score
,a.cum_in_230_3 AS cum_in_size4
,a.cum_in_230_lag1_3 AS cum_in_size4_lag1
,a.sum_out_230_clear AS sum_out_size4_clear
,a.cum_out_230_available AS cum_out_size4_available
,a.inv_extra_230_store AS inv_extra_size4_store
,a.cum_out_noneed_230_available AS cum_out_noneed_size4_available
,a.inv_noneed_230_store AS inv_noneed_size4_store
,a.rank_out_230 AS rank_out_size4
,a.cum_out_extra_230 AS cum_out_extra_size4
,a.cum_out_extra_230_lag1 AS cum_out_extra_size4_lag1
,a.cum_out_extra_230_available AS cum_out_extra_size4_available
,a.cum_out_230 AS cum_out_size4
,a.cum_out_230_lag1 AS cum_out_size4_lag1
,a.cum_noneed_230 AS cum_noneed_size4
,a.cum_noneed_230_lag1 AS cum_noneed_size4_lag1
,a.cnt_main_size3
,a.ind_mainsize_require
FROM belle_sh.xw_trans_by_SKU_b_spr_prepare2_{invsunday}_size3_vt12 a
LEFT JOIN belle_sh.yl_trans_by_SKU_size_rank_{invsunday}_spr_vt12 b
ON a.product_code=b.product_code
WHERE rank_size=4 AND size_no=230
UNION ALL
SELECT
 a.product_code
,b.size_no
,a.store_no
,a.ind_clear
,a.ind_priority_inv
,a.ind_no_out
,a.sum_qty_1week_store
,a.inv_end_235_total AS inv_end_size4_total
,a.sum_qty_2week_235 AS sum_qty_2week_size4
,a.inv_end_235_store AS inv_end_size4_store
,a.inv_end_235_i0215 AS inv_end_size4_i0215
,a.sum_inv_235_no_out AS sum_inv_size4_no_out
,a.inv_priority_235 AS inv_priority_size4
,a.inv_235_from_score AS inv_size4_from_score
,a.inv_235_from_score_in AS inv_size4_from_score_in
,a.new_sum_inv_235_no_out AS new_sum_inv_size4_no_out
,a.cum_235_inv_left AS cum_size4_inv_left
,a.ind_i014st_from_score_235 AS ind_i014st_from_score_size4
,a.cnt_235_require_score_3 AS cnt_size4_require_score
,a.cum_in_235_3 AS cum_in_size4
,a.cum_in_235_lag1_3 AS cum_in_size4_lag1
,a.sum_out_235_clear AS sum_out_size4_clear
,a.cum_out_235_available AS cum_out_size4_available
,a.inv_extra_235_store AS inv_extra_size4_store
,a.cum_out_noneed_235_available AS cum_out_noneed_size4_available
,a.inv_noneed_235_store AS inv_noneed_size4_store
,a.rank_out_235 AS rank_out_size4
,a.cum_out_extra_235 AS cum_out_extra_size4
,a.cum_out_extra_235_lag1 AS cum_out_extra_size4_lag1
,a.cum_out_extra_235_available AS cum_out_extra_size4_available
,a.cum_out_235 AS cum_out_size4
,a.cum_out_235_lag1 AS cum_out_size4_lag1
,a.cum_noneed_235 AS cum_noneed_size4
,a.cum_noneed_235_lag1 AS cum_noneed_size4_lag1
,a.cnt_main_size3
,a.ind_mainsize_require
FROM belle_sh.xw_trans_by_SKU_b_spr_prepare2_{invsunday}_size3_vt12 a
LEFT JOIN belle_sh.yl_trans_by_SKU_size_rank_{invsunday}_spr_vt12 b
ON a.product_code=b.product_code
WHERE rank_size=4 AND size_no=235
UNION ALL
SELECT
 a.product_code
,b.size_no
,a.store_no
,a.ind_clear
,a.ind_priority_inv
,a.ind_no_out
,a.sum_qty_1week_store
,a.inv_end_240_total AS inv_end_size4_total
,a.sum_qty_2week_240 AS sum_qty_2week_size4
,a.inv_end_240_store AS inv_end_size4_store
,a.inv_end_240_i0215 AS inv_end_size4_i0215
,a.sum_inv_240_no_out AS sum_inv_size4_no_out
,a.inv_priority_240 AS inv_priority_size4
,a.inv_240_from_score AS inv_size4_from_score
,a.inv_240_from_score_in AS inv_size4_from_score_in
,a.new_sum_inv_240_no_out AS new_sum_inv_size4_no_out
,a.cum_240_inv_left AS cum_size4_inv_left
,a.ind_i014st_from_score_240 AS ind_i014st_from_score_size4
,a.cnt_240_require_score_3 AS cnt_size4_require_score
,a.cum_in_240_3 AS cum_in_size4
,a.cum_in_240_lag1_3 AS cum_in_size4_lag1
,a.sum_out_240_clear AS sum_out_size4_clear
,a.cum_out_240_available AS cum_out_size4_available
,a.inv_extra_240_store AS inv_extra_size4_store
,a.cum_out_noneed_240_available AS cum_out_noneed_size4_available
,a.inv_noneed_240_store AS inv_noneed_size4_store
,a.rank_out_240 AS rank_out_size4
,a.cum_out_extra_240 AS cum_out_extra_size4
,a.cum_out_extra_240_lag1 AS cum_out_extra_size4_lag1
,a.cum_out_extra_240_available AS cum_out_extra_size4_available
,a.cum_out_240 AS cum_out_size4
,a.cum_out_240_lag1 AS cum_out_size4_lag1
,a.cum_noneed_240 AS cum_noneed_size4
,a.cum_noneed_240_lag1 AS cum_noneed_size4_lag1
,a.cnt_main_size3
,a.ind_mainsize_require
FROM belle_sh.xw_trans_by_SKU_b_spr_prepare2_{invsunday}_size3_vt12 a
LEFT JOIN belle_sh.yl_trans_by_SKU_size_rank_{invsunday}_spr_vt12 b
ON a.product_code=b.product_code
WHERE rank_size=4 AND size_no=240
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Xwtransbyskubdist1sprsize4vt12().run_command()
