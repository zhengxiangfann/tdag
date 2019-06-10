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


class Xwtransbyskubdist220190421sprsize2v11a(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.xw_trans_by_SKU_b_dist2_20190421_spr_size2_v11a;
CREATE TABLE belle_sh.xw_trans_by_SKU_b_dist2_20190421_spr_size2_v11a AS
SELECT
*
,(CASE
  WHEN ind_clear=1
  THEN 0
  WHEN ind_no_out=1
  THEN inv_end_size2_store
  WHEN sum_out_size2_clear>cnt_size2_require_score AND (ind_priority_inv>0 OR sum_qty_1week_store>0)
  THEN greatest(inv_priority_size2,inv_size2_from_score_in,inv_end_size2_store)
  WHEN sum_out_size2_clear>cnt_size2_require_score AND sum_qty_1week_store<=0 AND ind_priority_inv<=0 AND cnt_main_size1 >= 3
  THEN inv_end_size2_store
  WHEN sum_out_size2_clear>cnt_size2_require_score AND sum_qty_1week_store<=0 AND ind_priority_inv<=0 AND cnt_main_size1 < 3
  THEN inv_size2_from_score_in
  WHEN sum_out_size2_clear<=cnt_size2_require_score AND cnt_size2_require_score<=cum_out_extra_size2_available AND inv_extra_size2_store>0
  THEN inv_end_size2_store-if(cum_out_extra_size2_lag1>=cnt_size2_require_score,0,least(cnt_size2_require_score-cum_out_extra_size2_lag1,inv_extra_size2_store))
  WHEN sum_out_size2_clear<=cnt_size2_require_score AND cnt_size2_require_score<=cum_out_extra_size2_available AND inv_extra_size2_store<=0
  THEN if(ind_priority_inv>0 OR sum_qty_1week_store>0,greatest(inv_priority_size2,inv_size2_from_score_in),inv_end_size2_store)
  WHEN cnt_size2_require_score>cum_out_extra_size2_available AND sum_qty_1week_store<=0 AND ind_priority_inv<=0
  THEN if(cum_out_size2_lag1>=cnt_size2_require_score-cum_out_extra_size2_available,least(1,inv_end_size2_store),0)
  WHEN cnt_size2_require_score>cum_out_extra_size2_available AND (ind_priority_inv>0 OR sum_qty_1week_store>0) AND inv_extra_size2_store>0
  THEN greatest(inv_priority_size2,inv_size2_from_score_in)
  WHEN cnt_size2_require_score>cum_out_extra_size2_available AND (ind_priority_inv>0 OR sum_qty_1week_store>0) AND inv_extra_size2_store<=0
  THEN inv_end_size2_store+if(cum_in_size2_lag1>=cum_out_size2_available,0,least(cum_out_size2_available-cum_in_size2_lag1,greatest(inv_priority_size2,inv_size2_from_score_in)-inv_end_size2_store))
  ELSE -99
  END
 ) AS inv_size2_from_score_b
FROM belle_sh.xw_trans_by_SKU_b_dist1_20190421_spr_size2_v11a
;
SET hive.support.quoted.identifiers=NONE;"""
        self.call()

if __name__ == '__main__':
    Xwtransbyskubdist220190421sprsize2v11a().run_command()
