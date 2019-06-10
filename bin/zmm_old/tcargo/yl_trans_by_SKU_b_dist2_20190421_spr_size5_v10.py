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


class Yltransbyskubdist220190421sprsize5v10(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_b_dist2_20190421_spr_size5_v10;
CREATE TABLE belle_sh.yl_trans_by_SKU_b_dist2_20190421_spr_size5_v10 AS
SELECT
*
,(CASE
  WHEN ind_clear=1
  THEN 0
  WHEN ind_no_out=1
  THEN inv_end_220_store
  WHEN sum_out_220_clear>cnt_220_require_score AND (ind_priority_inv>0 OR sum_qty_1week_store>0)
  THEN greatest(inv_priority_220,inv_220_from_score_v2,inv_end_220_store)
  WHEN sum_out_220_clear>cnt_220_require_score AND sum_qty_1week_store<=0 AND ind_priority_inv<=0
  THEN inv_end_220_store
  WHEN sum_out_220_clear<=cnt_220_require_score AND cnt_220_require_score<=cum_out_extra_220_available AND inv_extra_220_store>0
  THEN inv_end_220_store-if(cum_out_extra_220_lag1>=cnt_220_require_score,0,least(cnt_220_require_score-cum_out_extra_220_lag1,inv_extra_220_store))
  WHEN sum_out_220_clear<=cnt_220_require_score AND cnt_220_require_score<=cum_out_extra_220_available AND inv_extra_220_store<=0
  THEN if(ind_priority_inv>0 OR sum_qty_1week_store>0,greatest(inv_priority_220,inv_220_from_score_v2),inv_end_220_store)
  WHEN cnt_220_require_score>cum_out_extra_220_available AND sum_qty_1week_store<=0 AND ind_priority_inv<=0
  THEN if(cum_out_220_lag1>=cnt_220_require_score-cum_out_extra_220_available,least(1,inv_end_220_store),0)
  WHEN cnt_220_require_score>cum_out_extra_220_available AND (ind_priority_inv>0 OR sum_qty_1week_store>0) AND inv_extra_220_store>0
  THEN greatest(inv_priority_220,inv_220_from_score_v2)
  WHEN cnt_220_require_score>cum_out_extra_220_available AND (ind_priority_inv>0 OR sum_qty_1week_store>0) AND inv_extra_220_store<=0
  THEN inv_end_220_store+if(cum_in_220_lag1>=cum_out_220_available,0,least(cum_out_220_available-cum_in_220_lag1,greatest(inv_priority_220,inv_220_from_score_v2)-inv_end_220_store))
  ELSE -99
  END
 ) AS inv_220_from_score_b
,inv_end_225_store+nvl(delta_size1_225,0)+nvl(delta_size2_225,0)+nvl(delta_size3_225,0)+nvl(delta_size4_225,0) AS inv_225_from_score_b
,inv_end_230_store+nvl(delta_size1_230,0)+nvl(delta_size2_230,0)+nvl(delta_size3_230,0)+nvl(delta_size4_230,0) AS inv_230_from_score_b
,inv_end_235_store+nvl(delta_size1_235,0)+nvl(delta_size2_235,0)+nvl(delta_size3_235,0)+nvl(delta_size4_235,0) AS inv_235_from_score_b
,inv_end_240_store+nvl(delta_size1_240,0)+nvl(delta_size2_240,0)+nvl(delta_size3_240,0)+nvl(delta_size4_240,0) AS inv_240_from_score_b
,(CASE
  WHEN ind_clear=1 OR cnt_main_size4=0
  THEN 0
  WHEN ind_no_out=1
  THEN inv_end_245_store
  WHEN sum_out_245_clear>cnt_245_require_score AND (ind_priority_inv>0 OR sum_qty_1week_store>0)
  THEN greatest(inv_priority_245,inv_245_from_score,inv_end_245_store)
  WHEN sum_out_245_clear>cnt_245_require_score AND sum_qty_1week_store<=0 AND ind_priority_inv<=0
  THEN inv_end_245_store
  WHEN sum_out_245_clear<=cnt_245_require_score AND cnt_245_require_score<=cum_out_extra_245_available AND inv_extra_245_store>0
  THEN inv_end_245_store-if(cum_out_extra_245_lag1>=cnt_245_require_score,0,least(cnt_245_require_score-cum_out_extra_245_lag1,inv_extra_245_store))
  WHEN sum_out_245_clear<=cnt_245_require_score AND cnt_245_require_score<=cum_out_extra_245_available AND inv_extra_245_store<=0
  THEN if(ind_priority_inv>0 OR sum_qty_1week_store>0,greatest(inv_priority_245,inv_245_from_score),inv_end_245_store)
  WHEN cnt_245_require_score>cum_out_extra_245_available AND sum_qty_1week_store<=0 AND ind_priority_inv<=0
  THEN if(cum_out_245_lag1>=cnt_245_require_score-cum_out_extra_245_available,least(1,inv_end_245_store),0)
  WHEN cnt_245_require_score>cum_out_extra_245_available AND (ind_priority_inv>0 OR sum_qty_1week_store>0) AND inv_extra_245_store>0
  THEN greatest(inv_priority_245,inv_245_from_score)
  WHEN cnt_245_require_score>cum_out_extra_245_available AND (ind_priority_inv>0 OR sum_qty_1week_store>0) AND inv_extra_245_store<=0
  THEN inv_end_245_store+if(cum_in_245_lag1>=cum_out_245_available,0,least(cum_out_245_available-cum_in_245_lag1,greatest(inv_priority_245,inv_245_from_score)-inv_end_245_store))
  ELSE -99
  END
 ) AS inv_245_from_score_b
FROM belle_sh.yl_trans_by_SKU_b_spr_prepare2_20190421_size4_v10
;"""
        self.call()

if __name__ == '__main__':
    Yltransbyskubdist220190421sprsize5v10().run_command()
