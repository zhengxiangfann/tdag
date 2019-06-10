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

class Xwtransbyskubdist2sprsize4vt12(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.xw_trans_by_SKU_b_dist2_{invsunday}_spr_size4_vt12;
CREATE TABLE belle_sh.xw_trans_by_SKU_b_dist2_{invsunday}_spr_size4_vt12 AS
SELECT
*
,(CASE
  WHEN ind_clear=1
  THEN 0
  WHEN sum_out_size4_clear > cnt_size4_require_score AND (ind_priority_inv>0 OR sum_qty_1week_store>0 OR ind_mainsize_require = 0)
  THEN greatest(inv_priority_size4,inv_size4_from_score_in,inv_end_size4_store)
  WHEN sum_out_size4_clear>cnt_size4_require_score AND sum_qty_1week_store<=0 AND ind_priority_inv<=0 AND cnt_main_size3 >= 3
  THEN inv_end_size4_store
  WHEN sum_out_size4_clear>cnt_size4_require_score AND sum_qty_1week_store<=0 AND ind_priority_inv<=0 AND cnt_main_size3 <  3
  THEN inv_size4_from_score_in
  WHEN sum_out_size4_clear<=cnt_size4_require_score AND cnt_size4_require_score<=cum_out_extra_size4_available AND inv_extra_size4_store>0
  THEN inv_end_size4_store-if(cum_out_extra_size4_lag1>=cnt_size4_require_score,0,least(cnt_size4_require_score-cum_out_extra_size4_lag1,inv_extra_size4_store))
  WHEN sum_out_size4_clear<=cnt_size4_require_score AND cnt_size4_require_score<=cum_out_extra_size4_available AND inv_extra_size4_store<=0
  THEN if(ind_priority_inv>0 OR sum_qty_1week_store>0 OR ind_mainsize_require = 0,greatest(inv_priority_size4,inv_size4_from_score_in),inv_end_size4_store)
  WHEN cnt_size4_require_score>cum_out_extra_size4_available AND cnt_size4_require_score<=(cum_out_extra_size4_available+ cum_out_noneed_size4_available)
	AND (inv_noneed_size4_store + inv_extra_size4_store) >0 AND ind_priority_inv<=0 AND sum_qty_1week_store<=0
  THEN if(cum_noneed_size4_lag1 >= (cnt_size4_require_score-cum_out_extra_size4_available),least(1,inv_end_size4_store),0)
  WHEN cnt_size4_require_score>cum_out_extra_size4_available AND cnt_size4_require_score<=(cum_out_extra_size4_available+ cum_out_noneed_size4_available)
       AND (ind_priority_inv>0 OR sum_qty_1week_store>0 OR ind_mainsize_require = 0) AND (inv_noneed_size4_store + inv_extra_size4_store) >0
  THEN greatest(inv_priority_size4,inv_size4_from_score_in)
  WHEN cnt_size4_require_score>cum_out_extra_size4_available AND cnt_size4_require_score<=(cum_out_extra_size4_available+ cum_out_noneed_size4_available)
       AND (ind_priority_inv>0 OR sum_qty_1week_store>0 OR ind_mainsize_require = 0) AND (inv_noneed_size4_store + inv_extra_size4_store) <= 0
  THEN inv_end_size4_store+if(cum_in_size4_lag1 >=(cum_out_extra_size4_available + cum_out_noneed_size4_available ),0,least(cum_out_extra_size4_available + cum_out_noneed_size4_available - cum_in_size4_lag1,greatest(inv_priority_size4,inv_size4_from_score_in)-inv_end_size4_store))
  WHEN cnt_size4_require_score>cum_out_extra_size4_available AND cnt_size4_require_score<=(cum_out_extra_size4_available+ cum_out_noneed_size4_available)
       AND ind_mainsize_require > 0 AND (inv_noneed_size4_store + inv_extra_size4_store) <= 0
  THEN inv_size4_from_score_in
  WHEN cnt_size4_require_score>(cum_out_extra_size4_available + cum_out_noneed_size4_available) AND cum_out_noneed_size4_available > 0
       AND (ind_priority_inv>0 OR sum_qty_1week_store>0 OR ind_mainsize_require = 0) AND (inv_noneed_size4_store + inv_extra_size4_store) <= 0
	   AND (cum_noneed_size4 + cum_out_extra_size4_available) > cum_in_size4
  THEN inv_end_size4_store+if(cum_in_size4_lag1 >=(cum_out_extra_size4_available + cum_out_noneed_size4_available ),0,least(cum_out_extra_size4_available + cum_out_noneed_size4_available - cum_in_size4_lag1,greatest(inv_priority_size4,inv_size4_from_score_in)-inv_end_size4_store))
  WHEN cnt_size4_require_score> (cum_out_extra_size4_available+cum_out_noneed_size4_available) AND sum_qty_1week_store<=0 AND ind_priority_inv<=0
  THEN if(cum_out_size4_lag1>=(cnt_size4_require_score-cum_out_extra_size4_available),least(1,inv_end_size4_store),0)
  WHEN cnt_size4_require_score> (cum_out_extra_size4_available+cum_out_noneed_size4_available) AND (ind_priority_inv>0 OR sum_qty_1week_store>0) AND inv_extra_size4_store>0
  THEN greatest(inv_priority_size4,inv_size4_from_score_in)
  WHEN cnt_size4_require_score>(cum_out_extra_size4_available+cum_out_noneed_size4_available) AND (ind_priority_inv>0 OR sum_qty_1week_store>0) AND inv_extra_size4_store<=0
  THEN inv_end_size4_store+if(cum_in_size4_lag1>=cum_out_size4_available,0,least(cum_out_size4_available-cum_in_size4_lag1,greatest(inv_priority_size4,inv_size4_from_score_in)-inv_end_size4_store))
  ELSE -99
  END
 ) AS inv_size4_from_score_b
FROM belle_sh.xw_trans_by_SKU_b_dist1_{invsunday}_spr_size4_vt12
;
SET hive.support.quoted.identifiers=NONE;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Xwtransbyskubdist2sprsize4vt12().run_command()
