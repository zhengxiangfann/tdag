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


class Yltransbyskudist2temp1stv10(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_dist2_temp_1st_v10;
CREATE TABLE belle_sh.yl_trans_by_SKU_dist2_temp_1st_v10 AS
SELECT
*
,if(inv_225_from_score_1st
   +inv_230_from_score_1st
   +inv_235_from_score_1st
   +inv_240_from_score_1st>=6
   ,1
   ,0) AS ind_main_size_6plus
, inv_220_from_score_1st
 +inv_225_from_score_1st
 +inv_230_from_score_1st
 +inv_235_from_score_1st
 +inv_240_from_score_1st
 +inv_245_from_score_1st
 AS sum_inv_from_score_1st
FROM
(
SELECT
*
,(CASE WHEN cum_220_inv_left>=0
       THEN inv_220_from_score
	   WHEN cum_220_inv_left<0 AND cum_220_inv_left_lag1>0
	   THEN if(ind_no_out=1 OR ind_last_week=1,least(inv_end_220_store,inv_220_from_score)+cum_220_inv_left_lag1,cum_220_inv_left_lag1)
	   ELSE if(ind_no_out=1 OR ind_last_week=1,least(inv_end_220_store,inv_220_from_score),0)
  END
) AS inv_220_from_score_1st
,(CASE WHEN cum_225_inv_left>=0
       THEN inv_225_from_score
	   WHEN cum_225_inv_left<0 AND cum_225_inv_left_lag1>0
	   THEN if(ind_no_out=1 OR ind_last_week=1,least(inv_end_225_store,inv_225_from_score)+cum_225_inv_left_lag1,cum_225_inv_left_lag1)
	   ELSE if(ind_no_out=1 OR ind_last_week=1,least(inv_end_225_store,inv_225_from_score),0)
  END
) AS inv_225_from_score_1st
,(CASE WHEN cum_230_inv_left>=0
       THEN inv_230_from_score
	   WHEN cum_230_inv_left<0 AND cum_230_inv_left_lag1>0
	   THEN if(ind_no_out=1 OR ind_last_week=1,least(inv_end_230_store,inv_230_from_score)+cum_230_inv_left_lag1,cum_230_inv_left_lag1)
	   ELSE if(ind_no_out=1 OR ind_last_week=1,least(inv_end_230_store,inv_230_from_score),0)
  END
) AS inv_230_from_score_1st
,(CASE WHEN cum_235_inv_left>=0
       THEN inv_235_from_score
	   WHEN cum_235_inv_left<0 AND cum_235_inv_left_lag1>0
	   THEN if(ind_no_out=1 OR ind_last_week=1,least(inv_end_235_store,inv_235_from_score)+cum_235_inv_left_lag1,cum_235_inv_left_lag1)
	   ELSE if(ind_no_out=1 OR ind_last_week=1,least(inv_end_235_store,inv_235_from_score),0)
  END
) AS inv_235_from_score_1st
,(CASE WHEN cum_240_inv_left>=0
       THEN inv_240_from_score
	   WHEN cum_240_inv_left<0 AND cum_240_inv_left_lag1>0
	   THEN if(ind_no_out=1 OR ind_last_week=1,least(inv_end_240_store,inv_240_from_score)+cum_240_inv_left_lag1,cum_240_inv_left_lag1)
	   ELSE if(ind_no_out=1 OR ind_last_week=1,least(inv_end_240_store,inv_240_from_score),0)
  END
) AS inv_240_from_score_1st
,(CASE WHEN cum_245_inv_left>=0
       THEN inv_245_from_score
	   WHEN cum_245_inv_left<0 AND cum_245_inv_left_lag1>0
	   THEN if(ind_no_out=1 OR ind_last_week=1,least(inv_end_245_store,inv_245_from_score)+cum_245_inv_left_lag1,cum_245_inv_left_lag1)
	   ELSE if(ind_no_out=1 OR ind_last_week=1,least(inv_end_245_store,inv_245_from_score),0)
  END
) AS inv_245_from_score_1st
FROM belle_sh.yl_trans_by_SKU_dist2_temp_v10
) a
;"""
        self.call()

if __name__ == '__main__':
    Yltransbyskudist2temp1stv10().run_command()
