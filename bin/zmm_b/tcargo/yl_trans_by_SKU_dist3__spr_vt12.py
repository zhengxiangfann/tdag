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

class Yltransbyskudist3sprvt12(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_dist3_{invsunday}_spr_vt12;
CREATE TABLE belle_sh.yl_trans_by_SKU_dist3_{invsunday}_spr_vt12 AS
SELECT
a.*
,(CASE WHEN min_cum_220_inv_left<=0
       THEN inv_220_from_score_1st
       ELSE if(rank_2nd_round_220<=min_cum_220_inv_left AND ind_clear=0 AND ind_no_in=0,inv_220_from_score_1st+1,inv_220_from_score_1st)
  END
) AS inv_220_from_score_final
,(CASE WHEN min_cum_225_inv_left<=0
       THEN inv_225_from_score_1st
       ELSE if(rank_2nd_round_225<=min_cum_225_inv_left AND ind_clear=0 AND ind_no_in=0,inv_225_from_score_1st+1,inv_225_from_score_1st)
  END
) AS inv_225_from_score_final
,(CASE WHEN min_cum_230_inv_left<=0
       THEN inv_230_from_score_1st
       ELSE if(rank_2nd_round_230<=min_cum_230_inv_left AND ind_clear=0 AND ind_no_in=0,inv_230_from_score_1st+1,inv_230_from_score_1st)
  END
) AS inv_230_from_score_final
,(CASE WHEN min_cum_235_inv_left<=0
       THEN inv_235_from_score_1st
       ELSE if(rank_2nd_round_235<=min_cum_235_inv_left AND ind_clear=0 AND ind_no_in=0,inv_235_from_score_1st+1,inv_235_from_score_1st)
  END
) AS inv_235_from_score_final
,(CASE WHEN min_cum_240_inv_left<=0
       THEN inv_240_from_score_1st
       ELSE if(rank_2nd_round_240<=min_cum_240_inv_left AND ind_clear=0 AND ind_no_in=0,inv_240_from_score_1st+1,inv_240_from_score_1st)
  END
) AS inv_240_from_score_final
,(CASE WHEN min_cum_245_inv_left<=0
       THEN inv_245_from_score_1st
       ELSE if(rank_2nd_round_245<=min_cum_245_inv_left AND ind_clear=0 AND ind_no_in=0,inv_245_from_score_1st+1,inv_245_from_score_1st)
  END
) AS inv_245_from_score_final
FROM belle_sh.yl_trans_by_SKU_temp_vt12 a
LEFT JOIN
(
SELECT
product_code
,min(cum_220_inv_left) AS min_cum_220_inv_left
,min(cum_225_inv_left) AS min_cum_225_inv_left
,min(cum_230_inv_left) AS min_cum_230_inv_left
,min(cum_235_inv_left) AS min_cum_235_inv_left
,min(cum_240_inv_left) AS min_cum_240_inv_left
,min(cum_245_inv_left) AS min_cum_245_inv_left
FROM belle_sh.yl_trans_by_SKU_dist2_{invsunday}_spr_test_vt12
GROUP BY
product_code
) b
ON a.product_code=b.product_code
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Yltransbyskudist3sprvt12().run_command()
