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


class Xwtransbyskubdist320190421sprv11a(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.xw_trans_by_SKU_b_dist3_20190421_spr_v11a;
CREATE TABLE belle_sh.xw_trans_by_SKU_b_dist3_20190421_spr_v11a AS
SELECT
a.*
,(CASE WHEN min_cum_220_inv_left_b<=0
       THEN inv_220_from_score_b
       ELSE if(rank_2nd_round_220_b<=min_cum_220_inv_left_b AND ind_clear=0,inv_220_from_score_b+1,inv_220_from_score_b)
  END
) AS inv_220_from_score_final_b
,(CASE WHEN min_cum_225_inv_left_b<=0
       THEN inv_225_from_score_b
       ELSE if(rank_2nd_round_225_b<=min_cum_225_inv_left_b AND ind_clear=0,inv_225_from_score_b+1,inv_225_from_score_b)
  END
) AS inv_225_from_score_final_b
,(CASE WHEN min_cum_230_inv_left_b<=0
       THEN inv_230_from_score_b
       ELSE if(rank_2nd_round_230_b<=min_cum_230_inv_left_b AND ind_clear=0,inv_230_from_score_b+1,inv_230_from_score_b)
  END
) AS inv_230_from_score_final_b
,(CASE WHEN min_cum_235_inv_left_b<=0
       THEN inv_235_from_score_b
       ELSE if(rank_2nd_round_235_b<=min_cum_235_inv_left_b AND ind_clear=0,inv_235_from_score_b+1,inv_235_from_score_b)
  END
) AS inv_235_from_score_final_b
,(CASE WHEN min_cum_240_inv_left_b<=0
       THEN inv_240_from_score_b
       ELSE if(rank_2nd_round_240_b<=min_cum_240_inv_left_b AND ind_clear=0,inv_240_from_score_b+1,inv_240_from_score_b)
  END
) AS inv_240_from_score_final_b
,(CASE WHEN min_cum_245_inv_left_b<=0
       THEN inv_245_from_score_b
       ELSE if(rank_2nd_round_245_b<=min_cum_245_inv_left_b AND ind_clear=0,inv_245_from_score_b+1,inv_245_from_score_b)
  END
) AS inv_245_from_score_final_b
FROM belle_sh.xw_trans_by_SKU_temp_b_v11a a
LEFT JOIN
         (
         SELECT
         product_code
         ,sum(inv_end_220_store-inv_220_from_score_b) AS min_cum_220_inv_left_b
         ,sum(inv_end_225_store-inv_225_from_score_b) AS min_cum_225_inv_left_b
         ,sum(inv_end_230_store-inv_230_from_score_b) AS min_cum_230_inv_left_b
         ,sum(inv_end_235_store-inv_235_from_score_b) AS min_cum_235_inv_left_b
         ,sum(inv_end_240_store-inv_240_from_score_b) AS min_cum_240_inv_left_b
         ,sum(inv_end_245_store-inv_245_from_score_b) AS min_cum_245_inv_left_b
         FROM belle_sh.xw_trans_by_SKU_temp_b_v11a
         GROUP BY
         product_code
         ) b
ON a.product_code=b.product_code
;"""
        self.call()

if __name__ == '__main__':
    Xwtransbyskubdist320190421sprv11a().run_command()
