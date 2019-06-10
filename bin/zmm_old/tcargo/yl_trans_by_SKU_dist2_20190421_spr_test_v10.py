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


class Yltransbyskudist220190421sprtestv10(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_dist2_20190421_spr_test_v10;
CREATE TABLE belle_sh.yl_trans_by_SKU_dist2_20190421_spr_test_v10 AS
SELECT
a.*
,inv_end_220_total-new_sum_inv_220_no_out-sum(inv_220_from_score-nvl(greatest(ind_last_week,ind_no_out),0)*least(inv_end_220_store,inv_220_from_score)) OVER (PARTITION BY product_code ORDER BY nvl(ind_priority_inv,0)*nvl(inv_priority_220,0) DESC,if(days_no_sale_4w=28,1,0),final_score_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_220_inv_left
,inv_end_225_total-new_sum_inv_225_no_out-sum(inv_225_from_score-nvl(greatest(ind_last_week,ind_no_out),0)*least(inv_end_225_store,inv_225_from_score)) OVER (PARTITION BY product_code ORDER BY nvl(ind_priority_inv,0)*nvl(inv_priority_225,0) DESC,if(days_no_sale_4w=28,1,0),final_score_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_225_inv_left
,inv_end_230_total-new_sum_inv_230_no_out-sum(inv_230_from_score-nvl(greatest(ind_last_week,ind_no_out),0)*least(inv_end_230_store,inv_230_from_score)) OVER (PARTITION BY product_code ORDER BY nvl(ind_priority_inv,0)*nvl(inv_priority_230,0) DESC,if(days_no_sale_4w=28,1,0),final_score_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_230_inv_left
,inv_end_235_total-new_sum_inv_235_no_out-sum(inv_235_from_score-nvl(greatest(ind_last_week,ind_no_out),0)*least(inv_end_235_store,inv_235_from_score)) OVER (PARTITION BY product_code ORDER BY nvl(ind_priority_inv,0)*nvl(inv_priority_235,0) DESC,if(days_no_sale_4w=28,1,0),final_score_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_235_inv_left
,inv_end_240_total-new_sum_inv_240_no_out-sum(inv_240_from_score-nvl(greatest(ind_last_week,ind_no_out),0)*least(inv_end_240_store,inv_240_from_score)) OVER (PARTITION BY product_code ORDER BY nvl(ind_priority_inv,0)*nvl(inv_priority_240,0) DESC,if(days_no_sale_4w=28,1,0),final_score_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_240_inv_left
,inv_end_245_total-new_sum_inv_245_no_out-sum(inv_245_from_score-nvl(greatest(ind_last_week,ind_no_out),0)*least(inv_end_245_store,inv_245_from_score)) OVER (PARTITION BY product_code ORDER BY nvl(ind_priority_inv,0)*nvl(inv_priority_245,0) DESC,if(days_no_sale_4w=28,1,0),final_score_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_245_inv_left
,(CASE
       WHEN    (inv_end_store=0 AND sum_qty_1week_store=0
			   )
	   THEN 4
	   WHEN (days_no_sale>45 AND days_no_sale_4w=28)
	   THEN 3
       WHEN (ind_no_out<>1 AND ind_last_week<>1 AND store_level='C' AND final_score_rank>cnt_store_sku/2 AND sum_qty_2week_store<=ind_c_limit)
	   THEN 1
	   ELSE 2
  END
 ) AS ind_dist_0
,if(store_no='I014ST' AND inv_220_from_score>=2,1,0) AS ind_i014st_from_score_220
,if(store_no='I014ST' AND inv_225_from_score>=2,1,0) AS ind_i014st_from_score_225
,if(store_no='I014ST' AND inv_230_from_score>=2,1,0) AS ind_i014st_from_score_230
,if(store_no='I014ST' AND inv_235_from_score>=2,1,0) AS ind_i014st_from_score_235
,if(store_no='I014ST' AND inv_240_from_score>=2,1,0) AS ind_i014st_from_score_240
,if(store_no='I014ST' AND inv_245_from_score>=2,1,0) AS ind_i014st_from_score_245
FROM belle_sh.yl_trans_by_SKU_ad_no_out_test_v10 a
;"""
        self.call()

if __name__ == '__main__':
    Yltransbyskudist220190421sprtestv10().run_command()
