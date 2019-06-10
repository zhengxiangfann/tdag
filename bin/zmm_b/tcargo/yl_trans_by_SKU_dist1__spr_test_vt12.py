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

class Yltransbyskudist1sprtestvt12(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_dist1_{invsunday}_spr_test_vt12;
CREATE TABLE belle_sh.yl_trans_by_SKU_dist1_{invsunday}_spr_test_vt12 AS
SELECT
a.*
,nvl(
(CASE WHEN ind_priority_inv*inv_priority_220>0
       THEN inv_priority_220
       WHEN    (inv_end_store=0 AND sum_qty_1week_store=0
			   )
            OR (days_no_sale>45 AND days_no_sale_4w=28)
			OR (ind_no_out<>1 AND ind_last_week<>1 AND store_level='C' AND final_score_rank>cnt_store_sku/2 AND sum_qty_2week_store<=ind_c_limit)
			OR inv_end_220_total=0
			OR ind_clear=1
	   THEN 0
       WHEN ind_no_out=1 OR ind_last_week=1
	   THEN if(sum_qty_2week_store>=2 OR (sum_qty_2week_store=1 AND inv_end_225_store+inv_end_230_store+inv_end_235_store+inv_end_240_store<=2)
	          ,least(if(ind_no_in=1,inv_end_220_store,99),1,greatest(inv_end_220_store,round(inv_end_220_total*final_score_pct,0),1))
              ,inv_end_220_store
			  )
	   ELSE least(if(ind_no_in=1,inv_end_220_store,99),1,greatest(round(inv_end_220_total*final_score_pct,0),1))
  END
 ),0) AS inv_220_from_score
,nvl(
(CASE WHEN ind_priority_inv*inv_priority_225>0
       THEN inv_priority_225
       WHEN    (inv_end_store=0 AND sum_qty_1week_store=0
			   )
            OR (days_no_sale>45 AND days_no_sale_4w=28)
			OR (ind_no_out<>1 AND ind_last_week<>1 AND store_level='C' AND final_score_rank>cnt_store_sku/2 AND sum_qty_2week_store<=ind_c_limit)
			OR inv_end_225_total=0
			OR ind_clear=1
	   THEN 0
       WHEN ind_no_out=1 OR ind_last_week=1
	   THEN if(sum_qty_2week_store>=2 OR (sum_qty_2week_store=1 AND inv_end_225_store+inv_end_230_store+inv_end_235_store+inv_end_240_store<=2)
	          ,least(if(ind_no_in=1,inv_end_225_store,99),upperlimit,greatest(inv_end_225_store,round(inv_end_225_total*final_score_pct,0),1))
              ,inv_end_225_store
			  )
	   ELSE least(if(ind_no_in=1,inv_end_225_store,99),upperlimit,greatest(round(inv_end_225_total*final_score_pct,0),1))
  END
 ),0) AS inv_225_from_score
,nvl(
(CASE WHEN ind_priority_inv*inv_priority_230>0
       THEN inv_priority_230
       WHEN    (inv_end_store=0 AND sum_qty_1week_store=0
			   )
            OR (days_no_sale>45 AND days_no_sale_4w=28)
			OR (ind_no_out<>1 AND ind_last_week<>1 AND store_level='C' AND final_score_rank>cnt_store_sku/2 AND sum_qty_2week_store<=ind_c_limit)
			OR inv_end_230_total=0
			OR ind_clear=1
	   THEN 0
       WHEN ind_no_out=1 OR ind_last_week=1
	   THEN if(sum_qty_2week_store>=2 OR (sum_qty_2week_store=1 AND inv_end_225_store+inv_end_230_store+inv_end_235_store+inv_end_240_store<=2)
	          ,least(if(ind_no_in=1,inv_end_230_store,99),upperlimit,greatest(inv_end_230_store,round(inv_end_230_total*final_score_pct,0),1))
              ,inv_end_230_store
			  )
	   ELSE least(if(ind_no_in=1,inv_end_230_store,99),upperlimit,greatest(round(inv_end_230_total*final_score_pct,0),1))
  END
 ),0) AS inv_230_from_score
,nvl(
(CASE WHEN ind_priority_inv*inv_priority_235>0
       THEN inv_priority_235
       WHEN    (inv_end_store=0 AND sum_qty_1week_store=0
			   )
            OR (days_no_sale>45 AND days_no_sale_4w=28)
			OR (ind_no_out<>1 AND ind_last_week<>1 AND store_level='C' AND final_score_rank>cnt_store_sku/2 AND sum_qty_2week_store<=ind_c_limit)
			OR inv_end_235_total=0
			OR ind_clear=1
	   THEN 0
       WHEN ind_no_out=1 OR ind_last_week=1
	   THEN if(sum_qty_2week_store>=2 OR (sum_qty_2week_store=1 AND inv_end_225_store+inv_end_230_store+inv_end_235_store+inv_end_240_store<=2)
	          ,least(if(ind_no_in=1,inv_end_235_store,99),upperlimit,greatest(inv_end_235_store,round(inv_end_235_total*final_score_pct,0),1))
              ,inv_end_235_store
			  )
	   ELSE least(if(ind_no_in=1,inv_end_235_store,99),upperlimit,greatest(round(inv_end_235_total*final_score_pct,0),1))
  END
 ),0) AS inv_235_from_score
,nvl(
(CASE WHEN ind_priority_inv*inv_priority_240>0
       THEN inv_priority_240
       WHEN    (inv_end_store=0 AND sum_qty_1week_store=0
			   )
            OR (days_no_sale>45 AND days_no_sale_4w=28)
			OR (ind_no_out<>1 AND ind_last_week<>1 AND store_level='C' AND final_score_rank>cnt_store_sku/2 AND sum_qty_2week_store<=ind_c_limit)
			OR inv_end_240_total=0
			OR ind_clear=1
	   THEN 0
       WHEN ind_no_out=1 OR ind_last_week=1
	   THEN if(sum_qty_2week_store>=2 OR (sum_qty_2week_store=1 AND inv_end_225_store+inv_end_230_store+inv_end_235_store+inv_end_240_store<=2)
	          ,least(if(ind_no_in=1,inv_end_240_store,99),upperlimit,greatest(inv_end_240_store,round(inv_end_240_total*final_score_pct,0),1))
              ,inv_end_240_store
			  )
	   ELSE least(if(ind_no_in=1,inv_end_240_store,99),upperlimit,greatest(round(inv_end_240_total*final_score_pct,0),1))
  END
 ),0) AS inv_240_from_score
,nvl(
(CASE WHEN ind_priority_inv*inv_priority_245>0
       THEN inv_priority_245
       WHEN    (inv_end_store=0 AND sum_qty_1week_store=0
			   )
            OR (days_no_sale>45 AND days_no_sale_4w=28)
			OR (ind_no_out<>1 AND ind_last_week<>1 AND store_level='C' AND final_score_rank>cnt_store_sku/2 AND sum_qty_2week_store<=ind_c_limit)
			OR inv_end_245_total=0
			OR ind_clear=1
	   THEN 0
	   ELSE inv_end_245_store
  END
 ),0) AS inv_245_from_score
FROM belle_sh.yl_trans_by_SKU_master{invsunday}_spr_test_vt12 a
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Yltransbyskudist1sprtestvt12().run_command()
