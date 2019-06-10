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

class Xwtransbyskubsprpreparevt12(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.xw_trans_by_SKU_b_spr_prepare_{invsunday}_vt12;
CREATE TABLE belle_sh.xw_trans_by_SKU_b_spr_prepare_{invsunday}_vt12 AS
SELECT
*
,sum(if(sum_qty_1week_store>0 OR inv_priority_225>0 OR (inv_end_225_store < 1 AND cnt_main_size <= 2 AND inv_end_store > 0 AND days_no_sale<= 28 ), if(cnt_main_size_gt2 <= 1 AND inv_end_225_store <=0 , greatest(greatest(inv_priority_225,inv_225_from_score,inv_225_from_score_main)-inv_end_225_store,0), greatest(least(1,inv_225_from_score) - inv_end_225_store,0)),0)) over(PARTITION BY product_code) AS cnt_225_require_score
,sum(if(sum_qty_1week_store>0 OR inv_priority_230>0 OR (inv_end_230_store < 1 AND cnt_main_size <= 2 AND inv_end_store > 0 AND days_no_sale<= 28 ), if(cnt_main_size_gt2 <= 1 AND inv_end_230_store <=0 , greatest(greatest(inv_priority_230,inv_230_from_score,inv_230_from_score_main)-inv_end_230_store,0), greatest(least(1,inv_230_from_score) - inv_end_230_store,0)),0)) over(PARTITION BY product_code) AS cnt_230_require_score
,sum(if(sum_qty_1week_store>0 OR inv_priority_235>0 OR (inv_end_235_store < 1 AND cnt_main_size <= 2 AND inv_end_store > 0 AND days_no_sale<= 28 ), if(cnt_main_size_gt2 <= 1 AND inv_end_235_store <=0 , greatest(greatest(inv_priority_235,inv_235_from_score,inv_235_from_score_main)-inv_end_235_store,0), greatest(least(1,inv_235_from_score) - inv_end_235_store,0)),0)) over(PARTITION BY product_code) AS cnt_235_require_score
,sum(if(sum_qty_1week_store>0 OR inv_priority_240>0 OR (inv_end_240_store < 1 AND cnt_main_size <= 2 AND inv_end_store > 0 AND days_no_sale<= 28 ), if(cnt_main_size_gt2 <= 1 AND inv_end_240_store <=0 , greatest(greatest(inv_priority_240,inv_240_from_score,inv_240_from_score_main)-inv_end_240_store,0), greatest(least(1,inv_240_from_score) - inv_end_240_store,0)),0)) over(PARTITION BY product_code) AS cnt_240_require_score
,sum(if(inv_priority_245>0, greatest(greatest(inv_priority_245,inv_245_from_score)-inv_end_245_store,0),0)) over(PARTITION BY product_code) 																			 AS cnt_245_require_score
,row_number() over(PARTITION BY product_code ORDER BY ind_priority_inv DESC,sum_qty_1week_store DESC,if(sum_qty_2week_store >=0,1,0) DESC, ind_mainsize_require, final_normed_avg_sr_score DESC) AS rank_in_allsize
,sum(if(sum_qty_1week_store>0 OR inv_priority_225>0 OR (inv_end_225_store < 1 AND cnt_main_size <= 2 AND inv_end_store > 0 AND days_no_sale<= 28 ), if(cnt_main_size_gt2 <= 1 AND inv_end_225_store <=0 , greatest(greatest(inv_priority_225,inv_225_from_score,inv_225_from_score_main)-inv_end_225_store,0), greatest(least(1,inv_225_from_score) - inv_end_225_store,0)),0)) over(PARTITION BY product_code ORDER BY inv_priority_225 DESC,sum_qty_1week_store DESC,if(sum_qty_2week_store >=0,1,0) DESC,ind_mainsize_require,final_normed_avg_sr_score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_in_225
,sum(if(sum_qty_1week_store>0 OR inv_priority_230>0 OR (inv_end_230_store < 1 AND cnt_main_size <= 2 AND inv_end_store > 0 AND days_no_sale<= 28 ), if(cnt_main_size_gt2 <= 1 AND inv_end_230_store <=0 , greatest(greatest(inv_priority_230,inv_230_from_score,inv_230_from_score_main)-inv_end_230_store,0), greatest(least(1,inv_230_from_score) - inv_end_230_store,0)),0)) over(PARTITION BY product_code ORDER BY inv_priority_230 DESC,sum_qty_1week_store DESC,if(sum_qty_2week_store >=0,1,0) DESC,ind_mainsize_require,final_normed_avg_sr_score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_in_230
,sum(if(sum_qty_1week_store>0 OR inv_priority_235>0 OR (inv_end_235_store < 1 AND cnt_main_size <= 2 AND inv_end_store > 0 AND days_no_sale<= 28 ), if(cnt_main_size_gt2 <= 1 AND inv_end_235_store <=0 , greatest(greatest(inv_priority_235,inv_235_from_score,inv_235_from_score_main)-inv_end_235_store,0), greatest(least(1,inv_235_from_score) - inv_end_235_store,0)),0)) over(PARTITION BY product_code ORDER BY inv_priority_235 DESC,sum_qty_1week_store DESC,if(sum_qty_2week_store >=0,1,0) DESC,ind_mainsize_require,final_normed_avg_sr_score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_in_235
,sum(if(sum_qty_1week_store>0 OR inv_priority_240>0 OR (inv_end_240_store < 1 AND cnt_main_size <= 2 AND inv_end_store > 0 AND days_no_sale<= 28 ), if(cnt_main_size_gt2 <= 1 AND inv_end_240_store <=0 , greatest(greatest(inv_priority_240,inv_240_from_score,inv_240_from_score_main)-inv_end_240_store,0), greatest(least(1,inv_240_from_score) - inv_end_240_store,0)),0)) over(PARTITION BY product_code ORDER BY inv_priority_240 DESC,sum_qty_1week_store DESC,if(sum_qty_2week_store >=0,1,0) DESC,ind_mainsize_require,final_normed_avg_sr_score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_in_240
,sum(if(                         inv_priority_245>0, greatest(greatest(inv_priority_245,inv_245_from_score)-inv_end_245_store,0),0)) over(PARTITION BY product_code ORDER BY inv_priority_245 DESC,sum_qty_1week_store DESC,final_normed_avg_sr_score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_in_245
,nvl(sum(nvl(if(sum_qty_1week_store>0 OR inv_priority_225>0 OR (inv_end_225_store < 1 AND cnt_main_size <= 2 AND inv_end_store >0 AND days_no_sale<= 28 ), if(cnt_main_size_gt2 <= 1 AND inv_end_225_store <=0 , greatest(greatest(inv_priority_225,inv_225_from_score,inv_225_from_score_main)-inv_end_225_store,0), greatest(least(1,inv_225_from_score) - inv_end_225_store,0)),0),0)) over(PARTITION BY product_code ORDER BY inv_priority_225 DESC,sum_qty_1week_store DESC,if(sum_qty_2week_store >=0,1,0) DESC,ind_mainsize_require,final_normed_avg_sr_score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_in_225_lag1
,nvl(sum(nvl(if(sum_qty_1week_store>0 OR inv_priority_230>0 OR (inv_end_230_store < 1 AND cnt_main_size <= 2 AND inv_end_store >0 AND days_no_sale<= 28 ), if(cnt_main_size_gt2 <= 1 AND inv_end_230_store <=0 , greatest(greatest(inv_priority_230,inv_230_from_score,inv_230_from_score_main)-inv_end_230_store,0), greatest(least(1,inv_230_from_score) - inv_end_230_store,0)),0),0)) over(PARTITION BY product_code ORDER BY inv_priority_230 DESC,sum_qty_1week_store DESC,if(sum_qty_2week_store >=0,1,0) DESC,ind_mainsize_require,final_normed_avg_sr_score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_in_230_lag1
,nvl(sum(nvl(if(sum_qty_1week_store>0 OR inv_priority_235>0 OR (inv_end_235_store < 1 AND cnt_main_size <= 2 AND inv_end_store >0 AND days_no_sale<= 28 ), if(cnt_main_size_gt2 <= 1 AND inv_end_235_store <=0 , greatest(greatest(inv_priority_235,inv_235_from_score,inv_235_from_score_main)-inv_end_235_store,0), greatest(least(1,inv_235_from_score) - inv_end_235_store,0)),0),0)) over(PARTITION BY product_code ORDER BY inv_priority_235 DESC,sum_qty_1week_store DESC,if(sum_qty_2week_store >=0,1,0) DESC,ind_mainsize_require,final_normed_avg_sr_score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_in_235_lag1
,nvl(sum(nvl(if(sum_qty_1week_store>0 OR inv_priority_240>0 OR (inv_end_240_store < 1 AND cnt_main_size <= 2 AND inv_end_store >0 AND days_no_sale<= 28 ), if(cnt_main_size_gt2 <= 1 AND inv_end_240_store <=0 , greatest(greatest(inv_priority_240,inv_240_from_score,inv_240_from_score_main)-inv_end_240_store,0), greatest(least(1,inv_240_from_score) - inv_end_240_store,0)),0),0)) over(PARTITION BY product_code ORDER BY inv_priority_240 DESC,sum_qty_1week_store DESC,if(sum_qty_2week_store >=0,1,0) DESC,ind_mainsize_require,final_normed_avg_sr_score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_in_240_lag1
,nvl(sum(nvl(if(                         inv_priority_245>0, greatest(greatest(inv_priority_245,inv_245_from_score)-inv_end_245_store,0),0),0)) over(PARTITION BY product_code ORDER BY inv_priority_245 DESC,sum_qty_1week_store DESC,final_normed_avg_sr_score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_in_245_lag1
,sum(if(ind_clear>0,inv_end_220_store,0)) over(PARTITION BY product_code) AS sum_out_220_clear
,sum(if(ind_clear>0,inv_end_225_store,0)) over(PARTITION BY product_code) AS sum_out_225_clear
,sum(if(ind_clear>0,inv_end_230_store,0)) over(PARTITION BY product_code) AS sum_out_230_clear
,sum(if(ind_clear>0,inv_end_235_store,0)) over(PARTITION BY product_code) AS sum_out_235_clear
,sum(if(ind_clear>0,inv_end_240_store,0)) over(PARTITION BY product_code) AS sum_out_240_clear
,sum(if(ind_clear>0,inv_end_245_store,0)) over(PARTITION BY product_code) AS sum_out_245_clear
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_220_store-greatest(inv_priority_220,inv_220_from_score),0),if(ind_no_out=0,inv_end_220_store,0))) over(PARTITION BY product_code) AS cum_out_220_available
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_225_store-greatest(inv_priority_225,inv_225_from_score),0),if(ind_no_out=0,inv_end_225_store,0))) over(PARTITION BY product_code) AS cum_out_225_available
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_230_store-greatest(inv_priority_230,inv_230_from_score),0),if(ind_no_out=0,inv_end_230_store,0))) over(PARTITION BY product_code) AS cum_out_230_available
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_235_store-greatest(inv_priority_235,inv_235_from_score),0),if(ind_no_out=0,inv_end_235_store,0))) over(PARTITION BY product_code) AS cum_out_235_available
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_240_store-greatest(inv_priority_240,inv_240_from_score),0),if(ind_no_out=0,inv_end_240_store,0))) over(PARTITION BY product_code) AS cum_out_240_available
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_245_store-greatest(inv_priority_245,inv_245_from_score),0),if(ind_no_out=0,inv_end_245_store,0))) over(PARTITION BY product_code) AS cum_out_245_available
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_220_store-greatest(inv_priority_220,inv_220_from_score),0),if(ind_clear=1,inv_end_220_store,if(ind_no_out=0,greatest(0,inv_end_220_store-1),0)))) over(PARTITION BY product_code) AS cum_out_extra_220_available
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_225_store-greatest(inv_priority_225,inv_225_from_score),0),if(ind_clear=1,inv_end_225_store,if(ind_no_out=0,greatest(0,inv_end_225_store-1),0)))) over(PARTITION BY product_code) AS cum_out_extra_225_available
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_230_store-greatest(inv_priority_230,inv_230_from_score),0),if(ind_clear=1,inv_end_230_store,if(ind_no_out=0,greatest(0,inv_end_230_store-1),0)))) over(PARTITION BY product_code) AS cum_out_extra_230_available
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_235_store-greatest(inv_priority_235,inv_235_from_score),0),if(ind_clear=1,inv_end_235_store,if(ind_no_out=0,greatest(0,inv_end_235_store-1),0)))) over(PARTITION BY product_code) AS cum_out_extra_235_available
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_240_store-greatest(inv_priority_240,inv_240_from_score),0),if(ind_clear=1,inv_end_240_store,if(ind_no_out=0,greatest(0,inv_end_240_store-1),0)))) over(PARTITION BY product_code) AS cum_out_extra_240_available
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_245_store-greatest(inv_priority_245,inv_245_from_score),0),if(ind_clear=1,inv_end_245_store,if(ind_no_out=0,greatest(0,inv_end_245_store-1),0)))) over(PARTITION BY product_code) AS cum_out_extra_245_available
,if(sum_qty_1week_store=0 AND ind_no_out=0,greatest(0,inv_end_220_store-1),if(ind_no_out=0,greatest(inv_end_220_store-greatest(inv_priority_220,inv_220_from_score),0),0)) AS inv_extra_220_store
,if(sum_qty_1week_store=0 AND ind_no_out=0,greatest(0,inv_end_225_store-1),if(ind_no_out=0,greatest(inv_end_225_store-greatest(inv_priority_225,inv_225_from_score),0),0)) AS inv_extra_225_store
,if(sum_qty_1week_store=0 AND ind_no_out=0,greatest(0,inv_end_230_store-1),if(ind_no_out=0,greatest(inv_end_230_store-greatest(inv_priority_230,inv_230_from_score),0),0)) AS inv_extra_230_store
,if(sum_qty_1week_store=0 AND ind_no_out=0,greatest(0,inv_end_235_store-1),if(ind_no_out=0,greatest(inv_end_235_store-greatest(inv_priority_235,inv_235_from_score),0),0)) AS inv_extra_235_store
,if(sum_qty_1week_store=0 AND ind_no_out=0,greatest(0,inv_end_240_store-1),if(ind_no_out=0,greatest(inv_end_240_store-greatest(inv_priority_240,inv_240_from_score),0),0)) AS inv_extra_240_store
,if(sum_qty_1week_store=0 AND ind_no_out=0,greatest(0,inv_end_245_store-1),if(ind_no_out=0,greatest(inv_end_245_store-greatest(inv_priority_245,inv_245_from_score),0),0)) AS inv_extra_245_store
,sum(if(ind_mainsize_require = 1 AND cnt_main_size = 4 AND sum_qty_2week_store <= 0, if(ind_no_out=0,inv_end_225_store,0),0)) over(PARTITION BY product_code) AS cum_out_noneed_225_available
,sum(if(ind_mainsize_require = 1 AND cnt_main_size = 4 AND sum_qty_2week_store <= 0, if(ind_no_out=0,inv_end_230_store,0),0)) over(PARTITION BY product_code) AS cum_out_noneed_230_available
,sum(if(ind_mainsize_require = 1 AND cnt_main_size = 4 AND sum_qty_2week_store <= 0, if(ind_no_out=0,inv_end_235_store,0),0)) over(PARTITION BY product_code) AS cum_out_noneed_235_available
,sum(if(ind_mainsize_require = 1 AND cnt_main_size = 4 AND sum_qty_2week_store <= 0, if(ind_no_out=0,inv_end_240_store,0),0)) over(PARTITION BY product_code) AS cum_out_noneed_240_available
,if(ind_mainsize_require = 1 AND cnt_main_size = 4 AND sum_qty_2week_store <= 0, if(ind_no_out=0,inv_end_225_store,0),0) AS inv_noneed_225_store
,if(ind_mainsize_require = 1 AND cnt_main_size = 4 AND sum_qty_2week_store <= 0, if(ind_no_out=0,inv_end_230_store,0),0) AS inv_noneed_230_store
,if(ind_mainsize_require = 1 AND cnt_main_size = 4 AND sum_qty_2week_store <= 0, if(ind_no_out=0,inv_end_235_store,0),0) AS inv_noneed_235_store
,if(ind_mainsize_require = 1 AND cnt_main_size = 4 AND sum_qty_2week_store <= 0, if(ind_no_out=0,inv_end_240_store,0),0) AS inv_noneed_240_store
FROM
(SELECT *
	,if(cnt_main_size <= 2,greatest(inv_225_from_score,1),inv_225_from_score) AS inv_225_from_score_main
	,if(cnt_main_size <= 2,greatest(inv_225_from_score,1),inv_230_from_score) AS inv_230_from_score_main
	,if(cnt_main_size <= 2,greatest(inv_225_from_score,1),inv_235_from_score) AS inv_235_from_score_main
	,if(cnt_main_size <= 2,greatest(inv_225_from_score,1),inv_240_from_score) AS inv_240_from_score_main
	,least(1,greatest(inv_end_225_store-1,0))+least(1,greatest(inv_end_230_store-1,0))+least(1,greatest(inv_end_235_store-1,0))+least(1,greatest(inv_end_240_store-1,0)) AS cnt_main_size_gt2
	,CASE WHEN sum_qty_2week_store >0 OR ind_priority_inv > 0 OR ind_no_out = 1 THEN 0
          WHEN cnt_main_size <= 2 AND inv_end_store > 0 AND days_no_sale<= 28 THEN 0
		  ELSE 1
		  END AS ind_mainsize_require
	FROM belle_sh.yl_trans_by_SKU_score_adjust_{invsunday}_spr_vt12
) a
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Xwtransbyskubsprpreparevt12().run_command()
