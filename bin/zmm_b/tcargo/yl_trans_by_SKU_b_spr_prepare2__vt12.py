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

class Yltransbyskubsprprepare2vt12(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_b_spr_prepare2_{invsunday}_vt12;
CREATE TABLE belle_sh.yl_trans_by_SKU_b_spr_prepare2_{invsunday}_vt12 AS
SELECT
*
,if(inv_end_225_store=0 AND sum_qty_1week_store=1 AND sum_qty_2week_store=1 AND cnt_main_size>=3 AND (sku_qty_per_inv>=0.1 OR inv_end_220_store+inv_end_245_store>0),0,inv_225_from_score) AS inv_225_from_score_in
,if(inv_end_230_store=0 AND sum_qty_1week_store=1 AND sum_qty_2week_store=1 AND cnt_main_size>=3 AND (sku_qty_per_inv>=0.1 OR inv_end_220_store+inv_end_245_store>0),0,inv_230_from_score) AS inv_230_from_score_in
,if(inv_end_235_store=0 AND sum_qty_1week_store=1 AND sum_qty_2week_store=1 AND cnt_main_size>=3 AND (sku_qty_per_inv>=0.1 OR inv_end_220_store+inv_end_245_store>0),0,inv_235_from_score) AS inv_235_from_score_in
,if(inv_end_240_store=0 AND sum_qty_1week_store=1 AND sum_qty_2week_store=1 AND cnt_main_size>=3 AND (sku_qty_per_inv>=0.1 OR inv_end_220_store+inv_end_245_store>0),0,inv_240_from_score) AS inv_240_from_score_in
,row_number() over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_220_store DESC) AS rank_out_220
,row_number() over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_225_store DESC) AS rank_out_225
,row_number() over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_230_store DESC) AS rank_out_230
,row_number() over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_235_store DESC) AS rank_out_235
,row_number() over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_240_store DESC) AS rank_out_240
,row_number() over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_245_store DESC) AS rank_out_245
,        sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_220_store-greatest(inv_priority_220,inv_220_from_score),0),if(ind_clear=1,inv_end_220_store,if(ind_no_out=0,greatest(0,inv_end_220_store-1),0)))) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_220_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_extra_220
,        sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_225_store-greatest(inv_priority_225,inv_225_from_score),0),if(ind_clear=1,inv_end_225_store,if(ind_no_out=0,greatest(0,inv_end_225_store-1),0)))) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_225_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_extra_225
,        sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_230_store-greatest(inv_priority_230,inv_230_from_score),0),if(ind_clear=1,inv_end_230_store,if(ind_no_out=0,greatest(0,inv_end_230_store-1),0)))) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_230_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_extra_230
,        sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_235_store-greatest(inv_priority_235,inv_235_from_score),0),if(ind_clear=1,inv_end_235_store,if(ind_no_out=0,greatest(0,inv_end_235_store-1),0)))) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_235_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_extra_235
,        sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_240_store-greatest(inv_priority_240,inv_240_from_score),0),if(ind_clear=1,inv_end_240_store,if(ind_no_out=0,greatest(0,inv_end_240_store-1),0)))) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_240_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_extra_240
,        sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_245_store-greatest(inv_priority_245,inv_245_from_score),0),if(ind_clear=1,inv_end_245_store,if(ind_no_out=0,greatest(0,inv_end_245_store-1),0)))) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_245_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_extra_245
,nvl(sum(nvl(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_220_store-greatest(inv_priority_220,inv_220_from_score),0),if(ind_clear=1,inv_end_220_store,if(ind_no_out=0,greatest(0,inv_end_220_store-1),0))),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_220_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_extra_220_lag1
,nvl(sum(nvl(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_225_store-greatest(inv_priority_225,inv_225_from_score),0),if(ind_clear=1,inv_end_225_store,if(ind_no_out=0,greatest(0,inv_end_225_store-1),0))),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_225_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_extra_225_lag1
,nvl(sum(nvl(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_230_store-greatest(inv_priority_230,inv_230_from_score),0),if(ind_clear=1,inv_end_230_store,if(ind_no_out=0,greatest(0,inv_end_230_store-1),0))),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_230_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_extra_230_lag1
,nvl(sum(nvl(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_235_store-greatest(inv_priority_235,inv_235_from_score),0),if(ind_clear=1,inv_end_235_store,if(ind_no_out=0,greatest(0,inv_end_235_store-1),0))),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_235_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_extra_235_lag1
,nvl(sum(nvl(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_240_store-greatest(inv_priority_240,inv_240_from_score),0),if(ind_clear=1,inv_end_240_store,if(ind_no_out=0,greatest(0,inv_end_240_store-1),0))),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_240_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_extra_240_lag1
,nvl(sum(nvl(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_245_store-greatest(inv_priority_245,inv_245_from_score),0),if(ind_clear=1,inv_end_245_store,if(ind_no_out=0,greatest(0,inv_end_245_store-1),0))),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_245_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_extra_245_lag1
,        sum(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_220_store),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_220_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_220
,        sum(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_225_store),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_225_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_225
,        sum(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_230_store),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_230_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_230
,        sum(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_235_store),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_235_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_235
,        sum(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_240_store),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_240_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_240
,        sum(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_245_store),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_245_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_245
,nvl(sum(nvl(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_220_store),0),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_220_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_220_lag1
,nvl(sum(nvl(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_225_store),0),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_225_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_225_lag1
,nvl(sum(nvl(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_230_store),0),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_230_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_230_lag1
,nvl(sum(nvl(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_235_store),0),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_235_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_235_lag1
,nvl(sum(nvl(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_240_store),0),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_240_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_240_lag1
,nvl(sum(nvl(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_245_store),0),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size>3,0,1),final_normed_avg_sr_score,inv_extra_245_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_245_lag1
FROM belle_sh.yl_trans_by_SKU_b_spr_prepare_{invsunday}_vt12 a
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Yltransbyskubsprprepare2vt12().run_command()
