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


class Xwtransbyskubsprprepare220190421size4v11a(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.xw_trans_by_SKU_b_spr_prepare2_20190421_size4_v11a;
CREATE TABLE belle_sh.xw_trans_by_SKU_b_spr_prepare2_20190421_size4_v11a AS
SELECT
*
,row_number() over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size4>3,0,1),final_normed_avg_sr_score,inv_extra_220_store DESC) AS rank_out_220
,row_number() over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size4>3,0,1),final_normed_avg_sr_score,inv_extra_245_store DESC) AS rank_out_245
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_220_store-greatest(inv_priority_220,inv_220_from_score),0),if(ind_clear=1,inv_end_220_store,if(ind_no_out=0,greatest(0,inv_end_220_store-1),0)))) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size4>3,0,1),final_normed_avg_sr_score,inv_extra_220_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_extra_220
,sum(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_245_store-greatest(inv_priority_245,inv_245_from_score),0),if(ind_clear=1,inv_end_245_store,if(ind_no_out=0,greatest(0,inv_end_245_store-1),0)))) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size4>3,0,1),final_normed_avg_sr_score,inv_extra_245_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_extra_245
,nvl(sum(nvl(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_220_store-greatest(inv_priority_220,inv_220_from_score),0),if(ind_clear=1,inv_end_220_store,if(ind_no_out=0,greatest(0,inv_end_220_store-1),0))),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size4>3,0,1),final_normed_avg_sr_score,inv_extra_220_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_extra_220_lag1
,nvl(sum(nvl(if((ind_priority_inv>0 OR sum_qty_1week_store>0) AND ind_clear=0 AND ind_no_out=0,greatest(inv_end_245_store-greatest(inv_priority_245,inv_245_from_score),0),if(ind_clear=1,inv_end_245_store,if(ind_no_out=0,greatest(0,inv_end_245_store-1),0))),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size4>3,0,1),final_normed_avg_sr_score,inv_extra_245_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_extra_245_lag1
,sum(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_220_store),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size4>3,0,1),final_normed_avg_sr_score,inv_extra_220_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_220
,sum(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_245_store),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size4>3,0,1),final_normed_avg_sr_score,inv_extra_245_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_out_245
,nvl(sum(nvl(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_220_store),0),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size4>3,0,1),final_normed_avg_sr_score,inv_extra_220_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_220_lag1
,nvl(sum(nvl(if(sum_qty_1week_store<=0 AND ind_clear=0 AND ind_no_out=0,least(1,inv_end_245_store),0),0)) over(PARTITION BY product_code ORDER BY ind_clear DESC,ind_no_out,sum_qty_1week_store,sum_qty_2week_store,if(days_no_sale>56,0,1),if(cnt_main_size4>3,0,1),final_normed_avg_sr_score,inv_extra_245_store DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS cum_out_245_lag1
FROM belle_sh.xw_trans_by_SKU_b_spr_prepare_20190421_size4_v11a a
;"""
        self.call()

if __name__ == '__main__':
    Xwtransbyskubsprprepare220190421size4v11a().run_command()
