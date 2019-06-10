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

class Yltransbyskubspronlyv2(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_b_{invsunday}_spr_only_v2;
CREATE TABLE belle_sh.yl_trans_by_SKU_b_{invsunday}_spr_only_v2 AS
SELECT
b.*
,(CASE WHEN cnt_all_size=1 AND days_no_sale>=56 THEN 0 WHEN rank_220_only_in<=sum_inv_220_only_out THEN inv_220_final+1 ELSE inv_220_final END ) AS inv_220_only_in
,(CASE WHEN cnt_all_size=1 AND days_no_sale>=56 THEN 0 WHEN rank_225_only_in<=sum_inv_225_only_out THEN inv_225_final+1 ELSE inv_225_final END ) AS inv_225_only_in
,(CASE WHEN cnt_all_size=1 AND days_no_sale>=56 THEN 0 WHEN rank_230_only_in<=sum_inv_230_only_out THEN inv_230_final+1 ELSE inv_230_final END ) AS inv_230_only_in
,(CASE WHEN cnt_all_size=1 AND days_no_sale>=56 THEN 0 WHEN rank_235_only_in<=sum_inv_235_only_out THEN inv_235_final+1 ELSE inv_235_final END ) AS inv_235_only_in
,(CASE WHEN cnt_all_size=1 AND days_no_sale>=56 THEN 0 WHEN rank_240_only_in<=sum_inv_240_only_out THEN inv_240_final+1 ELSE inv_240_final END ) AS inv_240_only_in
,(CASE WHEN cnt_all_size=1 AND days_no_sale>=56 THEN 0 WHEN rank_245_only_in<=sum_inv_245_only_out THEN inv_245_final+1 ELSE inv_245_final END ) AS inv_245_only_in
FROM
(
    SELECT
    a.*
    ,row_number() over(PARTITION BY product_code ORDER BY if((cnt_all_size=1 AND days_no_sale>=56) OR cnt_all_size=0,1,0),if(days_no_sale<56 AND cnt_all_size=1 AND inv_220_final=0,0,1),if(inv_end_220_store-inv_220_final>0 AND inv_220_final<=1,0,1),if(sum_qty_2week_store>0 AND inv_225_final*inv_230_final*inv_235_final*inv_240_final=0 AND inv_220_final<2,0,1),if(sum_qty_2week_store>1 AND inv_all_final<10 AND inv_220_final<=1,0,1),sum_qty_1week_store DESC,final_normed_avg_sr_score DESC) AS rank_220_only_in
    ,row_number() over(PARTITION BY product_code ORDER BY if((cnt_all_size=1 AND days_no_sale>=56) OR cnt_all_size=0,1,0),if(days_no_sale<56 AND cnt_all_size=1 AND inv_225_final=0,0,1),if(inv_end_225_store-inv_225_final>0 AND inv_225_final<=1,0,1),if(sum_qty_2week_store>0 AND inv_225_final*inv_230_final*inv_235_final*inv_240_final=0 AND inv_225_final<2,0,1),if(sum_qty_2week_store>1 AND inv_all_final<10 AND inv_225_final<=1,0,1),sum_qty_1week_store DESC,final_normed_avg_sr_score DESC) AS rank_225_only_in
    ,row_number() over(PARTITION BY product_code ORDER BY if((cnt_all_size=1 AND days_no_sale>=56) OR cnt_all_size=0,1,0),if(days_no_sale<56 AND cnt_all_size=1 AND inv_230_final=0,0,1),if(inv_end_230_store-inv_230_final>0 AND inv_230_final<=1,0,1),if(sum_qty_2week_store>0 AND inv_225_final*inv_230_final*inv_235_final*inv_240_final=0 AND inv_230_final<2,0,1),if(sum_qty_2week_store>1 AND inv_all_final<10 AND inv_230_final<=1,0,1),sum_qty_1week_store DESC,final_normed_avg_sr_score DESC) AS rank_230_only_in
    ,row_number() over(PARTITION BY product_code ORDER BY if((cnt_all_size=1 AND days_no_sale>=56) OR cnt_all_size=0,1,0),if(days_no_sale<56 AND cnt_all_size=1 AND inv_235_final=0,0,1),if(inv_end_235_store-inv_235_final>0 AND inv_235_final<=1,0,1),if(sum_qty_2week_store>0 AND inv_225_final*inv_230_final*inv_235_final*inv_240_final=0 AND inv_235_final<2,0,1),if(sum_qty_2week_store>1 AND inv_all_final<10 AND inv_235_final<=1,0,1),sum_qty_1week_store DESC,final_normed_avg_sr_score DESC) AS rank_235_only_in
    ,row_number() over(PARTITION BY product_code ORDER BY if((cnt_all_size=1 AND days_no_sale>=56) OR cnt_all_size=0,1,0),if(days_no_sale<56 AND cnt_all_size=1 AND inv_240_final=0,0,1),if(inv_end_240_store-inv_240_final>0 AND inv_240_final<=1,0,1),if(sum_qty_2week_store>0 AND inv_225_final*inv_230_final*inv_235_final*inv_240_final=0 AND inv_240_final<2,0,1),if(sum_qty_2week_store>1 AND inv_all_final<10 AND inv_240_final<=1,0,1),sum_qty_1week_store DESC,final_normed_avg_sr_score DESC) AS rank_240_only_in
    ,row_number() over(PARTITION BY product_code ORDER BY if((cnt_all_size=1 AND days_no_sale>=56) OR cnt_all_size=0,1,0),if(days_no_sale<56 AND cnt_all_size=1 AND inv_245_final=0,0,1),if(inv_end_245_store-inv_245_final>0 AND inv_245_final<=1,0,1),if(sum_qty_2week_store>0 AND inv_225_final*inv_230_final*inv_235_final*inv_240_final=0 AND inv_245_final<2,0,1),if(sum_qty_2week_store>1 AND inv_all_final<10 AND inv_245_final<=1,0,1),sum_qty_1week_store DESC,final_normed_avg_sr_score DESC) AS rank_245_only_in
    ,sum(if(cnt_all_size=1 AND days_no_sale>=56,inv_220_final,0)) over(PARTITION BY product_code) AS sum_inv_220_only_out
    ,sum(if(cnt_all_size=1 AND days_no_sale>=56,inv_225_final,0)) over(PARTITION BY product_code) AS sum_inv_225_only_out
    ,sum(if(cnt_all_size=1 AND days_no_sale>=56,inv_230_final,0)) over(PARTITION BY product_code) AS sum_inv_230_only_out
    ,sum(if(cnt_all_size=1 AND days_no_sale>=56,inv_235_final,0)) over(PARTITION BY product_code) AS sum_inv_235_only_out
    ,sum(if(cnt_all_size=1 AND days_no_sale>=56,inv_240_final,0)) over(PARTITION BY product_code) AS sum_inv_240_only_out
    ,sum(if(cnt_all_size=1 AND days_no_sale>=56,inv_245_final,0)) over(PARTITION BY product_code) AS sum_inv_245_only_out
    FROM
    (    SELECT
         *
         ,least(1,inv_220_final)+least(1,inv_225_final)+least(1,inv_230_final)+least(1,inv_235_final)+least(1,inv_240_final)+least(1,inv_245_final) AS cnt_all_size
         ,inv_220_final+inv_225_final+inv_230_final+inv_235_final+inv_240_final+inv_245_final AS inv_all_final
         FROM belle_sh.op_trans_by_sku_final_{invsunday}_spr
    ) a
) b
;
SET hive.support.quoted.identifiers=NONE;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Yltransbyskubspronlyv2().run_command()
