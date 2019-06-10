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


class Yltransbyskumaster20190421sprtestv10(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_master20190421_spr_test_v10;
CREATE TABLE belle_sh.yl_trans_by_SKU_master20190421_spr_test_v10 AS
SELECT
concat(a.product_code,"|",a.store_no) AS match_key
,a.*
,sum(sum_qty_2week_store) over(PARTITION BY a.product_code) AS sum_qty_2week_sku
,b.days_for_sale_1w
,b.days_no_sale
,b.days_no_sale_1w
,b.days_no_sale_4w
,nvl(c.ind_no_out,0) AS ind_no_out
,e.inv_end_220_I0215
,e.inv_end_225_I0215
,e.inv_end_230_I0215
,e.inv_end_235_I0215
,e.inv_end_240_I0215
,e.inv_end_245_I0215
,d.use_30D_ind
,d.use_60D_ind
,d.sale_rate_last_2_week
,d.avg_store_last_4_week_sr
,d.avg_sr_categ_name3_3_sum_treat
,d.avg_sr_categ_name3_2_treat
,d.avg_sr_heel_type2_treat
,d.avg_sr_discount_bin_treat
,d.avg_sr_sale_amt_bin_treat
,d.avg_sr_30d_treat
,d.avg_sr_60d_treat
,d.final_normed_avg_sr_score_orig
,d.final_normed_avg_sr_score
,d.final_score_rank
,d.final_score_min
,d.final_score_diff
,d.final_score_diff/sum(d.final_score_diff) over(PARTITION BY a.product_code) AS final_score_pct
,count(a.store_no) over(PARTITION BY a.product_code) AS cnt_store_sku
,sum(inv_end_220_store*nvl(ind_no_out,0)) over(PARTITION BY a.product_code) AS sum_inv_220_no_out
,sum(inv_end_225_store*nvl(ind_no_out,0)) over(PARTITION BY a.product_code) AS sum_inv_225_no_out
,sum(inv_end_230_store*nvl(ind_no_out,0)) over(PARTITION BY a.product_code) AS sum_inv_230_no_out
,sum(inv_end_235_store*nvl(ind_no_out,0)) over(PARTITION BY a.product_code) AS sum_inv_235_no_out
,sum(inv_end_240_store*nvl(ind_no_out,0)) over(PARTITION BY a.product_code) AS sum_inv_240_no_out
,sum(inv_end_245_store*nvl(ind_no_out,0)) over(PARTITION BY a.product_code) AS sum_inv_245_no_out
,f.store_level
,f.store_name
,nvl(g.ind_events,0) AS ind_events
,if(a.store_no IN ('I01FST','I01HST','I028ST','I029ST','I035ST','I050ST','I063ST','I064ST','I065ST','I078ST','IAJ001','IAJ003','IAJ010'),1,0) AS ind_bad_sale
,(CASE WHEN g.ind_events=1 THEN 3
       WHEN sum_qty_1week_store>=3 OR (sum_qty_1week_store=2 AND sum_qty_2week_store>4) OR sum_qty_2week_store>6
	   THEN 2
  ELSE 1
  END)
  AS upperlimit
,(CASE WHEN round(a.sku_inv_per_store,0)>=5 THEN -999
       WHEN round(a.sku_inv_per_store,0)<5 AND a.sku_qty_per_inv<0.25 THEN 0
	   ELSE 1
  END) AS ind_c_limit
,nvl(h.ind_priority_inv,0) AS ind_priority_inv
,nvl(h.inv_priority_220,0) AS inv_priority_220
,nvl(h.inv_priority_225,0) AS inv_priority_225
,nvl(h.inv_priority_230,0) AS inv_priority_230
,nvl(h.inv_priority_235,0) AS inv_priority_235
,nvl(h.inv_priority_240,0) AS inv_priority_240
,nvl(h.inv_priority_245,0) AS inv_priority_245
,nvl(i.ind_clear,0) AS ind_clear
,(CASE WHEN ind_last_week_added=1
            AND (   pct_inv_added<=0.3
			     OR (pct_inv_added>0.3 AND inv_end_sku>180)
			     OR (pct_inv_added>0.3 AND inv_end_sku>80 AND inv_end_sku<=180 AND sum_qty_1week_store>0)
			    )
	   THEN 1
	   ELSE 0
	   END
) AS ind_last_week
,j.label_sku_logic
,j.label_cumu_qty_pct
,j.label_avg_store_inv
,j.label_cnt_store_main_size_less_2_pct
,j.label_sku_cover_rate
,nvl(k.ind_no_in,0) AS ind_no_in
FROM belle_sh.yl_trans_by_SKU_sale_inv_20190421_spr a
LEFT JOIN belle_sh.yl_trans_by_SKU_days_nosale_20190421_spr b
ON a.product_code=b.product_code AND a.store_no=b.store_no
LEFT JOIN belle_sh.yl_trans_by_SKU_store_no_out20190421_spr c
ON a.store_no=c.store_no
LEFT JOIN belle_sh.yl_trans_by_SKU_score_20190421_spr d
ON a.product_code=d.product_code AND a.store_no=d.store_no
LEFT JOIN belle_sh.yl_trans_by_SKU_inv_total_20190421_spr e
ON a.product_code=e.product_code
LEFT JOIN belle_sh.yl_trans_by_SKU_storelevel f
ON a.store_no=f.store_no
LEFT JOIN belle_sh.yl_trans_by_SKU_events_20190421_spr g
ON a.store_no=g.store_no
LEFT JOIN belle_sh.yl_trans_by_SKU_priority_inv_20190421_spr h
ON a.store_no=h.store_no AND a.product_code=h.product_code
LEFT JOIN belle_sh.yl_trans_by_SKU_clear_inv_20190421_spr i
ON a.store_no=i.store_no AND a.product_code=i.product_code
LEFT JOIN belle_sh.yl_sku_filter_to_allocate_19_20190421_v2 j
ON a.product_code=j.product_code
LEFT JOIN belle_sh.yl_trans_by_SKU_store_no_in20190421_spr k
ON a.store_no=k.store_no
ORDER BY
a.product_code
,if(days_no_sale_4w=28,1,0)
,d.final_normed_avg_sr_score DESC
;"""
        self.call()

if __name__ == '__main__':
    Yltransbyskumaster20190421sprtestv10().run_command()
