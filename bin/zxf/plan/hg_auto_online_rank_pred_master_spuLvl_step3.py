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


class Hgautoonlinerankpredmasterspulvlstep3(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.hg_auto_online_rank_pred_master_spuLvl_step3;
CREATE TABLE belle_sh.hg_auto_online_rank_pred_master_spuLvl_step3 AS
SELECT
a.*
,(nvl(pct_sale_cat_p1mth_prev_yr,0) - nvl(pct_sale_cat_p2mth_prev_yr,0)) AS delta_pct_sale_cat_p1_p2mth_prev_yr
,(nvl(pct_sale_cat_p1mth_prev_yr,0) - nvl(pct_sale_cat_p3mth_prev_yr,0)) AS delta_pct_sale_cat_p1_p3mth_prev_yr
,(nvl(pct_sale_cat_p1mth_prev_yr,0) - nvl(pct_sale_cat_l1mth_prev_yr,0)) AS delta_pct_sale_cat_p1_l1mth_prev_yr
,(nvl(pct_sale_cat_p1mth_prev_yr,0) - nvl(pct_sale_cat_l2mth_prev_yr,0)) AS delta_pct_sale_cat_p1_l2mth_prev_yr
,nvl(b.rank_sale_p1mth_avg,0) 													AS rank_p1mth_prevYr_l12mth
,nvl(b.rank_sale_p1mth_avg,0) - nvl(b.rank_sale_l12mth,0)       				AS delta_rank_p1mth_prevYr_l12mth
,nvl(b.pct_sale_skuInCat4_p1mth_avg,0)  										AS pctSale_skuInCat4_p1mth_prevYr_l12mth
,nvl(b.pct_sale_skuInCat4_p1mth_avg,0) - nvl(b.pct_sale_sku_cat4_l12mth_avg,0)	AS delta_pctSale_skuInCat4_p1mth_prevYr_l12mth
,nvl(c.rank_sale_p1mth_avg,0) 													AS rank_p1mth_prevYr_l1mth
,nvl(c.rank_sale_p1mth_avg,0) - nvl(c.rank_sale_l1mth,0)       					AS delta_rank_p1mth_prevYr_l1mth
,nvl(c.pct_sale_skuInCat4_p1mth_avg,0)  										AS pctSale_skuInCat4_p1mth_prevYr_l1mth
,nvl(c.pct_sale_skuInCat4_p1mth_avg,0) - nvl(c.pct_sale_sku_cat4_l1mth_avg,0)	AS delta_pctSale_skuInCat4_p1mth_prevYr_l1mth
,nvl(d.rank_sale_p1mth_avg,0) 													AS rank_p1mth_prevYr_l1wk
,nvl(d.rank_sale_p1mth_avg,0) - nvl(d.rank_sale_l1wk,0)       					AS delta_rank_p1mth_prevYr_l1wk
,nvl(d.pct_sale_skuInCat4_p1mth_avg,0)  										AS pctSale_skuInCat4_p1mth_prevYr_l1wk
,nvl(d.pct_sale_skuInCat4_p1mth_avg,0) - nvl(d.pct_sale_sku_cat4_l1wk_avg,0)	AS delta_pctSale_skuInCat4_p1mth_prevYr_l1wk
FROM (SELECT * FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step2) a
LEFT JOIN (
	SELECT
	commodity_style_v2,rank_sale_l12mth,sales_year_v2_forPred,sales_year_v2_true
	,floor(avg(rank_sale_p1mth)) AS rank_sale_p1mth_avg
	,avg(pct_sale_skuInCat4_p1mth) AS pct_sale_skuInCat4_p1mth_avg
	,avg(pct_sale_sku_cat4_l12mth) AS pct_sale_sku_cat4_l12mth_avg
	FROM (
		SELECT
		commodity_style_v2,rank_sale_l12mth
		,rank_sale_p1mth, pct_sale_skuInCat4_p1mth, pct_sale_sku_cat4_l12mth
		,sales_year_v2 + 1 AS sales_year_v2_forPred
		,sales_year_v2 AS sales_year_v2_true
		FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step2
	) bb
	GROUP BY commodity_style_v2,rank_sale_l12mth,sales_year_v2_forPred,sales_year_v2_true
) b
ON a.sales_year_v2 = b.sales_year_v2_forPred AND a.commodity_style_v2 = b.commodity_style_v2 AND a.rank_sale_l12mth = b.rank_sale_l12mth
LEFT JOIN (
	SELECT
	commodity_style_v2,rank_sale_l1mth,sales_year_v2_forPred,sales_year_v2_true
	,floor(avg(rank_sale_p1mth)) AS rank_sale_p1mth_avg
	,avg(pct_sale_skuInCat4_p1mth) AS pct_sale_skuInCat4_p1mth_avg
	,avg(pct_sale_sku_cat4_l1mth) AS pct_sale_sku_cat4_l1mth_avg
	FROM (
		SELECT
		commodity_style_v2,rank_sale_l1mth
		,rank_sale_p1mth, pct_sale_skuInCat4_p1mth, pct_sale_sku_cat4_l1mth
		,sales_year_v2 + 1 AS sales_year_v2_forPred
		,sales_year_v2 AS sales_year_v2_true
		FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step2
	) cc
	GROUP BY commodity_style_v2,rank_sale_l1mth,sales_year_v2_forPred,sales_year_v2_true
) c
ON a.sales_year_v2 = c.sales_year_v2_forPred AND a.commodity_style_v2 = c.commodity_style_v2 AND a.rank_sale_l1mth = c.rank_sale_l1mth
LEFT JOIN (
	SELECT
	commodity_style_v2,rank_sale_l1wk,sales_year_v2_forPred,sales_year_v2_true
	,floor(avg(rank_sale_p1mth)) AS rank_sale_p1mth_avg
	,avg(pct_sale_skuInCat4_p1mth) AS pct_sale_skuInCat4_p1mth_avg
	,avg(pct_sale_sku_cat4_l1wk) AS pct_sale_sku_cat4_l1wk_avg
	FROM (
		SELECT
		commodity_style_v2,rank_sale_l1wk
		,rank_sale_p1mth, pct_sale_skuInCat4_p1mth, pct_sale_sku_cat4_l1wk
		,sales_year_v2 + 1 AS sales_year_v2_forPred
		,sales_year_v2 AS sales_year_v2_true
		FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step2
	) dd
	GROUP BY commodity_style_v2,rank_sale_l1wk,sales_year_v2_forPred,sales_year_v2_true
) d
ON a.sales_year_v2 = d.sales_year_v2_forPred AND a.commodity_style_v2 = d.commodity_style_v2 AND a.rank_sale_l1wk = d.rank_sale_l1wk
;"""
        self.call()

if __name__ == '__main__':
    Hgautoonlinerankpredmasterspulvlstep3().run_command()
