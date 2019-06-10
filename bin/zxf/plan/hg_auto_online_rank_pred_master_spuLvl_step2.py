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


class Hgautoonlinerankpredmasterspulvlstep2(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.hg_auto_online_rank_pred_master_spuLvl_step2;
CREATE TABLE belle_sh.hg_auto_online_rank_pred_master_spuLvl_step2 AS
SELECT
b.*
,rank_sale_l1wk - rank_sale_l2wk                                                                        AS delta_rank_l12wk_sale
,rank_sale_l2wk - rank_sale_l3wk                                                                        AS delta_rank_l23wk_sale
,rank_sale_l3wk - rank_sale_l4wk                                                                        AS delta_rank_l34wk_sale
,rank_sale_l12wk - rank_sale_l34wk                                                                      AS delta_rank_l12_34wk_sale
,rank_sale_l1mth - rank_sale_l2mth                                                                      AS delta_rank_l12mth_sale
,0.6*ln(rank_sale_l1wk / rank_sale_l2wk) + 0.3*ln(rank_sale_l2wk / rank_sale_l3wk) + 0.1*ln(rank_sale_l3wk / rank_sale_l4wk) AS trend_sales_score
,if(rank_sale_l12mth <= 30, 1, 0)                                                                       AS rank_top30_ind
,if(rank_sale_l12mth > 50, 1, 0)                                                                        AS rank_btm50_ind
,nvl(num_sale_l1wk / num_sale_l1wk_cat4,0)                                                              AS pct_sale_sku_cat4_l1wk
,nvl(num_sale_l12wk / num_sale_l12wk_cat4,0)                                                            AS pct_sale_sku_cat4_l12wk
,nvl(num_sale_l1mth / num_sale_l1mth_cat4,0)                                                            AS pct_sale_sku_cat4_l1mth
,nvl(num_sale_l12mth / num_sale_l12mth_cat4,0)                                                          AS pct_sale_sku_cat4_l12mth
,nvl((num_sale_l1wk / num_sale_l1wk_cat4),0) - nvl((num_sale_l2wk / num_sale_l2wk_cat4),0)              AS trend_pct_sale_l12wk
,nvl((num_sale_l12wk / num_sale_l12wk_cat4),0) - nvl((num_sale_l34wk / num_sale_l34wk_cat4),0)          AS trend_pct_sale_l12_34wk
,nvl((num_sale_l1mth / num_sale_l1mth_cat4),0) - nvl((num_sale_l2mth / num_sale_l2mth_cat4),0)          AS trend_pct_sale_l12mth
,nvl(num_sale_l1wk / num_sale_l1wk_cat4_v2,0)                                                              AS pct_sale_sku_cat4_l1wk_v2
,nvl(num_sale_l12wk / num_sale_l12wk_cat4_v2,0)                                                            AS pct_sale_sku_cat4_l12wk_v2
,nvl(num_sale_l1mth / num_sale_l1mth_cat4_v2,0)                                                            AS pct_sale_sku_cat4_l1mth_v2
,nvl(num_sale_l12mth / num_sale_l12mth_cat4_v2,0)                                                          AS pct_sale_sku_cat4_l12mth_v2
,nvl((num_sale_l1wk / num_sale_l1wk_cat4_v2),0) - nvl((num_sale_l2wk / num_sale_l2wk_cat4_v2),0)           AS trend_pct_sale_l12wk_v2
,nvl((num_sale_l12wk / num_sale_l12wk_cat4_v2),0) - nvl((num_sale_l34wk / num_sale_l34wk_cat4_v2),0)       AS trend_pct_sale_l12_34wk_v2
,nvl((num_sale_l1mth / num_sale_l1mth_cat4_v2),0) - nvl((num_sale_l2mth / num_sale_l2mth_cat4_v2),0)       AS trend_pct_sale_l12mth_v2
,nvl(num_sale_l1wk_cat4 / num_sale_l1wk_all,0)                                                          AS pct_sale_cat4_l1wk
,nvl(num_sale_l12wk_cat4 / num_sale_l12wk_all,0)                                                        AS pct_sale_cat4_l12wk
,nvl(num_sale_l1mth_cat4 / num_sale_l1mth_all,0)                                                        AS pct_sale_cat4_l1mth
,nvl(num_sale_l12mth_cat4 / num_sale_l12mth_all,0)                                                      AS pct_sale_cat4_l12mth
,nvl((num_sale_l12wk_cat4 / num_sale_l12wk_all),0) - nvl((num_sale_l34wk_cat4 / num_sale_l34wk_all),0)  AS delta_pct_sale_cat4_l12_34wk
,nvl((num_sale_l1mth_cat4 / num_sale_l1mth_all),0) - nvl((num_sale_l2mth_cat4 / num_sale_l2mth_all),0)  AS delta_pct_sale_cat4_l12mth
,nvl(num_sale_l1wk_cat4_v2 / num_sale_l1wk_all_v2,0)                                                          AS pct_sale_cat4_l1wk_v2
,nvl(num_sale_l12wk_cat4_v2 / num_sale_l12wk_all_v2,0)                                                        AS pct_sale_cat4_l12wk_v2
,nvl(num_sale_l1mth_cat4_v2 / num_sale_l1mth_all_v2,0)                                                        AS pct_sale_cat4_l1mth_v2
,nvl(num_sale_l12mth_cat4_v2 / num_sale_l12mth_all_v2,0)                                                      AS pct_sale_cat4_l12mth_v2
,nvl((num_sale_l12wk_cat4_v2 / num_sale_l12wk_all_v2),0) - nvl((num_sale_l34wk_cat4_v2 / num_sale_l34wk_all_v2),0)  AS delta_pct_sale_cat4_l12_34wk_v2
,nvl((num_sale_l1mth_cat4_v2 / num_sale_l1mth_all_v2),0) - nvl((num_sale_l2mth_cat4_v2 / num_sale_l2mth_all_v2),0)  AS delta_pct_sale_cat4_l12mth_v2
,nvl((num_sale_l12wk_cat4_v2 / num_sale_l12wk_all_v3),0) - nvl((num_sale_l34wk_cat4_v2 / num_sale_l34wk_all_v3),0)  AS delta_pct_sale_cat4_l12_34wk_v3
,nvl((num_sale_l1mth_cat4_v2 / num_sale_l1mth_all_v3),0) - nvl((num_sale_l2mth_cat4_v2 / num_sale_l2mth_all_v3),0)  AS delta_pct_sale_cat4_l12mth_v3
,if((b.sales_year_v2 = 2018 AND b.commodity_year = 2017) OR (b.sales_year_v2 = 2017 AND b.commodity_year = 2016),1,0) AS prod_prev_yr_ind
,if((b.sales_year_v2 = 2018 AND b.commodity_year = 2018) OR (b.sales_year_v2 = 2017 AND b.commodity_year = 2017),1,0) AS prod_curr_yr_ind
,if((b.sales_year_v2 = 2018 AND b.commodity_year = 2019) OR (b.sales_year_v2 = 2017 AND b.commodity_year = 2018),1,0) AS prod_next_yr_ind
,if(e.pct_sale_cat4_p1mth_prev_yr IS NOT NULL, e.pct_sale_cat4_p1mth_prev_yr, e.pct_avg_sale_cat4_p1mth_prev_yr) AS pct_sale_cat_p1mth_prev_yr
,if(e.pct_sale_cat4_p2mth_prev_yr IS NOT NULL, e.pct_sale_cat4_p2mth_prev_yr, e.pct_avg_sale_cat4_p2mth_prev_yr) AS pct_sale_cat_p2mth_prev_yr
,if(e.pct_sale_cat4_p3mth_prev_yr IS NOT NULL, e.pct_sale_cat4_p3mth_prev_yr, e.pct_avg_sale_cat4_p3mth_prev_yr) AS pct_sale_cat_p3mth_prev_yr
,if(e.pct_sale_cat4_l1mth_prev_yr IS NOT NULL, e.pct_sale_cat4_l1mth_prev_yr, e.pct_avg_sale_cat4_l1mth_prev_yr) AS pct_sale_cat_l1mth_prev_yr
,if(e.pct_sale_cat4_l2mth_prev_yr IS NOT NULL, e.pct_sale_cat4_l2mth_prev_yr, e.pct_avg_sale_cat4_l2mth_prev_yr) AS pct_sale_cat_l2mth_prev_yr
,CASE WHEN (rank_sale_p1mth BETWEEN 1 AND 3)   AND num_sale_p1mth>0 THEN '0'      WHEN (rank_sale_p1mth BETWEEN 4  AND 6)  AND num_sale_p1mth>0  THEN '1'
      WHEN (rank_sale_p1mth BETWEEN 7 AND 10)  AND num_sale_p1mth>0 THEN '2'      WHEN (rank_sale_p1mth BETWEEN 11 AND 20) AND num_sale_p1mth>0 THEN '3'
      WHEN (rank_sale_p1mth BETWEEN 21 AND 30) AND num_sale_p1mth>0 THEN '4'
      ELSE '5'
      END AS y_rank_bin_v1a
,num_sale_p1mth / num_sale_p1mth_all  AS pct_sale_p1mth
,num_sale_p1mth / num_sale_p1mth_cat4 AS pct_sale_skuInCat4_p1mth
FROM (
	SELECT
	*
	,nvl((num_sale_l1wk / (num_sale_l1wk + num_sale_l2wk)),0)    AS trend_sale_l12wk
	,nvl((num_sale_l2wk / (num_sale_l2wk + num_sale_l3wk)),0)    AS trend_sale_l23wk
	,nvl((num_sale_l3wk / (num_sale_l3wk + num_sale_l4wk)),0)    AS trend_sale_l34wk
	,nvl((num_sale_l12wk / (num_sale_l12wk + num_sale_l34wk)),0) AS trend_sale_l12_34wk
	,nvl((num_sale_l1mth / (num_sale_l1mth + num_sale_l2mth)),0) AS trend_sale_l12mth
	,rank() OVER (PARTITION BY commodity_brand_name,sales_year_v2,commodity_style_v2 ORDER BY num_sale_l1wk DESC)   AS rank_sale_l1wk
	,rank() OVER (PARTITION BY commodity_brand_name,sales_year_v2,commodity_style_v2 ORDER BY num_sale_l2wk DESC)   AS rank_sale_l2wk
	,rank() OVER (PARTITION BY commodity_brand_name,sales_year_v2,commodity_style_v2 ORDER BY num_sale_l3wk DESC)   AS rank_sale_l3wk
	,rank() OVER (PARTITION BY commodity_brand_name,sales_year_v2,commodity_style_v2 ORDER BY num_sale_l4wk DESC)   AS rank_sale_l4wk
	,rank() OVER (PARTITION BY commodity_brand_name,sales_year_v2,commodity_style_v2 ORDER BY num_sale_l12wk DESC)  AS rank_sale_l12wk
	,rank() OVER (PARTITION BY commodity_brand_name,sales_year_v2,commodity_style_v2 ORDER BY num_sale_l34wk DESC)  AS rank_sale_l34wk
	,rank() OVER (PARTITION BY commodity_brand_name,sales_year_v2,commodity_style_v2 ORDER BY num_sale_l1mth DESC)  AS rank_sale_l1mth
	,rank() OVER (PARTITION BY commodity_brand_name,sales_year_v2,commodity_style_v2 ORDER BY num_sale_l2mth DESC)  AS rank_sale_l2mth
	,rank() OVER (PARTITION BY commodity_brand_name,sales_year_v2,commodity_style_v2 ORDER BY num_sale_l12mth DESC) AS rank_sale_l12mth
	,dense_rank() OVER (PARTITION BY commodity_brand_name,sales_year_v2,commodity_style_v2 ORDER BY num_sale_p1mth DESC)  AS rank_sale_p1mth
	FROM (
		SELECT
		commodity_brand_name,commodity_year,commodity_season,commodity_catname_two,sales_year_v2,commodity_style_v2,commodity_style_no,channel_ind,first_online_date
		,sum(if(wk_ind = 'L1st_wk',nvl(sale_commodity_num,0),0))                 AS num_sale_l1wk
		,sum(if(wk_ind = 'L2nd_wk',nvl(sale_commodity_num,0),0))                 AS num_sale_l2wk
		,sum(if(wk_ind = 'L3rd_wk',nvl(sale_commodity_num,0),0))                 AS num_sale_l3wk
		,sum(if(wk_ind = 'L4th_wk',nvl(sale_commodity_num,0),0))                 AS num_sale_l4wk
		,sum(if(wk_ind IN ('L1st_wk','L2nd_wk'),nvl(sale_commodity_num,0),0))    AS num_sale_l12wk
		,sum(if(wk_ind IN ('L3rd_wk','L4th_wk'),nvl(sale_commodity_num,0),0))    AS num_sale_l34wk
		,sum(if(mth_ind = 'L1st_mth',nvl(sale_commodity_num,0),0))               AS num_sale_l1mth
		,sum(if(mth_ind = 'L2nd_mth',nvl(sale_commodity_num,0),0))               AS num_sale_l2mth
		,sum(if(mth_ind IN ('L1st_mth','L2nd_mth'),nvl(sale_commodity_num,0),0)) AS num_sale_l12mth
		,sum(if(mth_ind = 'P1st_mth',nvl(sale_commodity_num,0),0))               AS num_sale_p1mth
		,max(online_days)                                                        AS online_days
		,min(new_prod_ind)                                                       AS new_prod_ind
		FROM (SELECT * FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_3 WHERE sales_year_v2 > 0) tmp
		GROUP BY commodity_brand_name,commodity_year,commodity_season,commodity_catname_two,sales_year_v2,commodity_style_v2,commodity_style_no,channel_ind,first_online_date
	) a
	WHERE num_sale_l12mth > 0
) b
LEFT JOIN (
	SELECT
	commodity_brand_name,sales_year_v2,commodity_style_v2
	,sum(if(wk_ind = 'L1st_wk',nvl(sale_commodity_num,0),0))                 AS num_sale_l1wk_cat4
	,sum(if(wk_ind = 'L2nd_wk',nvl(sale_commodity_num,0),0))                 AS num_sale_l2wk_cat4
	,sum(if(wk_ind IN ('L1st_wk','L2nd_wk'),nvl(sale_commodity_num,0),0))    AS num_sale_l12wk_cat4
	,sum(if(wk_ind IN ('L3rd_wk','L4th_wk'),nvl(sale_commodity_num,0),0))    AS num_sale_l34wk_cat4
	,sum(if(mth_ind = 'L1st_mth',nvl(sale_commodity_num,0),0))               AS num_sale_l1mth_cat4
	,sum(if(mth_ind = 'L2nd_mth',nvl(sale_commodity_num,0),0))               AS num_sale_l2mth_cat4
	,sum(if(mth_ind IN ('L1st_mth','L2nd_mth'),nvl(sale_commodity_num,0),0)) AS num_sale_l12mth_cat4
	,sum(if(mth_ind = 'P1st_mth',nvl(sale_commodity_num,0),0)) 				 AS num_sale_p1mth_cat4
	FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_3
	GROUP BY commodity_brand_name,sales_year_v2,commodity_style_v2
) c
ON b.commodity_brand_name = c.commodity_brand_name AND b.commodity_style_v2 = c.commodity_style_v2 AND b.sales_year_v2 = c.sales_year_v2
LEFT JOIN (
	SELECT
	commodity_brand_name,sales_year_v2
	,sum(if(wk_ind = 'L1st_wk',nvl(sale_commodity_num,0),0))                 AS num_sale_l1wk_all
	,sum(if(wk_ind IN ('L1st_wk','L2nd_wk'),nvl(sale_commodity_num,0),0))    AS num_sale_l12wk_all
	,sum(if(wk_ind IN ('L3rd_wk','L4th_wk'),nvl(sale_commodity_num,0),0))    AS num_sale_l34wk_all
	,sum(if(mth_ind = 'L1st_mth',nvl(sale_commodity_num,0),0))               AS num_sale_l1mth_all
	,sum(if(mth_ind = 'L2nd_mth',nvl(sale_commodity_num,0),0))               AS num_sale_l2mth_all
	,sum(if(mth_ind IN ('L1st_mth','L2nd_mth'),nvl(sale_commodity_num,0),0)) AS num_sale_l12mth_all
	,sum(if(mth_ind = 'P1st_mth',nvl(sale_commodity_num,0),0)) 				 AS num_sale_p1mth_all
	FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_3
	GROUP BY commodity_brand_name,sales_year_v2
) d
ON b.commodity_brand_name = d.commodity_brand_name AND b.sales_year_v2 = d.sales_year_v2
LEFT JOIN (
	SELECT
	commodity_brand_name,commodity_year,sales_year_v2,commodity_style_v2
	,sum(if(wk_ind = 'L1st_wk',nvl(sale_commodity_num,0),0))                 AS num_sale_l1wk_cat4_v2
	,sum(if(wk_ind = 'L2nd_wk',nvl(sale_commodity_num,0),0))                 AS num_sale_l2wk_cat4_v2
	,sum(if(wk_ind IN ('L1st_wk','L2nd_wk'),nvl(sale_commodity_num,0),0))    AS num_sale_l12wk_cat4_v2
	,sum(if(wk_ind IN ('L3rd_wk','L4th_wk'),nvl(sale_commodity_num,0),0))    AS num_sale_l34wk_cat4_v2
	,sum(if(mth_ind = 'L1st_mth',nvl(sale_commodity_num,0),0))               AS num_sale_l1mth_cat4_v2
	,sum(if(mth_ind = 'L2nd_mth',nvl(sale_commodity_num,0),0))               AS num_sale_l2mth_cat4_v2
	,sum(if(mth_ind IN ('L1st_mth','L2nd_mth'),nvl(sale_commodity_num,0),0)) AS num_sale_l12mth_cat4_v2
	FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_3
	GROUP BY commodity_brand_name,commodity_year,sales_year_v2,commodity_style_v2
) c2
ON b.commodity_brand_name = c2.commodity_brand_name AND b.commodity_year = c2.commodity_year AND b.commodity_style_v2 = c2.commodity_style_v2 AND b.sales_year_v2 = c2.sales_year_v2
LEFT JOIN (
	SELECT
	commodity_brand_name,commodity_year,sales_year_v2
	,sum(if(wk_ind = 'L1st_wk',nvl(sale_commodity_num,0),0))                 AS num_sale_l1wk_all_v2
	,sum(if(wk_ind IN ('L1st_wk','L2nd_wk'),nvl(sale_commodity_num,0),0))    AS num_sale_l12wk_all_v2
	,sum(if(wk_ind IN ('L3rd_wk','L4th_wk'),nvl(sale_commodity_num,0),0))    AS num_sale_l34wk_all_v2
	,sum(if(mth_ind = 'L1st_mth',nvl(sale_commodity_num,0),0))               AS num_sale_l1mth_all_v2
	,sum(if(mth_ind = 'L2nd_mth',nvl(sale_commodity_num,0),0))               AS num_sale_l2mth_all_v2
	,sum(if(mth_ind IN ('L1st_mth','L2nd_mth'),nvl(sale_commodity_num,0),0)) AS num_sale_l12mth_all_v2
	FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_3
	GROUP BY commodity_brand_name,commodity_year,sales_year_v2
) d2
ON b.commodity_brand_name = d2.commodity_brand_name AND b.commodity_year = d2.commodity_year AND b.sales_year_v2 = d2.sales_year_v2
LEFT JOIN (
	SELECT
	commodity_brand_name,commodity_year,commodity_catname_two,sales_year_v2
	,sum(if(wk_ind = 'L1st_wk',nvl(sale_commodity_num,0),0))                 AS num_sale_l1wk_all_v3
	,sum(if(wk_ind IN ('L1st_wk','L2nd_wk'),nvl(sale_commodity_num,0),0))    AS num_sale_l12wk_all_v3
	,sum(if(wk_ind IN ('L3rd_wk','L4th_wk'),nvl(sale_commodity_num,0),0))    AS num_sale_l34wk_all_v3
	,sum(if(mth_ind = 'L1st_mth',nvl(sale_commodity_num,0),0))               AS num_sale_l1mth_all_v3
	,sum(if(mth_ind = 'L2nd_mth',nvl(sale_commodity_num,0),0))               AS num_sale_l2mth_all_v3
	,sum(if(mth_ind IN ('L1st_mth','L2nd_mth'),nvl(sale_commodity_num,0),0)) AS num_sale_l12mth_all_v3
	FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_3
	GROUP BY commodity_brand_name,commodity_year,commodity_catname_two,sales_year_v2
) d3
ON b.commodity_brand_name = d3.commodity_brand_name AND b.commodity_year = d3.commodity_year AND b.commodity_catname_two = d3.commodity_catname_two AND b.sales_year_v2 = d3.sales_year_v2
LEFT JOIN belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_2 e
ON b.commodity_brand_name = e.commodity_brand_name AND b.sales_year_v2 = e.sales_year_v2_for_pred AND b.commodity_style_v2 = e.commodity_style_v2
;"""
        self.call()

if __name__ == '__main__':
    Hgautoonlinerankpredmasterspulvlstep2().run_command()
