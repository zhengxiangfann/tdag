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


class Hgautoonlinerankpredmasterspulvlstep12(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_2;
CREATE TABLE belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_2 AS
SELECT
a.commodity_brand_name,a.sales_year_v2,a.commodity_style_v2
,a.sales_year_v2 + 1                                     		  AS sales_year_v2_for_pred
,a.num_sale_cat4_p1mth_prev_yr / b.num_sale_all_p1mth_prev_yr AS pct_sale_cat4_p1mth_prev_yr
,a.num_sale_cat4_p2mth_prev_yr / b.num_sale_all_p2mth_prev_yr AS pct_sale_cat4_p2mth_prev_yr
,a.num_sale_cat4_p3mth_prev_yr / b.num_sale_all_p3mth_prev_yr AS pct_sale_cat4_p3mth_prev_yr
,a.num_sale_cat4_l1mth_prev_yr / b.num_sale_all_l1mth_prev_yr AS pct_sale_cat4_l1mth_prev_yr
,a.num_sale_cat4_l2mth_prev_yr / b.num_sale_all_l2mth_prev_yr AS pct_sale_cat4_l2mth_prev_yr
,b.pct_avg_sale_cat4_p1mth_prev_yr,b.pct_avg_sale_cat4_p2mth_prev_yr,b.pct_avg_sale_cat4_p3mth_prev_yr,b.pct_avg_sale_cat4_l1mth_prev_yr,b.pct_avg_sale_cat4_l2mth_prev_yr
FROM (
	SELECT
	commodity_brand_name,sales_year_v2,commodity_style_v2
	,sum(if(mth_ind = 'P1st_mth',nvl(sale_commodity_num,0),0))                      AS num_sale_cat4_p1mth_prev_yr
	,sum(if(mth_ind = 'P2nd_mth',nvl(sale_commodity_num,0),0))                      AS num_sale_cat4_p2mth_prev_yr
	,sum(if(mth_ind = 'P3rd_mth',nvl(sale_commodity_num,0),0))                      AS num_sale_cat4_p3mth_prev_yr
	,sum(if(mth_ind = 'L1st_mth',nvl(sale_commodity_num,0),0))                      AS num_sale_cat4_l1mth_prev_yr
	,sum(if(mth_ind = 'L2nd_mth',nvl(sale_commodity_num,0),0))                      AS num_sale_cat4_l2mth_prev_yr
	FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_1 aa
	GROUP BY commodity_brand_name,sales_year_v2,commodity_style_v2
) a
LEFT JOIN (
	SELECT
	commodity_brand_name,sales_year_v2
	,sum(if(mth_ind = 'P1st_mth',nvl(sale_commodity_num,0),0))                      AS num_sale_all_p1mth_prev_yr
	,sum(if(mth_ind = 'P2nd_mth',nvl(sale_commodity_num,0),0))                      AS num_sale_all_p2mth_prev_yr
	,sum(if(mth_ind = 'P3rd_mth',nvl(sale_commodity_num,0),0))                      AS num_sale_all_p3mth_prev_yr
	,sum(if(mth_ind = 'L1st_mth',nvl(sale_commodity_num,0),0))                      AS num_sale_all_l1mth_prev_yr
	,sum(if(mth_ind = 'L2nd_mth',nvl(sale_commodity_num,0),0))                      AS num_sale_all_l2mth_prev_yr
	,1 / count(DISTINCT if(mth_ind = 'P1st_mth',commodity_style_v2,NULL))    AS pct_avg_sale_cat4_p1mth_prev_yr
	,1 / count(DISTINCT if(mth_ind = 'P2nd_mth',commodity_style_v2,NULL))    AS pct_avg_sale_cat4_p2mth_prev_yr
	,1 / count(DISTINCT if(mth_ind = 'P3rd_mth',commodity_style_v2,NULL))    AS pct_avg_sale_cat4_p3mth_prev_yr
	,1 / count(DISTINCT if(mth_ind = 'l1st_mth',commodity_style_v2,NULL))    AS pct_avg_sale_cat4_l1mth_prev_yr
	,1 / count(DISTINCT if(mth_ind = 'L2nd_mth',commodity_style_v2,NULL))    AS pct_avg_sale_cat4_l2mth_prev_yr
	FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_1 bb
	GROUP BY commodity_brand_name,sales_year_v2
) b
ON a.commodity_brand_name = b.commodity_brand_name AND a.sales_year_v2 = b.sales_year_v2
;"""
        self.call()

if __name__ == '__main__':
    Hgautoonlinerankpredmasterspulvlstep12().run_command()
