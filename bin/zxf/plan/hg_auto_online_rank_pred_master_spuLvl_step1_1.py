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


class Hgautoonlinerankpredmasterspulvlstep11(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_1;
CREATE TABLE belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_1 AS
SELECT
aa.*
,if(commodity_year IN (sales_year_v2-1,sales_year_v2,sales_year_v2+1), datediff(concat(sales_year_v2,'{cur_mth_dt}'),first_online_date), -999) AS online_days
FROM (
	SELECT
	a.commodity_brand_name,a.commodity_year,a.commodity_season,a.commodity_style_no,a.commodity_catname_one
	,a.report_date,a.sales_year_v2,a.sales_month,a.new_prod_ind,a.wk_ind,a.mth_ind,a.sale_commodity_num
	,b.channel_ind
	,c.commodity_style_v2
	,d.first_online_date
	,e.commodity_catname_two AS commodity_catname_two
	FROM (
		SELECT
		commodity_brand_name,commodity_year,commodity_season,commodity_style_no,commodity_catname_one,commodity_catname_two,report_date,sales_year_v2,sales_month,wk_ind,mth_ind
		,sum(nvl(sale_commodity_num,0)) AS sale_commodity_num
		,max(new_prod_ind)              AS new_prod_ind
		FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step0_1
		GROUP BY commodity_brand_name,commodity_year,commodity_season,commodity_style_no,commodity_catname_one,commodity_catname_two,report_date,sales_year_v2,sales_month,wk_ind,mth_ind
	) a
	LEFT JOIN (
		SELECT
		sales_year_v2,commodity_style_no,channel_ind
		FROM (
			SELECT
			bbb.*
			,row_number() OVER (PARTITION BY sales_year_v2,commodity_style_no ORDER BY sale_commodity_num_gp DESC) AS rank_sale
			FROM (
				SELECT
				sales_year_v2,commodity_style_no,channel_ind,commodity_supplier_code
				,sum(if(mth_ind IN ('L1st_mth','L2nd_mth'), nvl(sale_commodity_num,0), 0)) AS sale_commodity_num_gp
				FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step0_1
				GROUP BY sales_year_v2,commodity_style_no,channel_ind,commodity_supplier_code
			) bbb
		) bb
		WHERE rank_sale = 1
	) b
	ON a.sales_year_v2 = b.sales_year_v2 AND a.commodity_style_no = b.commodity_style_no
	LEFT JOIN (
		SELECT
		sales_year_v2,commodity_style_no,commodity_style_v2
		FROM (
			SELECT
			ccc.*
			,row_number() OVER (PARTITION BY sales_year_v2,commodity_style_no ORDER BY sale_commodity_num_gp DESC) AS rank_sale
			FROM (
				SELECT
				sales_year_v2,commodity_style_no,commodity_style_v2,commodity_supplier_code
				,sum(if(mth_ind IN ('L1st_mth','L2nd_mth'), nvl(sale_commodity_num,0), 0)) AS sale_commodity_num_gp
				FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step0_1
				GROUP BY sales_year_v2,commodity_style_no,commodity_style_v2,commodity_supplier_code
			) ccc
		) cc
		WHERE rank_sale = 1
	) c
	ON a.sales_year_v2 = c.sales_year_v2 AND a.commodity_style_no = c.commodity_style_no
	LEFT JOIN (
		SELECT
		commodity_style_no
		,if(min(first_online_date) IS NULL, min(report_date), min(first_online_date)) AS first_online_date
		FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step0_1
		GROUP BY commodity_style_no
	) d
	ON a.commodity_style_no=d.commodity_style_no
	LEFT JOIN (
		SELECT DISTINCT
		commodity_style_no, commodity_catname_two
		FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step0_1_for201906_asof20190513
		WHERE commodity_catname_two <> 'N/A'
	) e
	ON a.commodity_style_no=e.commodity_style_no
) aa
WHERE sales_year_v2 >= 2016 AND (commodity_year BETWEEN (sales_year_v2-1) AND (sales_year_v2+1))
;"""
        self.call()

if __name__ == '__main__':
    Hgautoonlinerankpredmasterspulvlstep11().run_command()
