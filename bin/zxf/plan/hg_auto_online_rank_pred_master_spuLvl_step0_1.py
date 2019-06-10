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


class Hgautoonlinerankpredmasterspulvlstep01(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.hg_auto_online_rank_pred_master_spuLvl_step0_1;
CREATE TABLE belle_sh.hg_auto_online_rank_pred_master_spuLvl_step0_1 AS
SELECT
commodity_brand_name,commodity_season,commodity_style_no,commodity_supplier_code,commodity_no,commodity_catname_one,commodity_catname_three
,commodity_style,commodity_status,channel,platform,report_date,delivery_type,commodity_year,sales_year_v2,sales_month,first_online_date
,commodity_catname_two_new AS commodity_catname_two
,if(commodity_catname_two_new = '{low_sales_cat2}', commodity_catname_two_new, concat(commodity_style,'_',commodity_catname_two_new))      AS commodity_style_v2
,cast(sale_commodity_num AS bigint) 			                            AS sale_commodity_num
,if(channel = '电商专销' OR commodity_supplier_code = 'BBLBYZ81DL1DC7',1,0)  AS channel_ind
,CASE WHEN datediff(concat(sales_year_v2,'{cur_mth_dt}'),report_date) BETWEEN 1 AND 7   THEN 'L1st_wk'
	  WHEN datediff(concat(sales_year_v2,'{cur_mth_dt}'),report_date) BETWEEN 8 AND 14  THEN 'L2nd_wk'
	  WHEN datediff(concat(sales_year_v2,'{cur_mth_dt}'),report_date) BETWEEN 15 AND 21 THEN 'L3rd_wk'
	  WHEN datediff(concat(sales_year_v2,'{cur_mth_dt}'),report_date) BETWEEN 22 AND 28 THEN 'L4th_wk'
	  WHEN datediff(concat(sales_year_v2,'{cur_mth_dt}'),report_date) BETWEEN 29 AND 35 THEN 'L5th_wk'
	  WHEN datediff(concat(sales_year_v2,'{cur_mth_dt}'),report_date) BETWEEN 36 AND 42 THEN 'L6th_wk'
	  WHEN datediff(concat(sales_year_v2,'{cur_mth_dt}'),report_date) BETWEEN 43 AND 49 THEN 'L7th_wk'
	  WHEN datediff(concat(sales_year_v2,'{cur_mth_dt}'),report_date) BETWEEN 50 AND 56 THEN 'L8th_wk'
	  ELSE 'N/A'
	  END AS wk_ind
,CASE WHEN datediff(report_date,concat(sales_year_v2,'-','{p1st_mth}','-01')) BETWEEN 0 AND 29 	THEN 'P1st_mth'
	  WHEN datediff(report_date,concat(sales_year_v2,'-','{p2nd_mth}','-01')) BETWEEN 0 AND 29  THEN 'P2nd_mth'
	  WHEN datediff(report_date,concat(sales_year_v2,'-','{p3rd_mth}','-01')) BETWEEN 0 AND 29  THEN 'P3rd_mth'
	  WHEN datediff(concat(sales_year_v2,'{cur_mth_dt}'),report_date) 		  BETWEEN 1 AND 30  THEN 'L1st_mth'
	  WHEN datediff(concat(sales_year_v2,'{cur_mth_dt}'),report_date) 		  BETWEEN 31 AND 60 THEN 'L2nd_mth'
	  ELSE 'N/A'
	  END AS mth_ind
,CASE WHEN (cast('{p1st_mth}'     AS bigint) BETWEEN 1 AND 8) AND (commodity_year >= sales_year_v2)                               THEN 1
      WHEN ((cast('{p1st_mth}'    AS bigint)>=9) AND (commodity_season IN ('秋','冬')) AND (commodity_year >= sales_year_v2))
           OR ((cast('{p1st_mth}' AS bigint)>=9) AND (commodity_season IN ('春','夏')) AND (commodity_year > sales_year_v2))      THEN 1
      WHEN (cast('{p1st_mth}'     AS bigint)<=2) AND (commodity_season IN ('秋','冬')) AND (commodity_year >= sales_year_v2 - 1)  THEN 1
      ELSE 0
      END AS new_prod_ind
FROM (
    SELECT
    a1.*
	,nvl(if(a1.commodity_catname_two = '女靴', '靴', a3.commodity_catname_two_new),'N/A') AS commodity_catname_two_new
	,nvl(if(a1.commodity_catname_two <> '女靴', a3.commodity_style, a2.prop_value),'N/A') AS commodity_style
    ,if(substr(a4.commodity_first_sell_date,1,10) IS NULL, a6.report_date_sku_min, substr(a4.commodity_first_sell_date,1,10))   AS first_online_date
    FROM (
    	SELECT
    	tmp1.*,tmp2.commodity_style_no
    	,cast(tmp2.commodity_years AS bigint) AS commodity_year
    	,CASE WHEN (cast('{p1st_mth}' AS bigint) <= 4) THEN if(sales_month<=(4+2), sales_year, if(sales_month>=9, sales_year+1, -999))
    	      WHEN (cast('{p1st_mth}' AS bigint) >= 11) THEN if(sales_month<=2, sales_year - 1, if(sales_month >= (11-4), sales_year, -999))
    	      ELSE sales_year
    	      END AS sales_year_v2
    	FROM (
    		SELECT
    		commodity_brand_name,commodity_season,commodity_supplier_code,commodity_no,commodity_catname_one,commodity_catname_three
			,commodity_status,channel,platform,report_date,delivery_type,sale_commodity_num
		    ,cast(substr(report_date,1,4) AS bigint) 						    AS sales_year
		    ,cast(substr(report_date,6,2) AS bigint) 						    AS sales_month
		    ,if(commodity_catname_two = '女士凉鞋', '女凉', if(commodity_catname_two = '女士单鞋', '女单', commodity_catname_two)) AS commodity_catname_two
    		FROM belle_sh.dm_online_sale_for_billz
    		WHERE commodity_catname_one = '女鞋' AND platform = '淘宝' AND commodity_brand_name IN ('百丽')
    	) tmp1
    	LEFT JOIN (
    		SELECT
			commodity_supplier_code,commodity_style_no,commodity_years
			FROM belle_sh.commodity_base_info
			WHERE commodity_catname_one = '女鞋' AND commodity_brand_name IN ('百丽')
				  AND cast(substr(commodity_supplier_code,-1,1) AS bigint) = cast(substr(commodity_style_no,-1,1) AS bigint)
    	) tmp2
    	ON tmp1.commodity_supplier_code = tmp2.commodity_supplier_code
    ) a1
    LEFT JOIN (SELECT * FROM belle_sh.hg_tbl_commodity_extend_prop_clean) a2
    ON a1.commodity_no=a2.commodity_no
    LEFT JOIN (
		SELECT
		commodity_supplier_code, commodity_style, commodity_catname_two_new
		FROM belle_sh.hg_sku_commodity_style_mapping_formodeling_v2
		WHERE commodity_style <> 'N/A'
	) a3
    ON a1.commodity_supplier_code=a3.commodity_supplier_code
    LEFT JOIN (
		SELECT
		commodity_supplier_code, commodity_first_sell_date
		FROM belle_sh.dm_commodity_styleno_commodity_first_date
    	WHERE platform = '淘宝' AND commodity_first_sell_date IS NOT NULL
	) a4
    ON a1.commodity_supplier_code = a4.commodity_supplier_code
	LEFT JOIN (
		SELECT
		commodity_supplier_code
		,min(report_date) AS report_date_sku_min
		FROM (
			SELECT
			*
			FROM belle_sh.dm_online_sale_for_billz
			WHERE commodity_catname_one = '女鞋' AND platform = '淘宝' AND commodity_brand_name IN ('百丽')
		) aa6
		GROUP BY commodity_supplier_code
	) a6
	ON a1.commodity_supplier_code=a6.commodity_supplier_code
) a
WHERE sales_year_v2 >= 2016 AND (commodity_year BETWEEN (sales_year_v2-1) AND (sales_year_v2+1))
;"""
        self.call()

if __name__ == '__main__':
    Hgautoonlinerankpredmasterspulvlstep01().run_command()
