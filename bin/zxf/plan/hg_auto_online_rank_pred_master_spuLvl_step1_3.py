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


class Hgautoonlinerankpredmasterspulvlstep13(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_3;
CREATE TABLE belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_3 AS
SELECT
a.*
FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_1 a
INNER JOIN (
	SELECT DISTINCT
	commodity_brand_name, sales_year_v2, commodity_style_v2
	FROM (
		SELECT
		tmp1.commodity_brand_name,tmp1.sales_year_v2,tmp1.commodity_catname_two,tmp1.commodity_style_v2
		,sale_cat4
		,sale_cat4 / sale_cat2 AS sale_ratio_cat4
		,spu_cnt_cat4
		FROM (
			SELECT
			commodity_brand_name,sales_year_v2,commodity_catname_two,commodity_style_v2
			,sum(nvl(sale_commodity_num,0)) AS sale_cat4
			,count(DISTINCT commodity_style_no) AS spu_cnt_cat4
			FROM (SELECT * FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_1 WHERE mth_ind IN ('L1st_mth','L2nd_mth')) aaa
			GROUP BY commodity_brand_name,sales_year_v2,commodity_catname_two,commodity_style_v2
		) tmp1
		LEFT JOIN (
			SELECT
			commodity_brand_name,sales_year_v2,commodity_catname_two
			,sum(nvl(sale_commodity_num,0)) AS sale_cat2
			,count(DISTINCT commodity_style_no) AS spu_cnt_cat2
			FROM (SELECT * FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step1_1 WHERE mth_ind IN ('L1st_mth','L2nd_mth')) bbb
			GROUP BY commodity_brand_name,sales_year_v2,commodity_catname_two
		) tmp2
		ON tmp1.commodity_brand_name = tmp2.commodity_brand_name AND tmp1.sales_year_v2 = tmp2.sales_year_v2 AND tmp1.commodity_catname_two = tmp2.commodity_catname_two
	) bb
	WHERE sale_ratio_cat4 >= 0.005
) b
ON a.commodity_brand_name = b.commodity_brand_name AND a.sales_year_v2 = b.sales_year_v2 AND a.commodity_style_v2 = b.commodity_style_v2
WHERE a.online_days > 0 AND a.sales_year_v2 >= 2017
;"""
        self.call()

if __name__ == '__main__':
    Hgautoonlinerankpredmasterspulvlstep13().run_command()
