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


class Hgautoonlinerankpredmasterv3trainspulvl(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.hg_auto_online_rank_pred_master_v3_train_spuLvl;
CREATE TABLE belle_sh.hg_auto_online_rank_pred_master_v3_train_spuLvl AS
SELECT
commodity_brand_name,commodity_year,commodity_season,commodity_catname_two,sales_year_v2,commodity_style_v2,commodity_style_no,first_online_date,online_days,new_prod_ind
,y_rank_bin_v1a
,pct_sale_p1mth
,rank_sale_p1mth AS y_rank_sale
,num_sale_p1mth AS y_num_sale
,num_sale_l1wk,num_sale_l2wk,num_sale_l3wk,num_sale_l4wk,num_sale_l12wk,num_sale_l34wk,num_sale_l1mth,num_sale_l2mth,num_sale_l12mth
,trend_sale_l12wk,trend_sale_l23wk,trend_sale_l34wk,trend_sale_l12_34wk,trend_sale_l12mth
,rank_sale_l1wk,rank_sale_l2wk,rank_sale_l3wk,rank_sale_l4wk,rank_sale_l12wk,rank_sale_l34wk,rank_sale_l1mth,rank_sale_l2mth,rank_sale_l12mth
,delta_rank_l12wk_sale,delta_rank_l23wk_sale,delta_rank_l34wk_sale,delta_rank_l12_34wk_sale,delta_rank_l12mth_sale,trend_sales_score,rank_top30_ind,rank_btm50_ind
,pct_sale_sku_cat4_l1wk,pct_sale_sku_cat4_l12wk,pct_sale_sku_cat4_l1mth,pct_sale_sku_cat4_l12mth,trend_pct_sale_l12wk,trend_pct_sale_l12_34wk,trend_pct_sale_l12mth
,pct_sale_cat4_l1wk,pct_sale_cat4_l12wk,pct_sale_cat4_l1mth,pct_sale_cat4_l12mth,delta_pct_sale_cat4_l12_34wk,delta_pct_sale_cat4_l12mth
,nvl(pct_sale_cat_p1mth_prev_yr,0) AS pct_sale_cat_p1mth_prev_yr
,delta_pct_sale_cat_p1_p2mth_prev_yr,delta_pct_sale_cat_p1_p3mth_prev_yr,delta_pct_sale_cat_p1_l1mth_prev_yr,delta_pct_sale_cat_p1_l2mth_prev_yr
,pct_sale_sku_cat4_l1wk_v2, pct_sale_sku_cat4_l12wk_v2,pct_sale_sku_cat4_l1mth_v2,pct_sale_sku_cat4_l12mth_v2,trend_pct_sale_l12wk_v2,trend_pct_sale_l12_34wk_v2,trend_pct_sale_l12mth_v2
,pct_sale_cat4_l1wk_v2,pct_sale_cat4_l12wk_v2,pct_sale_cat4_l1mth_v2,pct_sale_cat4_l12mth_v2,delta_pct_sale_cat4_l12_34wk_v2,delta_pct_sale_cat4_l12mth_v2
,delta_pct_sale_cat4_l12_34wk_v3,delta_pct_sale_cat4_l12mth_v3
,rank_p1mth_prevYr_l12mth,rank_p1mth_prevYr_l1mth,rank_p1mth_prevYr_l1wk,delta_rank_p1mth_prevYr_l12mth,delta_rank_p1mth_prevYr_l1mth,delta_rank_p1mth_prevYr_l1wk
,pctSale_skuInCat4_p1mth_prevYr_l12mth,pctSale_skuInCat4_p1mth_prevYr_l1mth,pctSale_skuInCat4_p1mth_prevYr_l1wk
,delta_pctSale_skuInCat4_p1mth_prevYr_l12mth,delta_pctSale_skuInCat4_p1mth_prevYr_l1mth,delta_pctSale_skuInCat4_p1mth_prevYr_l1wk
,prod_prev_yr_ind,prod_curr_yr_ind,prod_next_yr_ind
,if(commodity_catname_two = '{main_cat2}','1','0') AS main_cat2_ind
,channel_ind
,if(online_days<=7,1,0)  AS new_in_1wk
,if(online_days<=14,1,0) AS new_in_2wk
,if(online_days<=30,1,0) AS new_in_1mth
,if(online_days<=61,1,0) AS new_in_2mth
FROM belle_sh.hg_auto_online_rank_pred_master_spuLvl_step3
WHERE sales_year_v2 = ({pred_yr} - 1)
;"""
        self.call()

if __name__ == '__main__':
    Hgautoonlinerankpredmasterv3trainspulvl().run_command()
