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


class Mzskustoreweeksale(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.mz_sku_store_week_sale;
CREATE TABLE belle_sh.mz_sku_store_week_sale AS
SELECT sku_store.*,
(mean4_sku_store - tr4_sku_store*(-1.5)) AS itcpt_sku_store,
store.mean2_store,
store.mean4_store,
store.futr2_store_pred_meg,
store_season.mean2_season_store,
store_season.mean4_season_store,
store_season.futr2_season_store_pred_meg,
area_season.mean2_season_area,
area_season.mean4_season_area,
area_season.futr2_season_area_pred_meg,
area_sku.itcpt_sku_area,
area_sku.tr4_sku_area
FROM(
    SELECT *,
    (1.5*(cnt_qty_sku_store - mean4_sku_store)+0.5*(cnt_qty_sku_store_L1 - mean4_sku_store)-0.5*(cnt_qty_sku_store_L2 - mean4_sku_store)-1.5*(cnt_qty_sku_store_L3 - mean4_sku_store)) AS tr4_sku_store,
    if((ind_peak+ind_peak_L1+ind_peak_L2+ind_peak_L3)>0,1,0) AS ind_peak_last4
    FROM(
        SELECT *,
        nvl((cnt_qty_sku_store*(1-ind_peak) + cnt_qty_sku_store_L1*(1-ind_peak_L1) + cnt_qty_sku_store_L2*(1-ind_peak_L2) + cnt_qty_sku_store_L3*(1-ind_peak_L3))/(4-ind_peak-ind_peak_L1-ind_peak_L2-ind_peak_L3),1) AS mean4_sku_store,
        nvl((cnt_qty_sku_store*(1-ind_peak) + cnt_qty_sku_store_L1*(1-ind_peak_L1))/(2-ind_peak-ind_peak_L1),1) AS mean2_sku_store
        FROM (
            SELECT *,
                nvl(LAG(ind_peak, 1,0) OVER (PARTITION BY product_code,seasonYearSKU,store_no ORDER BY weekYearSale ASC),0) AS ind_peak_L1,
                nvl(LAG(ind_peak, 2,0) OVER (PARTITION BY product_code,seasonYearSKU,store_no ORDER BY weekYearSale ASC),0) AS ind_peak_L2,
                nvl(LAG(ind_peak, 3,0) OVER (PARTITION BY product_code,seasonYearSKU,store_no ORDER BY weekYearSale ASC),0) AS ind_peak_L3
            FROM (
                SELECT *,
                    CASE WHEN rh_last4 > 12 AND cnt_qty_sku_store > 5 THEN 1
                    ELSE 0 END AS ind_peak
                FROM(
                    SELECT *,
                    (cnt_qty_sku_store + cnt_qty_sku_store_L1 + cnt_qty_sku_store_L2 + cnt_qty_sku_store_L3)/4 AS mean4_last,
                    (cnt_qty_sku_store + cnt_qty_sku_store_L1 )/2 AS mean2_last,
                    (cnt_qty_sku_store_F1 + cnt_qty_sku_store_F2) AS futr2_sku_store,
                    (cnt_qty_sku_store_F1 + cnt_qty_sku_store_F2 + cnt_qty_sku_store_F3 + cnt_qty_sku_store_F4) AS futr4_sku_store,
                    CASE WHEN (cnt_qty_sku_store + cnt_qty_sku_store_L1 + cnt_qty_sku_store_L2 + cnt_qty_sku_store_L3)/4 > 0 THEN pow(cnt_qty_sku_store,2)/(cnt_qty_sku_store + cnt_qty_sku_store_L1 + cnt_qty_sku_store_L2 + cnt_qty_sku_store_L3)*4
                    ELSE 0 END AS rh_last4
                    FROM(
                        SELECT *,
                        nvl(LAG(cnt_qty_sku_store, 1,NULL) OVER (PARTITION BY product_code,seasonYearSKU,store_no ORDER BY weekYearSale ASC),0) AS cnt_qty_sku_store_L1,
                        nvl(LAG(cnt_qty_sku_store, 2,NULL) OVER (PARTITION BY product_code,seasonYearSKU,store_no ORDER BY weekYearSale ASC),0) AS cnt_qty_sku_store_L2,
                        nvl(LAG(cnt_qty_sku_store, 3,NULL) OVER (PARTITION BY product_code,seasonYearSKU,store_no ORDER BY weekYearSale ASC),0) AS cnt_qty_sku_store_L3,
                        nvl(lead(cnt_qty_sku_store, 1,NULL) OVER (PARTITION BY product_code,seasonYearSKU,store_no ORDER BY weekYearSale ASC),0) AS cnt_qty_sku_store_F1,
                        nvl(lead(cnt_qty_sku_store, 2,NULL) OVER (PARTITION BY product_code,seasonYearSKU,store_no ORDER BY weekYearSale ASC),0) AS cnt_qty_sku_store_F2,
                        nvl(lead(cnt_qty_sku_store, 3,NULL) OVER (PARTITION BY product_code,seasonYearSKU,store_no ORDER BY weekYearSale ASC),0) AS cnt_qty_sku_store_F3,
                        nvl(lead(cnt_qty_sku_store, 4,NULL) OVER (PARTITION BY product_code,seasonYearSKU,store_no ORDER BY weekYearSale ASC),0) AS cnt_qty_sku_store_F4
                        FROM(
                            SELECT product_code, store_no, weekYearSale, seasonYearSKU,
                            sum(qty) AS cnt_qty_sku_store,
                            sum(in_inv) AS in_inv,
                            sum(sum_amt) AS sum_amt
                            FROM belle_sh.mz_sku_store_size_week
                            GROUP BY product_code, store_no, weekYearSale, seasonYearSKU
                            ) sku_store_1
                        ) sku_store_2
                    ) sku_store_21
                ) sku_store_22
            ) sku_store_23
        ) sku_store_3
    ) sku_store
LEFT JOIN belle_sh.mz_store_week_sale_full store
    ON sku_store.store_no = store.store_no AND sku_store.weekYearSale = store.weekYearSale
LEFT JOIN belle_sh.mz_store_week_season_sale_full store_season
    ON sku_store.store_no = store_season.store_no AND sku_store.weekYearSale = store_season.weekYearSale AND sku_store.seasonYearSKU = store_season.seasonYearSKU
LEFT JOIN belle_sh.mz_week_season_area_sale_full area_season
    ON sku_store.weekYearSale = area_season.weekYearSale AND sku_store.seasonYearSKU = area_season.seasonYearSKU
LEFT JOIN belle_sh.mz_week_sku_area_sale area_sku
    ON sku_store.product_code = area_sku.product_code AND sku_store.weekYearSale = area_sku.weekYearSale AND sku_store.seasonYearSKU = area_sku.seasonYearSKU"""
        self.call()

if __name__ == '__main__':
    Mzskustoreweeksale().run_command()
