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


class Mzweekskuareasale(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.mz_week_sku_area_sale;
CREATE TABLE belle_sh.mz_week_sku_area_sale AS
SELECT *,
(mean4_sku_area - tr4_sku_area*(-1.5)) AS itcpt_sku_area
FROM(
    SELECT *,
    (1.5*(cnt_qty_sku_area - mean4_sku_area)+0.5*(cnt_qty_sku_area_L1 - mean4_sku_area)-0.5*(cnt_qty_sku_area_L2 - mean4_sku_area)-1.5*(cnt_qty_sku_area_L3 - mean4_sku_area)) AS tr4_sku_area
    FROM(
        SELECT *,
        (cnt_qty_sku_area + cnt_qty_sku_area_L1 + cnt_qty_sku_area_L2 + cnt_qty_sku_area_L3)/4 AS mean4_sku_area,
        (cnt_qty_sku_area + cnt_qty_sku_area_L1 )/2 AS mean2_sku_area,
        (cnt_qty_sku_area_F1 + cnt_qty_sku_area_F2) AS futr2_sku_area
        FROM(
            SELECT *,
            LAG(cnt_qty_sku_area, 1,NULL) OVER (PARTITION BY product_code, seasonYearSKU ORDER BY weekYearSale ASC) AS cnt_qty_sku_area_L1,
            LAG(cnt_qty_sku_area, 2,NULL) OVER (PARTITION BY product_code, seasonYearSKU ORDER BY weekYearSale ASC) AS cnt_qty_sku_area_L2,
            LAG(cnt_qty_sku_area, 3,NULL) OVER (PARTITION BY product_code, seasonYearSKU ORDER BY weekYearSale ASC) AS cnt_qty_sku_area_L3,
            lead(cnt_qty_sku_area, 1,NULL) OVER (PARTITION BY product_code, seasonYearSKU ORDER BY weekYearSale ASC) AS cnt_qty_sku_area_F1,
            lead(cnt_qty_sku_area, 2,NULL) OVER (PARTITION BY product_code, seasonYearSKU ORDER BY weekYearSale ASC) AS cnt_qty_sku_area_F2
            FROM(
                SELECT weekYearSale, product_code, seasonYearSKU,
                sum(qty) AS cnt_qty_sku_area,
                sum(sum_amt) AS sum_amt
                FROM belle_sh.mz_sku_store_size_week
                GROUP BY weekYearSale, product_code, seasonYearSKU
                ) sku_area_1
            ) sku_area_2
        ) sku_area_3
    ) sku_area_4"""
        self.call()

if __name__ == '__main__':
    Mzweekskuareasale().run_command()
