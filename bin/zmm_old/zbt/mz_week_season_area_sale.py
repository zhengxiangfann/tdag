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


class Mzweekseasonareasale(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.mz_week_season_area_sale;
CREATE TABLE belle_sh.mz_week_season_area_sale AS
SELECT *,
(mean4_season_area - tr4_season_area*(-1.5)) AS itcpt_season_area
FROM(
    SELECT *,
    (1.5*(cnt_qty_season_area - mean4_season_area)+0.5*(cnt_qty_season_area_L1 - mean4_season_area)-0.5*(cnt_qty_season_area_L2 - mean4_season_area)-1.5*(cnt_qty_season_area_L3 - mean4_season_area)) AS tr4_season_area
    FROM(
        SELECT *,
        (cnt_qty_season_area + cnt_qty_season_area_L1 + cnt_qty_season_area_L2 + cnt_qty_season_area_L3)/4 AS mean4_season_area,
        (cnt_qty_season_area + cnt_qty_season_area_L1 )/2 AS mean2_season_area,
        (cnt_qty_season_area_F1 + cnt_qty_season_area_F2) AS futr2_season_area
        FROM(
            SELECT *,
            LAG(cnt_qty_season_area, 1,NULL) OVER (PARTITION BY seasonYearSKU ORDER BY weekYearSale ASC) AS cnt_qty_season_area_L1,
            LAG(cnt_qty_season_area, 2,NULL) OVER (PARTITION BY seasonYearSKU ORDER BY weekYearSale ASC) AS cnt_qty_season_area_L2,
            LAG(cnt_qty_season_area, 3,NULL) OVER (PARTITION BY seasonYearSKU ORDER BY weekYearSale ASC) AS cnt_qty_season_area_L3,
            lead(cnt_qty_season_area, 1,NULL) OVER (PARTITION BY seasonYearSKU ORDER BY weekYearSale ASC) AS cnt_qty_season_area_F1,
            lead(cnt_qty_season_area, 2,NULL) OVER (PARTITION BY seasonYearSKU ORDER BY weekYearSale ASC) AS cnt_qty_season_area_F2
            FROM(
                SELECT weekYearSale, WEEK, sale_year, seasonYearSKU, product_season_name, product_year_name,
                sum(qty) AS cnt_qty_season_area,
                sum(sum_amt) AS sum_amt
                FROM belle_sh.mz_sku_store_size_week
                GROUP BY weekYearSale, WEEK, sale_year, seasonYearSKU, product_season_name, product_year_name
                ) season_area_1
            ) season_area_2
        ) season_area_3
    ) season_area_4"""
        self.call()

if __name__ == '__main__':
    Mzweekseasonareasale().run_command()
