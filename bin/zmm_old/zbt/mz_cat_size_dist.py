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


class Mzcatsizedist(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE  belle_sh.mz_cat_size_dist;
CREATE TABLE belle_sh.mz_cat_size_dist AS
SELECT
    c.*
    ,sum_qty_220/sum_qty AS pct_qty_220
    ,sum_qty_225/sum_qty AS pct_qty_225
    ,sum_qty_230/sum_qty AS pct_qty_230
    ,sum_qty_235/sum_qty AS pct_qty_235
    ,sum_qty_240/sum_qty AS pct_qty_240
    ,sum_qty_245/sum_qty AS pct_qty_245
    ,cumu_qty_220/cumu_qty AS pct_cumu_qty_220
    ,cumu_qty_225/cumu_qty AS pct_cumu_qty_225
    ,cumu_qty_230/cumu_qty AS pct_cumu_qty_230
    ,cumu_qty_235/cumu_qty AS pct_cumu_qty_235
    ,cumu_qty_240/cumu_qty AS pct_cumu_qty_240
    ,cumu_qty_245/cumu_qty AS pct_cumu_qty_245
    ,sum_qty_220_lastyear/sum_qty_lastyear AS pct_qty_220_lastyear
    ,sum_qty_225_lastyear/sum_qty_lastyear AS pct_qty_225_lastyear
    ,sum_qty_230_lastyear/sum_qty_lastyear AS pct_qty_230_lastyear
    ,sum_qty_235_lastyear/sum_qty_lastyear AS pct_qty_235_lastyear
    ,sum_qty_240_lastyear/sum_qty_lastyear AS pct_qty_240_lastyear
    ,sum_qty_245_lastyear/sum_qty_lastyear AS pct_qty_245_lastyear
    ,if(sum_qty_lastyear IS NULL, 1,least(1, cumu_qty/sum_qty_lastyear/2) ) AS factor
FROM
(
    SELECT
    a.*
    ,sum(sum_qty_220) over(PARTITION BY a.product_year_name,a.product_season_name,a.category_name3,a.heel_type_name ORDER BY a.weekYearSale ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_qty_220
    ,sum(sum_qty_225) over(PARTITION BY a.product_year_name,a.product_season_name,a.category_name3,a.heel_type_name ORDER BY a.weekYearSale ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_qty_225
    ,sum(sum_qty_230) over(PARTITION BY a.product_year_name,a.product_season_name,a.category_name3,a.heel_type_name ORDER BY a.weekYearSale ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_qty_230
    ,sum(sum_qty_235) over(PARTITION BY a.product_year_name,a.product_season_name,a.category_name3,a.heel_type_name ORDER BY a.weekYearSale ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_qty_235
    ,sum(sum_qty_240) over(PARTITION BY a.product_year_name,a.product_season_name,a.category_name3,a.heel_type_name ORDER BY a.weekYearSale ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_qty_240
    ,sum(sum_qty_245) over(PARTITION BY a.product_year_name,a.product_season_name,a.category_name3,a.heel_type_name ORDER BY a.weekYearSale ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_qty_245
    ,sum(sum_qty) over(PARTITION BY a.product_year_name,a.product_season_name,a.category_name3,a.heel_type_name ORDER BY a.weekYearSale ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumu_qty
    ,sum_qty_220_lastyear
    ,sum_qty_225_lastyear
    ,sum_qty_230_lastyear
    ,sum_qty_235_lastyear
    ,sum_qty_240_lastyear
    ,sum_qty_245_lastyear
    ,sum_qty_lastyear
FROM belle_sh.mz_cat_size_stat a
LEFT JOIN
(
    SELECT
    product_year_name
    ,cast(cast(product_year_name AS int)+1 AS string) AS product_year_match
    ,product_season_name
    ,category_name3
    ,heel_type_name
    ,sum(sum_qty_220) AS sum_qty_220_lastyear
    ,sum(sum_qty_225) AS sum_qty_225_lastyear
    ,sum(sum_qty_230) AS sum_qty_230_lastyear
    ,sum(sum_qty_235) AS sum_qty_235_lastyear
    ,sum(sum_qty_240) AS sum_qty_240_lastyear
    ,sum(sum_qty_245) AS sum_qty_245_lastyear
    ,sum(sum_qty)     AS sum_qty_lastyear
    FROM belle_sh.mz_cat_size_stat
    GROUP BY
    product_year_name
    ,cast(cast(product_year_name AS int)+1 AS string)
    ,product_season_name
    ,category_name3
    ,heel_type_name
) b
ON a.product_year_name=b.product_year_match AND a.product_season_name=b.product_season_name AND a.category_name3=b.category_name3 AND a.heel_type_name=b.heel_type_name
) c"""
        self.call()

if __name__ == '__main__':
    Mzcatsizedist().run_command()
