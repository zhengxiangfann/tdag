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


class Mzskuweek2018cpz(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP TABLE belle_sh.mz_sku_week_2018_cpz;


CREATE TABLE belle_sh.mz_sku_week_2018_cpz AS
SELECT d.*,
       CASE
           WHEN (num_rank_season)/(cnt_sku_week_season) <= 0.2 THEN '畅销款'
           WHEN (num_rank_season)/(cnt_sku_week_season) <= 0.8 THEN '平销款'
           ELSE '滞销款'
       END AS ind_cpz_season
FROM
  (SELECT c.*,
          row_number() over(PARTITION BY weekYearSale, seasonYearSKU
                            ORDER BY last_2_week_qty DESC) AS num_rank_season,
          count(DISTINCT product_code) over(PARTITION BY weekYearSale, seasonYearSKU) AS cnt_sku_week_season
   FROM
     (SELECT product_code,
             weekYearSale,
             product_year_name,
             seasonYearSKU,
             qty+nvl(qty_L1, 0) AS last_2_week_qty
      FROM
        (SELECT a.*,
                lag(qty, 1) over(PARTITION BY product_code
                                 ORDER BY weekYearSale) AS qty_L1
         FROM
           (SELECT product_code,
                   weekYearSale,
                   product_year_name,
                   seasonYearSKU,
                   sum(qty) AS qty
            FROM belle_sh.mz_sku_store_size_week
            GROUP BY product_code,
                     weekYearSale,
                     product_year_name,
                     seasonYearSKU) a) b) c) d"""
        self.call()

if __name__ == '__main__':
    Mzskuweek2018cpz().run_command()
