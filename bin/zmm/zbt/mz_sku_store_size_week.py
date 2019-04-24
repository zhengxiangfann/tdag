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


class Mzskustoresizeweek(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP TABLE belle_sh.mz_sku_store_size_week;


CREATE TABLE belle_sh.mz_sku_store_size_week AS
SELECT grpweek.*,
       B.in_inv,
       concat_ws('-',
                 CAST(grpweek.sale_year AS VARCHAR(20)),
                 substring(concat('00', CAST(grpweek.week AS VARCHAR(20))), -2)) AS weekYearSale,
       concat_ws('-',
                 grpweek.product_season_name,
                 grpweek.product_year_name) AS seasonYearSKU
FROM
  (SELECT store_no,
          product_code,
          size_no,
          WEEK,
          sale_year,
          product_season_name,
          product_year_name,
          product_dsn,
          max(inv_date) AS date_inv_end,
          sum(nvl(qty, 0)) AS qty,
          sum(nvl(amount, 0)) AS sum_amt,
          sum(nvl(tag_amount, 0)) AS sum_tag_amt,
          sum(nvl(amount, 0))/sum(nvl(tag_amount, 0)) AS rto_discount,
          count(distinct(inv_date)) AS sale_days,
          sum(nvl(qty, 0))/count(distinct(inv_date)) AS rto_sell
   FROM
     (SELECT addweek.*,
             CASE
                 WHEN sale_month=12
                      AND WEEK=1 THEN (year(to_date(from_unixtime(unix_timestamp(inv_date, 'yyyyMMdd'))))+1)
                 ELSE (year(to_date(from_unixtime(unix_timestamp(inv_date, 'yyyyMMdd')))))
             END AS sale_year
      FROM
        (SELECT *,
                weekofyear(to_date(from_unixtime(unix_timestamp(inv_date, 'yyyyMMdd')))) AS WEEK,
                month(to_date(from_unixtime(unix_timestamp(inv_date, 'yyyyMMdd')))) AS sale_month,
                substring(product_code, 1, 6) AS product_dsn
         FROM belle_sh.mz_sale_inv_st_2017_2019
         WHERE brand_no = 'ST'
           AND region_name = '华东' ) addweek) corweek
   GROUP BY store_no,
            product_code,
            size_no,
            WEEK,
            sale_year,
            product_season_name,
            product_year_name,
            product_dsn) grpweek
LEFT JOIN belle_sh.mz_sale_inv_st_2017_2019 B ON grpweek.store_no = B.store_no
AND grpweek.product_code = B.product_code
AND grpweek.size_no = B.size_no
AND grpweek.date_inv_end = B.inv_date"""
        self.call()

if __name__ == '__main__':
    Mzskustoresizeweek().run_command()
