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


class Mzskuweek2018xjh(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP TABLE belle_sh.mz_sku_week_2018_xjh;


CREATE TABLE belle_sh.mz_sku_week_2018_xjh AS
SELECT B.*,
       datediff(to_date(from_unixtime(unix_timestamp(dateOld, 'yyyyMMdd'))),
                to_date(from_unixtime(unix_timestamp(date_inv_end, 'yyyyMMdd'))))/7 AS weekToOld
FROM
  (SELECT A.*,
          CASE
              WHEN A.product_season_name IN ('秋',
                                             '冬') THEN concat(cast((cast(product_year_name AS int)+1) AS VARCHAR(20)), '0301')
              WHEN A.product_season_name IN ('春',
                                             '夏') THEN concat(product_year_name, '0901')
              ELSE NULL
          END AS dateOld,
          CASE
              WHEN A.product_season_name IN ('秋',
                                             '冬')
                   AND A.date_inv_end < concat(cast((cast(product_year_name AS int)+1) AS VARCHAR(20)), '0301') THEN 'new'
              WHEN A.product_season_name IN ('春',
                                             '夏')
                   AND A.date_inv_end < concat(product_year_name, '0901') THEN 'new'
              ELSE 'old'
          END AS indNew
   FROM
     (SELECT product_code,
             weekYearSale,
             seasonYearSKU,
             product_season_name,
             product_year_name,
             date_inv_end
      FROM belle_sh.mz_sku_store_size_week
      GROUP BY product_code,
               weekYearSale,
               seasonYearSKU,
               product_season_name,
               product_year_name,
               date_inv_end) A) B"""
        self.call()

if __name__ == '__main__':
    Mzskuweek2018xjh().run_command()
