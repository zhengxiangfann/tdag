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


class Mzskustartweek(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP TABLE belle_sh.mz_sku_start_week;


CREATE TABLE belle_sh.mz_sku_start_week AS
SELECT a.*,
       datediff(to_date(from_unixtime(UNIX_TIMESTAMP(sunday_date, 'yyyyMMdd'))),
                to_date(from_unixtime(UNIX_TIMESTAMP(start_date, 'yyyyMMdd')))) AS online_days
FROM
  (SELECT product_code,
          seasonYearSKU,
          min(date_inv_end) AS start_date,
          from_unixtime(unix_timestamp(date_sub(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), pmod(datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), '2012-01-01'), 7)), 'yyyy-MM-dd'), 'yyyyMMdd') AS sunday_date
   FROM belle_sh.mz_sku_store_size_week
   GROUP BY product_code,
            seasonYearSKU) a"""
        self.call()

if __name__ == '__main__':
    Mzskustartweek().run_command()
