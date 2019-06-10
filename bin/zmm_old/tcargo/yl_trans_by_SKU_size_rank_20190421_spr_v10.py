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


class Yltransbyskusizerank20190421sprv10(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_size_rank_20190421_spr_v10;
CREATE TABLE belle_sh.yl_trans_by_SKU_size_rank_20190421_spr_v10 AS
SELECT
product_code
,size_no
,rank() over(PARTITION BY product_code ORDER BY inv_end_sku_size,size_no DESC) AS rank_size
FROM (
      SELECT
      product_code
      ,size_no
      ,sum(nvl(in_inv,0)) AS inv_end_sku_size
      FROM belle_sh.hg_shanghai_19_sale_inv a
      WHERE product_year_name=2019
      AND size_no>220 AND size_no<245
      AND inv_date=20190421
      GROUP BY
      product_code
      ,size_no
     ) a
;
SET hive.support.quoted.identifiers=NONE;"""
        self.call()

if __name__ == '__main__':
    Yltransbyskusizerank20190421sprv10().run_command()
