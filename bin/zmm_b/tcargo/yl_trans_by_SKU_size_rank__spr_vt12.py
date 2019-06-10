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

class Yltransbyskusizeranksprvt12(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_size_rank_{invsunday}_spr_vt12;
CREATE TABLE belle_sh.yl_trans_by_SKU_size_rank_{invsunday}_spr_vt12 AS
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
      WHERE size_no>220 AND size_no<245
      AND inv_date={invsunday}
      GROUP BY
      product_code
      ,size_no
     ) a
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Yltransbyskusizeranksprvt12().run_command()
