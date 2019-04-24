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


class Mzstoreweekseasonsalefull(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP TABLE belle_sh.mz_store_week_season_sale_full;


CREATE TABLE belle_sh.mz_store_week_season_sale_full AS
SELECT store_no,
       weekYearSale,
       seasonYearSKU,
       cnt_qty_season_store,
       futr2_season_store,
       tr4_season_store,
       rto_l4w_yoy,
       mean2_season_store,
       mean4_season_store,
       futr2_season_store_LY1,
       ((futr2_season_store_LY1*rto_l4w_yoy + 2*mean4_season_store + 2*mean2_season_store)/3) AS futr2_season_store_pred_meg,
       ((((futr2_season_store_LY1*rto_l4w_yoy + 2*mean4_season_store + 2*mean2_season_store)/3)-futr2_season_store)/futr2_season_store) AS err_futr2_season_store_pred_meg
FROM
  (SELECT store_no,
          weekYearSale,
          seasonYearSKU,
          cnt_qty_season_store,
          mean2_season_store,
          mean4_season_store,
          mean4_season_store_LY1,
          futr2_season_store,
          futr2_season_store_LY1,
          tr4_season_store,
          itcpt_season_store,
          (mean4_season_store/mean4_season_store_LY1) AS rto_l4w_yoy
   FROM
     (SELECT A.store_no,
             A.weekYearSale,
             A.seasonYearSKU,
             A.cnt_qty_season_store,
             A.mean4_season_store,
             A.mean2_season_store,
             A.futr2_season_store,
             A.tr4_season_store,
             A.itcpt_season_store,
             B.mean4_season_store AS mean4_season_store_LY1,
             B.mean2_season_store AS mean2_season_store_LY1,
             B.futr2_season_store AS futr2_season_store_LY1,
             B.tr4_season_store AS tr4_season_store_LY1,
             B.itcpt_season_store AS itcpt_season_store_LY1
      FROM belle_sh.mz_store_week_season_sale A
      LEFT JOIN belle_sh.mz_store_week_season_sale B ON A.store_no = B.store_no
      AND A.week = B.week
      AND A.sale_year = B.sale_year + 1
      AND A.product_season_name = B.product_season_name
      AND cast(A.product_year_name AS int) = cast(B.product_year_name AS int) + 1) store_1) store_2
WHERE store_2.mean4_season_store_LY1 IS NOT NULL"""
        self.call()

if __name__ == '__main__':
    Mzstoreweekseasonsalefull().run_command()
