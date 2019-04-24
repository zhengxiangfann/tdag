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


class Mzweekseasonareasalefull(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP TABLE belle_sh.mz_week_season_area_sale_full;


CREATE TABLE belle_sh.mz_week_season_area_sale_full AS
SELECT weekYearSale,
       seasonYearSKU,
       cnt_qty_season_area,
       futr2_season_area,
       tr4_season_area,
       rto_l4w_yoy,
       mean2_season_area,
       mean4_season_area,
       futr2_season_area_LY1,
       ((futr2_season_area_LY1*rto_l4w_yoy + 2*mean4_season_area + 2*mean2_season_area)/3) AS futr2_season_area_pred_meg,
       ((((futr2_season_area_LY1*rto_l4w_yoy + 2*mean4_season_area + 2*mean2_season_area)/3)-futr2_season_area)/futr2_season_area) AS err_futr2_season_area_pred_meg
FROM
  (SELECT weekYearSale,
          seasonYearSKU,
          cnt_qty_season_area,
          mean2_season_area,
          mean4_season_area,
          mean4_season_area_LY1,
          futr2_season_area,
          futr2_season_area_LY1,
          tr4_season_area,
          itcpt_season_area,
          (mean4_season_area/mean4_season_area_LY1) AS rto_l4w_yoy
   FROM
     (SELECT A.weekYearSale,
             A.seasonYearSKU,
             A.cnt_qty_season_area,
             A.mean4_season_area,
             A.mean2_season_area,
             A.futr2_season_area,
             A.tr4_season_area,
             A.itcpt_season_area,
             B.mean4_season_area AS mean4_season_area_LY1,
             B.futr2_season_area AS futr2_season_area_LY1
      FROM belle_sh.mz_week_season_area_sale A
      LEFT JOIN belle_sh.mz_week_season_area_sale B ON A.week = B.week
      AND A.sale_year = B.sale_year + 1
      AND A.product_season_name = B.product_season_name
      AND cast(A.product_year_name AS int) = cast(B.product_year_name AS int) + 1) season_area_1) season_area_2
WHERE season_area_2.mean4_season_area_LY1 IS NOT NULL"""
        self.call()

if __name__ == '__main__':
    Mzweekseasonareasalefull().run_command()
