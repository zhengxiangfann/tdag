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


class Mzskustoreweeksalepred(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP TABLE belle_sh.mz_sku_store_week_sale_pred;


CREATE TABLE belle_sh.mz_sku_store_week_sale_pred AS
SELECT rto.*,
       hist4_sku_store*least(rto_store,
                             rto_store_season,
                             rto_area_season) AS futr2_sku_store_pred_1,
       hist4_sku_store*(rto_store+rto_store_season+rto_area_season)/3 AS futr2_sku_store_pred_2,
       hist4_sku_store*greatest(rto_store,
                                rto_store_season,
                                rto_area_season) AS futr2_sku_store_pred_3,
       ((hist4_sku_store+1)*(rto_store + rto_store_season + rto_area_season)/3 -1) AS futr2_sku_store_pred_4,
       ((hist4_sku_store+1)*greatest(rto_store, rto_store_season, rto_area_season) -1) AS futr2_sku_store_pred_5
FROM
  (SELECT product_code,
          store_no,
          weekyearsale,
          seasonYearSKU,
          in_inv,
          mean2_sku_store*2 AS hist2_sku_store,
          mean4_sku_store,
          (0.5*nvl(mean4_sku_store, 0)+ 1.5*nvl(mean2_sku_store, 0)) AS hist4_sku_store,
          least(nvl((futr2_store_pred_meg/(nvl(mean4_store, 0)+nvl(mean2_store, 0))),1), 5) AS rto_store,
          least(nvl((futr2_season_store_pred_meg/(0.5*nvl(mean4_season_store, 0)+1.5*nvl(mean2_season_store, 0))),1), 5) AS rto_store_season,
          least(nvl((futr2_season_area_pred_meg/(0.5*nvl(mean4_season_area, 0)+1.5*nvl(mean2_season_area, 0))),1), 5) AS rto_area_season,
          cnt_qty_sku_store,
          cnt_qty_sku_store_L1,
          cnt_qty_sku_store_L2,
          cnt_qty_sku_store_F1,
          cnt_qty_sku_store_F2,
          cnt_qty_sku_store_F3,
          cnt_qty_sku_store_F4,
          futr2_sku_store,
          futr4_sku_store
   FROM belle_sh.mz_sku_store_week_sale) rto"""
        self.call()

if __name__ == '__main__':
    Mzskustoreweeksalepred().run_command()
