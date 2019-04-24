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


class Mzskusizestoreweeksalepred(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP TABLE belle_sh.mz_sku_size_store_week_sale_pred;


CREATE TABLE belle_sh.mz_sku_size_store_week_sale_pred AS
SELECT product_code,
       store_no,
       weekyearsale,
       seasonyearsku,
       category_name3,
       heel_type_name,
       in_inv,
       cnt_qty_sku_store_L1,
       cnt_qty_sku_store_L2,
       hist2_sku_store,
       mean4_sku_store*4 AS hist4_sku_store,
       rto_store,
       rto_store_season,
       rto_area_season,
       cnt_qty_sku_store_F1 AS futr1_sku_store,
       futr2_sku_store,
       futr4_sku_store,
       futr2_sku_store_pred_1,
       futr2_sku_store_pred_2,
       futr2_sku_store_pred_3,
       futr2_sku_store_pred_4,
       futr2_sku_store_pred_5,
       pct_220,
       pct_225,
       pct_230,
       pct_235,
       pct_240,
       pct_245,
       futr2_sku_store_pred_1*nvl(pct_220/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_220_1,
       futr2_sku_store_pred_1*nvl(pct_225/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_225_1,
       futr2_sku_store_pred_1*nvl(pct_230/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_230_1,
       futr2_sku_store_pred_1*nvl(pct_235/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_235_1,
       futr2_sku_store_pred_1*nvl(pct_240/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_240_1,
       futr2_sku_store_pred_1*nvl(pct_245/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_245_1,
       futr2_sku_store_pred_2*nvl(pct_220/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_220_2,
       futr2_sku_store_pred_2*nvl(pct_225/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_225_2,
       futr2_sku_store_pred_2*nvl(pct_230/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_230_2,
       futr2_sku_store_pred_2*nvl(pct_235/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_235_2,
       futr2_sku_store_pred_2*nvl(pct_240/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_240_2,
       futr2_sku_store_pred_2*nvl(pct_245/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_245_2,
       futr2_sku_store_pred_3*nvl(pct_220/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_220_3,
       futr2_sku_store_pred_3*nvl(pct_225/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_225_3,
       futr2_sku_store_pred_3*nvl(pct_230/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_230_3,
       futr2_sku_store_pred_3*nvl(pct_235/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_235_3,
       futr2_sku_store_pred_3*nvl(pct_240/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_240_3,
       futr2_sku_store_pred_3*nvl(pct_245/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_245_3,
       futr2_sku_store_pred_4*nvl(pct_220/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_220_4,
       futr2_sku_store_pred_4*nvl(pct_225/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_225_4,
       futr2_sku_store_pred_4*nvl(pct_230/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_230_4,
       futr2_sku_store_pred_4*nvl(pct_235/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_235_4,
       futr2_sku_store_pred_4*nvl(pct_240/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_240_4,
       futr2_sku_store_pred_4*nvl(pct_245/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_245_4,
       futr2_sku_store_pred_5*nvl(pct_225/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_225_5,
       futr2_sku_store_pred_5*nvl(pct_230/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_230_5,
       futr2_sku_store_pred_5*nvl(pct_220/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_220_5,
       futr2_sku_store_pred_5*nvl(pct_235/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_235_5,
       futr2_sku_store_pred_5*nvl(pct_240/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_240_5,
       futr2_sku_store_pred_5*nvl(pct_245/(pct_220+pct_225+pct_230+pct_235+pct_240+pct_245),
                                  1/6) AS pred_245_5
FROM
  (SELECT A.*,
          B.category_name3,
          B.heel_type_name,
          C.pct_cumu_qty_220,
          C.pct_cumu_qty_225,
          C.pct_cumu_qty_230,
          C.pct_cumu_qty_235,
          C.pct_cumu_qty_240,
          C.pct_cumu_qty_245,
          C.cumu_qty,
          C.pct_qty_220_lastyear,
          C.pct_qty_225_lastyear,
          C.pct_qty_230_lastyear,
          C.pct_qty_235_lastyear,
          C.pct_qty_240_lastyear,
          C.pct_qty_245_lastyear,
          C.sum_qty_lastyear,
          (nvl(C.pct_cumu_qty_220*C.factor, 0) + nvl(C.pct_qty_220_lastyear*(1-C.factor), 0)) AS pct_220,
          (nvl(C.pct_cumu_qty_225*C.factor, 0) + nvl(C.pct_qty_225_lastyear*(1-C.factor), 0)) AS pct_225,
          (nvl(C.pct_cumu_qty_230*C.factor, 0) + nvl(C.pct_qty_230_lastyear*(1-C.factor), 0)) AS pct_230,
          (nvl(C.pct_cumu_qty_235*C.factor, 0) + nvl(C.pct_qty_235_lastyear*(1-C.factor), 0)) AS pct_235,
          (nvl(C.pct_cumu_qty_240*C.factor, 0) + nvl(C.pct_qty_240_lastyear*(1-C.factor), 0)) AS pct_240,
          (nvl(C.pct_cumu_qty_245*C.factor, 0) + nvl(C.pct_qty_245_lastyear*(1-C.factor), 0)) AS pct_245
   FROM belle_sh.mz_sku_store_week_sale_pred A
   LEFT JOIN belle_sh.mz_sku_cat_map B ON A.product_code = B.product_code
   AND A.seasonYearSKU = B.seasonYearSKU
   LEFT JOIN belle_sh.mz_cat_size_dist C ON B.seasonYearSKU = C.seasonYearSKU
   AND B.category_name3=C.category_name3
   AND B.heel_type_name=C.heel_type_name
   AND A.weekYearSale=C.weekYearSale) A"""
        self.call()

if __name__ == '__main__':
    Mzskusizestoreweeksalepred().run_command()
