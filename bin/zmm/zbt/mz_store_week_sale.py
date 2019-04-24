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


class Mzstoreweeksale(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP TABLE belle_sh.mz_store_week_sale;


CREATE TABLE belle_sh.mz_store_week_sale AS
SELECT *,
       (mean4_store - tr4_store*(-1.5)) AS itcpt_store
FROM
  (SELECT *,
          (1.5*(cnt_qty_store - mean4_store)+0.5*(cnt_qty_store_L1 - mean4_store)-0.5*(cnt_qty_store_L2 - mean4_store)-1.5*(cnt_qty_store_L3 - mean4_store)) AS tr4_store
   FROM
     (SELECT *,
             (cnt_qty_store + cnt_qty_store_L1 + cnt_qty_store_L2 + cnt_qty_store_L3)/4 AS mean4_store,
             (cnt_qty_store + cnt_qty_store_L1)/2 AS mean2_store,
             (cnt_qty_store_F1 + cnt_qty_store_F2) AS futr2_store
      FROM
        (SELECT *,
                LAG(cnt_qty_store, 1, NULL) OVER (PARTITION BY store_no
                                                  ORDER BY weekYearSale ASC) AS cnt_qty_store_L1,
                                                 LAG(cnt_qty_store, 2, NULL) OVER (PARTITION BY store_no
                                                                                   ORDER BY weekYearSale ASC) AS cnt_qty_store_L2,
                                                                                  LAG(cnt_qty_store, 3, NULL) OVER (PARTITION BY store_no
                                                                                                                    ORDER BY weekYearSale ASC) AS cnt_qty_store_L3,
                                                                                                                   lead(cnt_qty_store, 1, NULL) OVER (PARTITION BY store_no
                                                                                                                                                      ORDER BY weekYearSale ASC) AS cnt_qty_store_F1,
                                                                                                                                                     lead(cnt_qty_store, 2, NULL) OVER (PARTITION BY store_no
                                                                                                                                                                                        ORDER BY weekYearSale ASC) AS cnt_qty_store_F2
         FROM
           (SELECT store_no,
                   weekYearSale,
                   WEEK,
                   sale_year,
                   sum(qty) AS cnt_qty_store,
                   sum(sum_amt) AS sum_amt
            FROM belle_sh.mz_sku_store_size_week
            GROUP BY store_no,
                     weekYearSale,
                     WEEK,
                     sale_year) store_1) store_2) store_3) store_4"""
        self.call()

if __name__ == '__main__':
    Mzstoreweeksale().run_command()
