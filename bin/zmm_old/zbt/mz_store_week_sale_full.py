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


class Mzstoreweeksalefull(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.mz_store_week_sale_full;
CREATE TABLE belle_sh.mz_store_week_sale_full AS
SELECT
store_no,
weekYearSale,
cnt_qty_store,
futr2_store,
tr4_store,
rto_l4w_yoy,
mean2_store,
mean4_store,
futr2_store_LY1,
((futr2_store_LY1*rto_l4w_yoy + 2*mean4_store + 2*mean2_store)/3) AS futr2_store_pred_meg,
((((futr2_store_LY1*rto_l4w_yoy + 2*mean4_store + 2*mean2_store)/3)-futr2_store)/futr2_store) AS err_futr2_store_pred_meg
FROM(
    SELECT
    store_no,
    weekYearSale,
    cnt_qty_store,
    mean2_store,
    mean4_store,
    mean4_store_LY1,
    futr2_store,
    futr2_store_LY1,
    tr4_store,
    itcpt_store,
    (mean4_store/mean4_store_LY1) AS rto_l4w_yoy
    FROM(
        SELECT
        A.store_no,
        A.weekYearSale,
        A.cnt_qty_store,
        A.mean4_store,
        A.mean2_store,
        A.futr2_store,
        A.tr4_store,
        A.itcpt_store,
        B.mean4_store AS mean4_store_LY1,
        B.mean2_store AS mean2_store_LY1,
        B.futr2_store AS futr2_store_LY1
        FROM
            belle_sh.mz_store_week_sale A
        LEFT JOIN
            belle_sh.mz_store_week_sale B
        ON A.store_no = B.store_no AND A.week = B.week AND A.sale_year = B.sale_year + 1
        ) store_1
    ) store_2
    WHERE store_2.mean4_store_LY1 IS NOT NULL"""
        self.call()

if __name__ == '__main__':
    Mzstoreweeksalefull().run_command()
