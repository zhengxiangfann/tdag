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


class Mzcatsizestat(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE  belle_sh.mz_cat_size_stat;
CREATE TABLE belle_sh.mz_cat_size_stat AS
    SELECT
    b.product_year_name
    ,b.product_season_name
    ,b.seasonYearSKU
    ,b.category_name3
    ,b.heel_type_name
    ,a.weekYearSale
    ,sum(if(size_no='220',nvl(qty,0),0)) AS sum_qty_220
    ,sum(if(size_no='225',nvl(qty,0),0)) AS sum_qty_225
    ,sum(if(size_no='230',nvl(qty,0),0)) AS sum_qty_230
    ,sum(if(size_no='235',nvl(qty,0),0)) AS sum_qty_235
    ,sum(if(size_no='240',nvl(qty,0),0)) AS sum_qty_240
    ,sum(if(size_no='245',nvl(qty,0),0)) AS sum_qty_245
    ,sum(nvl(qty,0)) AS sum_qty
    FROM belle_sh.mz_sku_store_size_week a
    LEFT JOIN
    (
        SELECT *
        FROM belle_sh.mz_sku_cat_map
    ) b
    ON a.product_code=b.product_code AND a.seasonYearSKU=b.seasonYearSKU
    GROUP BY b.product_year_name,b.product_season_name,b.seasonYearSKU,b.category_name3,b.heel_type_name,a.weekYearSale"""
        self.call()

if __name__ == '__main__':
    Mzcatsizestat().run_command()
