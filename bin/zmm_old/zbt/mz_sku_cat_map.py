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


class Mzskucatmap(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE  belle_sh.mz_sku_cat_map;
CREATE TABLE belle_sh.mz_sku_cat_map AS
SELECT product_code,product_year_name,product_season_name, seasonYearSKU, heel_type_name, category_name3
FROM (
    SELECT product_code,product_year_name,product_season_name,
    concat_ws('-',product_season_name,product_year_name) AS seasonYearSKU,
    CASE WHEN heel_type_name IN ('低','平') THEN '低'
         WHEN heel_type_name = '中' THEN '中'
         WHEN heel_type_name = '高' THEN '高'
         ELSE '不涉及'
         END AS heel_type_name,
    IF(category_name3 LIKE '%靴%','靴',IF(category_name3 LIKE '%空%','空',category_name3)) AS category_name3
    FROM data_belle.dim_pro_allinfo
    WHERE category_flag!=0 AND category_name1='鞋' AND brand_no='ST'
    ) A
    GROUP BY product_code,product_year_name,product_season_name, seasonYearSKU, heel_type_name, category_name3"""
        self.call()

if __name__ == '__main__':
    Mzskucatmap().run_command()
