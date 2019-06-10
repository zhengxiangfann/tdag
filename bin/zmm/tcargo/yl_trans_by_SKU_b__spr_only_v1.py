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

class Yltransbyskubspronlyv1(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_b_{invsunday}_spr_only_v1;
CREATE TABLE belle_sh.yl_trans_by_SKU_b_{invsunday}_spr_only_v1 AS
SELECT
a.*
,a.inv_220_final AS inv_220_only_in
,coalesce(b.inv_225_final_in,c.inv_225_final_out,a.inv_225_final) AS inv_225_only_in
,coalesce(b.inv_230_final_in,c.inv_230_final_out,a.inv_230_final) AS inv_230_only_in
,coalesce(b.inv_235_final_in,c.inv_235_final_out,a.inv_235_final) AS inv_235_only_in
,coalesce(b.inv_240_final_in,c.inv_240_final_out,a.inv_240_final) AS inv_240_only_in
,a.inv_245_final AS inv_245_only_in
FROM belle_sh.op_trans_by_sku_final_{invsunday}_spr a
LEFT JOIN belle_sh.yl_temp_size_combine b
ON a.product_code=b.product_code AND a.store_no=b.store_no_in
LEFT JOIN belle_sh.yl_temp_size_combine c
ON a.product_code=c.product_code AND a.store_no=c.store_no_out
;
SET hive.support.quoted.identifiers=NONE;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Yltransbyskubspronlyv1().run_command()
