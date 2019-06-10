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


class Xwtransbyskufinal20190421sprv11a(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.xw_trans_by_sku_final_20190421_spr_v11a;
CREATE TABLE belle_sh.xw_trans_by_sku_final_20190421_spr_v11a AS
SELECT
a.*
,inv_220_final-inv_end_220_store AS delta_220
,inv_225_final-inv_end_225_store AS delta_225
,inv_230_final-inv_end_230_store AS delta_230
,inv_235_final-inv_end_235_store AS delta_235
,inv_240_final-inv_end_240_store AS delta_240
,inv_245_final-inv_end_245_store AS delta_245
FROM
(SELECT
a.*
,b.inv_220_from_score_final_b
,b.inv_225_from_score_final_b
,b.inv_230_from_score_final_b
,b.inv_235_from_score_final_b
,b.inv_240_from_score_final_b
,b.inv_245_from_score_final_b
,(CASE WHEN j.label_sku_logic IN (1,2,3,7,9,10,11) THEN b.inv_220_from_score_final_b ELSE a.inv_end_220_store END) AS inv_220_final
,(CASE WHEN j.label_sku_logic IN (1,2,3,7,9,10,11) THEN b.inv_225_from_score_final_b ELSE a.inv_end_225_store END) AS inv_225_final
,(CASE WHEN j.label_sku_logic IN (1,2,3,7,9,10,11) THEN b.inv_230_from_score_final_b ELSE a.inv_end_230_store END) AS inv_230_final
,(CASE WHEN j.label_sku_logic IN (1,2,3,7,9,10,11) THEN b.inv_235_from_score_final_b ELSE a.inv_end_235_store END) AS inv_235_final
,(CASE WHEN j.label_sku_logic IN (1,2,3,7,9,10,11) THEN b.inv_240_from_score_final_b ELSE a.inv_end_240_store END) AS inv_240_final
,(CASE WHEN j.label_sku_logic IN (1,2,3,7,9,10,11) THEN b.inv_245_from_score_final_b ELSE a.inv_end_245_store END) AS inv_245_final
,j.label_sku_logic AS label_sku_logic_v2
FROM belle_sh.yl_trans_by_SKU_dist3_20190421_spr_v10 a
LEFT JOIN belle_sh.xw_trans_by_SKU_b_dist3_20190421_spr_v11a b
ON a.product_code=b.product_code AND a.store_no=b.store_no
LEFT JOIN belle_sh.yl_sku_filter_to_allocate_19_20190421_v2 j
ON a.product_code=j.product_code
) a
;"""
        self.call()

if __name__ == '__main__':
    Xwtransbyskufinal20190421sprv11a().run_command()
