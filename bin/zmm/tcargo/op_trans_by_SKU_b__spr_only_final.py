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

class Optransbyskubspronlyfinal(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.op_trans_by_SKU_b_{invsunday}_spr_only_final;
CREATE TABLE belle_sh.op_trans_by_SKU_b_{invsunday}_spr_only_final AS
SELECT
a.`(inv_220_final|inv_225_final|inv_230_final|inv_235_final|inv_240_final|inv_245_final)?+.+`
,if(a.label_sku_logic_v2 IN (7,10),a.inv_220_only_in,b.inv_220_only_in) AS inv_220_final
,if(a.label_sku_logic_v2 IN (7,10),a.inv_225_only_in,b.inv_225_only_in) AS inv_225_final
,if(a.label_sku_logic_v2 IN (7,10),a.inv_230_only_in,b.inv_230_only_in) AS inv_230_final
,if(a.label_sku_logic_v2 IN (7,10),a.inv_235_only_in,b.inv_235_only_in) AS inv_235_final
,if(a.label_sku_logic_v2 IN (7,10),a.inv_240_only_in,b.inv_240_only_in) AS inv_240_final
,if(a.label_sku_logic_v2 IN (7,10),a.inv_245_only_in,b.inv_245_only_in) AS inv_245_final
FROM belle_sh.yl_trans_by_SKU_b_{invsunday}_spr_only_v2 a
LEFT JOIN belle_sh.yl_trans_by_SKU_b_{invsunday}_spr_only_v1 b
ON a.product_code=b.product_code AND a.store_no=b.store_no
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Optransbyskubspronlyfinal().run_command()
