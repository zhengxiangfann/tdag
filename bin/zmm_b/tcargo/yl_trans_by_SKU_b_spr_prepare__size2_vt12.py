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

class Yltransbyskubsprpreparesize2vt12(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_b_spr_prepare_{invsunday}_size2_vt12;
CREATE TABLE belle_sh.yl_trans_by_SKU_b_spr_prepare_{invsunday}_size2_vt12 AS
SELECT
a.*
,nvl(delta_size2_225,0) AS delta_size2_225
,nvl(delta_size2_230,0) AS delta_size2_230
,nvl(delta_size2_235,0) AS delta_size2_235
,nvl(delta_size2_240,0) AS delta_size2_240
,least(1,inv_end_225_store+nvl(delta_size1_225,0)+nvl(delta_size2_225,0))
+least(1,inv_end_230_store+nvl(delta_size1_230,0)+nvl(delta_size2_230,0))
+least(1,inv_end_235_store+nvl(delta_size1_235,0)+nvl(delta_size2_235,0))
+least(1,inv_end_240_store+nvl(delta_size1_240,0)+nvl(delta_size2_240,0))
 AS cnt_main_size2
FROM belle_sh.yl_trans_by_SKU_b_spr_prepare_{invsunday}_size1_vt12 a
LEFT JOIN ( SELECT
            product_code
            ,store_no
            ,if(size_no=225,inv_size2_from_score_b-inv_end_size2_store,0) AS delta_size2_225
            ,if(size_no=230,inv_size2_from_score_b-inv_end_size2_store,0) AS delta_size2_230
            ,if(size_no=235,inv_size2_from_score_b-inv_end_size2_store,0) AS delta_size2_235
            ,if(size_no=240,inv_size2_from_score_b-inv_end_size2_store,0) AS delta_size2_240
            FROM belle_sh.yl_trans_by_SKU_b_dist2_{invsunday}_spr_size2_vt12
		  ) b
ON a.product_code=b.product_code AND a.store_no=b.store_no
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Yltransbyskubsprpreparesize2vt12().run_command()
