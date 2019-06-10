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


class Xwtransbyskubsprprepare20190421size4v11a(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.xw_trans_by_SKU_b_spr_prepare_20190421_size4_v11a;
CREATE TABLE belle_sh.xw_trans_by_SKU_b_spr_prepare_20190421_size4_v11a AS
SELECT
a.`(cnt_main_size_gt2)?+.+`
,nvl(delta_size4_225,0) AS delta_size4_225
,nvl(delta_size4_230,0) AS delta_size4_230
,nvl(delta_size4_235,0) AS delta_size4_235
,nvl(delta_size4_240,0) AS delta_size4_240
,least(1,inv_end_225_store+nvl(delta_size1_225,0)+nvl(delta_size2_225,0)+nvl(delta_size3_225,0)+nvl(delta_size4_225,0))
+least(1,inv_end_230_store+nvl(delta_size1_230,0)+nvl(delta_size2_230,0)+nvl(delta_size3_230,0)+nvl(delta_size4_230,0))
+least(1,inv_end_235_store+nvl(delta_size1_235,0)+nvl(delta_size2_235,0)+nvl(delta_size3_235,0)+nvl(delta_size4_235,0))
+least(1,inv_end_240_store+nvl(delta_size1_240,0)+nvl(delta_size2_240,0)+nvl(delta_size3_240,0)+nvl(delta_size4_240,0))
 AS cnt_main_size4
,least(1,inv_end_225_store+nvl(delta_size1_225,0)+nvl(delta_size2_225,0)+nvl(delta_size3_225,0)+nvl(delta_size4_225,0)-1)
+least(1,inv_end_230_store+nvl(delta_size1_230,0)+nvl(delta_size2_230,0)+nvl(delta_size3_230,0)+nvl(delta_size4_230,0)-1)
+least(1,inv_end_235_store+nvl(delta_size1_235,0)+nvl(delta_size2_235,0)+nvl(delta_size3_235,0)+nvl(delta_size4_235,0)-1)
+least(1,inv_end_240_store+nvl(delta_size1_240,0)+nvl(delta_size2_240,0)+nvl(delta_size3_240,0)+nvl(delta_size4_240,0)-1)
 AS cnt_main_size_gt2
,greatest(inv_end_225_store+nvl(delta_size1_225,0)+nvl(delta_size2_225,0)+nvl(delta_size3_225,0)+nvl(delta_size4_225,0) ,0) AS inv_end_225_store4
,greatest(inv_end_230_store+nvl(delta_size1_230,0)+nvl(delta_size2_230,0)+nvl(delta_size3_230,0)+nvl(delta_size4_230,0) ,0) AS inv_end_230_store4
,greatest(inv_end_235_store+nvl(delta_size1_235,0)+nvl(delta_size2_235,0)+nvl(delta_size3_235,0)+nvl(delta_size4_235,0) ,0) AS inv_end_235_store4
,greatest(inv_end_240_store+nvl(delta_size1_240,0)+nvl(delta_size2_240,0)+nvl(delta_size3_240,0)+nvl(delta_size4_240,0) ,0) AS inv_end_240_store4
FROM belle_sh.xw_trans_by_SKU_b_spr_prepare_20190421_size3_v11a a
LEFT JOIN ( SELECT
            product_code
            ,store_no
            ,if(size_no=225,inv_size4_from_score_b-inv_end_size4_store,0) AS delta_size4_225
            ,if(size_no=230,inv_size4_from_score_b-inv_end_size4_store,0) AS delta_size4_230
            ,if(size_no=235,inv_size4_from_score_b-inv_end_size4_store,0) AS delta_size4_235
            ,if(size_no=240,inv_size4_from_score_b-inv_end_size4_store,0) AS delta_size4_240
            FROM belle_sh.xw_trans_by_SKU_b_dist2_20190421_spr_size4_v11a
		  ) b
ON a.product_code=b.product_code AND a.store_no=b.store_no
;"""
        self.call()

if __name__ == '__main__':
    Xwtransbyskubsprprepare20190421size4v11a().run_command()
