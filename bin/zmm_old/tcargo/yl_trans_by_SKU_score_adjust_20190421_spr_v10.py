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


class Yltransbyskuscoreadjust20190421sprv10(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_score_adjust_20190421_spr_v10;
CREATE TABLE belle_sh.yl_trans_by_SKU_score_adjust_20190421_spr_v10 AS
SELECT
a.*
,inv_220_from_score_temp AS inv_220_from_score
,least(if(rank_size_225<3,1,99),inv_225_from_score_temp) AS inv_225_from_score
,least(if(rank_size_230<3,1,99),inv_230_from_score_temp) AS inv_230_from_score
,least(if(rank_size_235<3,1,99),inv_235_from_score_temp) AS inv_235_from_score
,least(if(rank_size_240<3,1,99),inv_240_from_score_temp) AS inv_240_from_score
,inv_245_from_score_temp AS inv_245_from_score
FROM
(
SELECT
`(inv_220_from_score|inv_225_from_score|inv_230_from_score|inv_235_from_score|inv_240_from_score|inv_245_from_score)?+.+`
,inv_220_from_score AS inv_220_from_score_temp
,inv_225_from_score AS inv_225_from_score_temp
,inv_230_from_score AS inv_230_from_score_temp
,inv_235_from_score AS inv_235_from_score_temp
,inv_240_from_score AS inv_240_from_score_temp
,inv_245_from_score AS inv_245_from_score_temp
FROM belle_sh.yl_trans_by_SKU_dist2_20190421_spr_test_v10
) a
LEFT JOIN
(
SELECT
product_code
,sum(if(size_no=225,rank_size,0)) AS rank_size_225
,sum(if(size_no=230,rank_size,0)) AS rank_size_230
,sum(if(size_no=235,rank_size,0)) AS rank_size_235
,sum(if(size_no=240,rank_size,0)) AS rank_size_240
FROM belle_sh.yl_trans_by_SKU_size_rank_20190421_spr_v10
GROUP BY product_code
) b
ON a.product_code=b.product_code
;"""
        self.call()

if __name__ == '__main__':
    Yltransbyskuscoreadjust20190421sprv10().run_command()
