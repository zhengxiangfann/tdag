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

class Yltransbyskuscoreadjustsprvt12(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_by_SKU_score_adjust_{invsunday}_spr_vt12;
SET hive.support.quoted.identifiers=NONE;
CREATE TABLE belle_sh.yl_trans_by_SKU_score_adjust_{invsunday}_spr_vt12 AS
SELECT
a.*
,if(ind_no_out=1,greatest(inv_end_220_store,inv_220_from_score_temp),inv_220_from_score_temp) AS inv_220_from_score
,least(if(rank_size_225<if(use_30d_ind_sku=1 AND label_sku_logic*1 IN (7,10),4,3) AND ind_no_out=0,1,99),if(ind_no_out=1,greatest(inv_end_225_store,inv_225_from_score_temp),inv_225_from_score_temp)) AS inv_225_from_score
,least(if(rank_size_230<if(use_30d_ind_sku=1 AND label_sku_logic*1 IN (7,10),4,3) AND ind_no_out=0,1,99),if(ind_no_out=1,greatest(inv_end_230_store,inv_230_from_score_temp),inv_230_from_score_temp)) AS inv_230_from_score
,least(if(rank_size_235<if(use_30d_ind_sku=1 AND label_sku_logic*1 IN (7,10),4,3) AND ind_no_out=0,1,99),if(ind_no_out=1,greatest(inv_end_235_store,inv_235_from_score_temp),inv_235_from_score_temp)) AS inv_235_from_score
,least(if(rank_size_240<if(use_30d_ind_sku=1 AND label_sku_logic*1 IN (7,10),4,3) AND ind_no_out=0,1,99),if(ind_no_out=1,greatest(inv_end_240_store,inv_240_from_score_temp),inv_240_from_score_temp)) AS inv_240_from_score
,if(ind_no_out=1,greatest(inv_end_245_store,inv_245_from_score_temp),inv_245_from_score_temp) AS inv_245_from_score
,least(1,inv_end_225_store)+least(1,inv_end_230_store)+least(1,inv_end_235_store)+least(1,inv_end_240_store) AS cnt_main_size
FROM
(
SELECT
\`(inv_220_from_score|inv_225_from_score|inv_230_from_score|inv_235_from_score|inv_240_from_score|inv_245_from_score)?+.+\`
,inv_220_from_score AS inv_220_from_score_temp
,inv_225_from_score AS inv_225_from_score_temp
,inv_230_from_score AS inv_230_from_score_temp
,inv_235_from_score AS inv_235_from_score_temp
,inv_240_from_score AS inv_240_from_score_temp
,inv_245_from_score AS inv_245_from_score_temp
FROM belle_sh.yl_trans_by_SKU_dist2_{invsunday}_spr_test_vt12
) a
LEFT JOIN
(
SELECT
product_code
,sum(if(size_no=225,rank_size,0)) AS rank_size_225
,sum(if(size_no=230,rank_size,0)) AS rank_size_230
,sum(if(size_no=235,rank_size,0)) AS rank_size_235
,sum(if(size_no=240,rank_size,0)) AS rank_size_240
FROM belle_sh.yl_trans_by_SKU_size_rank_{invsunday}_spr_vt12
GROUP BY product_code
) b
ON a.product_code=b.product_code
;
SET mapred.reduce.tasks=10;
SET hive.merge.mapredfiles = TRUE;
SET invsunday=regexp_replace(date_add(from_unixtime(unix_timestamp()),1 - dayofweek(from_unixtime(unix_timestamp()))),'-','');""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Yltransbyskuscoreadjustsprvt12().run_command()
