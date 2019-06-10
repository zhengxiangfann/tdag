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

class Yltransinvmorenext1mth(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_trans_inv_more_next1mth_{invsunday};
CREATE TABLE belle_sh.yl_trans_inv_more_next1mth_{invsunday} AS
SELECT
product_code
,cast({invsunday} AS string) AS inv_sunday
,sum(nvl(order_qty,0)+nvl(replenish_qty,0)-nvl(order_arrive_qty,0)-nvl(replenish_arrive_qty,0)) AS in_inv_offset_total
,sum(nvl(replenish_qty,0)-nvl(replenish_arrive_qty,0)) AS in_inv_offset_more
,sum(nvl(order_qty,0)-nvl(order_arrive_qty,0)) AS in_inv_offset_first
FROM data_belle.dm_oforr_orgpro_shoes
WHERE region_no NOT IN('1','2','3','4','5','6','7','8','9','N')
AND order_unit_type_name = '品牌部'
AND brand_no='ST'
AND order_unit_name='上海ST'
AND regexp_replace(report_date, '-', '')<={invsunday}
AND store_lno='I0215'
AND product_year_name=2019
GROUP BY product_code
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Yltransinvmorenext1mth().run_command()
