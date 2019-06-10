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

class Yltempsizecombine(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_temp_size_combine;
CREATE TABLE belle_sh.yl_temp_size_combine AS
SELECT
a.product_code
,a.store_no AS store_no_in
,(CASE
  WHEN a.inv_235_final>0
  THEN 1
  ELSE a.inv_225_final
  END) AS inv_225_final_in
,(CASE
  WHEN a.inv_240_final>0
  THEN 1
  ELSE a.inv_230_final
  END) AS inv_230_final_in
,(CASE
  WHEN a.inv_225_final>0
  THEN 1
  ELSE a.inv_235_final
  END) AS inv_235_final_in
,(CASE
  WHEN a.inv_230_final>0
  THEN 1
  ELSE a.inv_240_final
  END) AS inv_240_final_in
,b.store_no AS store_no_out
,(CASE
  WHEN a.inv_235_final>0
  THEN b.inv_225_final-1
  ELSE b.inv_225_final
  END) AS inv_225_final_out
,(CASE
  WHEN a.inv_240_final>0
  THEN b.inv_230_final-1
  ELSE b.inv_230_final
  END) AS inv_230_final_out
,(CASE
  WHEN a.inv_225_final>0
  THEN b.inv_235_final-1
  ELSE b.inv_235_final
  END) AS inv_235_final_out
,(CASE
  WHEN a.inv_230_final>0
  THEN b.inv_240_final-1
  ELSE b.inv_240_final
  END) AS inv_240_final_out
FROM (SELECT
      *
      ,row_number() over(PARTITION BY product_code ORDER BY days_no_sale,final_normed_avg_sr_score DESC) AS rank_1size_in
      FROM belle_sh.op_trans_by_sku_final_{invsunday}_spr
      WHERE
          inv_225_final
         +inv_230_final
         +inv_235_final
         +inv_240_final=1
      AND days_no_sale<56
	  ) a
INNER JOIN (SELECT
            *
            ,row_number() over(PARTITION BY product_code ORDER BY days_no_sale DESC,final_normed_avg_sr_score) AS rank_4size_out
            FROM belle_sh.op_trans_by_sku_final_{invsunday}_spr
            WHERE nvl(sum_qty_2week_store,0)<=0
            AND least(1,inv_225_final)
               +least(1,inv_230_final)
               +least(1,inv_235_final)
               +least(1,inv_240_final)=4
			) b
ON a.product_code=b.product_code AND a.rank_1size_in=b.rank_4size_out
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Yltempsizecombine().run_command()
