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


class Promonthsalrankfinal18(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """drop  table belle_jw.pro_month_sal_rank_final_18;
create table belle_jw.pro_month_sal_rank_final_18
as 
select
  ym
    ,commodity_style_no
    , category_name1
    , category_name2
    , channel
    , prop_value_adj
    , commodity_no
    , commodity_supplier_code
    , commodity_style
    , shoe_tree
    , product_season_name
    , product_year_name
    , default_pic
    , sale_commodity_num_tmall
    , sale_total_amount_tmall
    , all_sale_commodity_num_tmall
    , all_sale_total_amount_tmall
    , ROW_NUMBER() over(order by all_sale_commodity_num_tmall desc) rn
  from (
  select
    ym
    ,commodity_style_no
    , category_name1
    , category_name2
    , channel
    , prop_value_adj
    , commodity_no
    , commodity_supplier_code
    , commodity_style
    , shoe_tree
    , product_season_name
    , product_year_name
    , default_pic
    , sale_commodity_num_tmall
    , sale_total_amount_tmall
    , all_sale_total_amount_tmall
    , all_sale_commodity_num_tmall
    ,ROW_NUMBER() over(partition by ym,commodity_style_no order by sale_commodity_num_tmall desc) rn
    from
    (
     select
      ym
      ,commodity_style_no
      , category_name1
      , category_name2
      , channel
      , prop_value_adj
      , commodity_no
      , commodity_supplier_code
      , commodity_style
      , shoe_tree
      , product_season_name
      , product_year_name
      , default_pic
      , sale_commodity_num_tmall
      , sale_total_amount_tmall
      , sum(sale_total_amount_tmall) over(partition by ym,commodity_style_no order by sale_commodity_num_tmall asc) all_sale_total_amount_tmall
      , sum(sale_commodity_num_tmall) over(partition by ym,commodity_style_no order by sale_commodity_num_tmall asc) all_sale_commodity_num_tmall
    from belle_jw.pro_month_sku_sal 
  ) as t ) as t1 where rn = 1  and all_sale_commodity_num_tmall>0;
 
"""
        self.call()

if __name__ == '__main__':
    Promonthsalrankfinal18().run_command()
