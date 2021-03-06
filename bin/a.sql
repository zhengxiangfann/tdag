--去年按月份* sku 的销量
drop table belle_jw.pro_month_sku_sal_18;
create table belle_jw.pro_month_sku_sal_18
as
select
     substr(bill.report_date,1,7) as ym
    , coalesce(bill.commodity_style_no, t_info.style_number) as commodity_style_no
    , t_info.category_name1
    , t_info.category_name2
    , t_info.product_season_name
    , t_info.product_year_name
    , t_prop.channel
    , t_prop.prop_value_adj
    , bill.commodity_no
    , bill.commodity_supplier_code
    , t_s.commodity_style
    , t_info.pattern as shoe_tree
    , t_f.default_pic
    , min(t_first.first_sale_date_tmall) as first_sale_date_tmall
    , sum(bill.final_num) as final_num
    , sum(bill.final_amt) as final_amt
    , sum(bill.sale_commodity_num) as sale_commodity_num
    , sum(if(platform='淘宝', bill.sale_commodity_num, 0)) as sale_commodity_num_tmall
    , sum(bill.sale_total_amount) as sale_total_amount
    , sum(if(platform='淘宝', bill.sale_total_amount, 0)) as sale_total_amount_tmall
    , avg(bill.final_amt) as avg_final_amt
    , avg(bill.sale_total_amount) as avg_sale_total_amount
    , sum(inventory_quantity) as inventory_quantity
    , sum(nonarrival) as nonarrival
from 
    belle_sh.dm_online_sale_for_billz as bill
    left join belle_jw.pro_commodity_style_info as t_s
    on bill.commodity_no = t_s.commodity_no
    left join belle_jw.pro_channel_prop as t_prop
    on bill.commodity_no = t_prop.commodity_no
    left join belle_jw.commodity_base_info as t_f 
    on bill.commodity_no = t_f.commodity_no
    left join belle_jw.pro_first_info as t_first
    on bill.commodity_no = t_first.product_no
    left join
    (
      select
        product_no
        ,sum(inventory_quantity) as inventory_quantity
        ,sum(nonarrival) as nonarrival
      from 
        belle_jw.pro_size_inventory
      group by 
        product_no
    ) as t_i
    on bill.commodity_no = t_i.product_no
    full join data_belle.dim_pro_allinfo as t_info
    on bill.commodity_supplier_code = t_info.product_code
    and bill.commodity_brand_name = t_info.brand_cname
where
    bill.report_date >='2018-03-01'
    and bill.report_date <='2018-03-31'
    and bill.commodity_brand_name ='百丽'
    and t_info.product_year_name='2018'
group by
    substr(bill.report_date,1,7)
    , coalesce(bill.commodity_style_no, t_info.style_number)
    , t_info.category_name1
    , t_info.category_name2
    , t_info.product_season_name
    , t_info.product_year_name
    , t_prop.channel
    , t_prop.prop_value_adj
    , bill.commodity_no
    , bill.commodity_supplier_code
    , t_s.commodity_style
    , t_info.pattern
    , t_f.default_pic;
   
-------------
-- 找出同一款号中销量最大sku的图片;


drop table belle_jw.pro_month_sal_rank_final_18;
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
 

