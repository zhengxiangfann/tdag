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

class Ylskufiltertoallocatetmp119(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_sku_filter_to_allocate_tmp1_19_{invsunday};
CREATE TABLE belle_sh.yl_sku_filter_to_allocate_tmp1_19_{invsunday} AS
SELECT e.*
	, max(e.qty_rank_row_number) over(PARTITION BY e.product_season_name,e.year,e.week) AS cnt_sku
	, nvl(e.cumu_sum_qty,0)/SH_total_qty AS cumu_qty_pct
	,(nvl(e.inv_end,0)/e.qty) AS inv_divide_qty
	, nvl(e.inv_end/e.cnt_sku_in_store,0) AS avg_store_inv
	, nvl(e.cnt_sku_in_store,0)/48 AS sku_cover_rate
	, nvl(e.cnt_store_main_size_less_2/e.cnt_sku_in_store,0) AS cnt_store_main_size_less_2_pct
	,nvl(f.in_inv_offset,0) AS in_inv_offset
	,(nvl(e.inv_end,0)+nvl(f.in_inv_offset,0))/e.cnt_sku_in_store AS avg_store_inv_v2
	,nvl(g.in_inv_offset_more,0) AS in_inv_offset_more
	,nvl(g.in_inv_offset_first,0) AS in_inv_offset_first
FROM
(
SELECT
a.product_code,
a.product_season_name,
a.year,
a.week,
a.qty,
rank() over(PARTITION BY a.product_season_name,a.year,a.week ORDER BY a.qty DESC) AS qty_rank,
row_number() over(PARTITION BY a.product_season_name,a.year,a.week ORDER BY a.qty DESC) AS qty_rank_row_number,
sum(a.qty) OVER (PARTITION BY a.year,a.week,a.product_season_name ORDER BY a.qty DESC) AS cumu_sum_qty,
sum(a.qty) OVER (PARTITION BY a.year,a.week,a.product_season_name) AS SH_total_qty,
b.inv_end,
b.inv_date AS inv_sunday,
c.cnt_sku_in_store,
nvl(dd.cnt_store_main_size_less_2,0) AS cnt_store_main_size_less_2
FROM
    (SELECT
	product_code,product_season_name,WEEK,YEAR,
    sum(nvl(greatest(qty,0),0)) AS qty
    FROM belle_sh.sy_hg_shanghai_19_sale_before_allstore
    WHERE product_year_name=2019
    GROUP BY product_code,product_season_name,WEEK,YEAR
	) a
LEFT JOIN
    (
    SELECT
	product_code,WEEK,YEAR
	,inv_date
	,sum(nvl(in_inv,0)) AS inv_end
    FROM belle_sh.sy_hg_shanghai_19_sale_before_allstore
    WHERE product_year_name=2019
	AND pmod(datediff(to_date(from_unixtime(unix_timestamp(inv_date,'yyyyMMdd'))), '2017-12-31'), 7)=0
	GROUP BY product_code,WEEK,YEAR,inv_date
    ) b
ON a.product_code=b.product_code AND a.week=b.week AND a.year=b.year
LEFT JOIN
    (SELECT
	YEAR,WEEK,product_code,
	count(distinct(store_no)) AS cnt_sku_in_store
	FROM belle_sh.sy_hg_shanghai_19_sale_before_allstore
	WHERE product_year_name=2019
	GROUP BY YEAR,WEEK,product_code
	)c
ON a.year=c.year AND a.week=c.week AND a.product_code=c.product_code
LEFT JOIN
    (
	SELECT
	d.year,
	d.week,
	d.product_code,
	count(distinct(d.store_no)) AS cnt_store_main_size_less_2
	FROM
	    (SELECT
        store_no,
        product_code,
		YEAR,
        WEEK,
        sum(least(in_inv,1)) AS cnt_main_size_no
        FROM belle_sh.sy_hg_shanghai_19_sale_before_allstore
        WHERE size_no >= 225 AND size_no <= 240
        AND product_year_name=2019
	    AND pmod(datediff(to_date(from_unixtime(unix_timestamp(inv_date,'yyyyMMdd'))), '2017-12-31'), 7)=0
	    GROUP BY store_no,product_code,WEEK,YEAR) d
	WHERE d.cnt_main_size_no<=2
	GROUP BY d.year,d.week,d.product_code) dd
ON a.year=dd.year AND a.week=dd.week AND a.product_code=dd.product_code
)e
LEFT JOIN belle_sh.yl_trans_inv_offset_{invsunday} f
ON e.product_code=f.product_code AND e.year=substring(f.inv_sunday,1,4) AND e.week=weekofyear(to_date(from_unixtime(unix_timestamp(f.inv_sunday,'yyyyMMdd'))))
LEFT JOIN belle_sh.yl_trans_inv_more_next1mth_{invsunday} g
ON e.product_code=g.product_code AND e.year=substring(g.inv_sunday,1,4) AND e.week=weekofyear(to_date(from_unixtime(unix_timestamp(g.inv_sunday,'yyyyMMdd'))))
WHERE e.year=substring(cast({invsunday} AS string),1,4) AND e.week=weekofyear(to_date(from_unixtime(unix_timestamp(cast({invsunday} AS string),'yyyyMMdd'))))
;""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Ylskufiltertoallocatetmp119().run_command()
