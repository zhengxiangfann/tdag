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

class Ylskufiltertoallocate19v2(BaseDag):
    '''

    auto create class and fill sqll

    '''

    def __init__(self):
        BaseDag.__init__(self)

    def run_command(self):
        self.sql = """DROP  TABLE belle_sh.yl_sku_filter_to_allocate_19_{invsunday}_v2;
CREATE TABLE belle_sh.yl_sku_filter_to_allocate_19_{invsunday}_v2 AS
SELECT product_code
	,product_season_name
	,YEAR
	,WEEK
	,cast({invsunday} AS string) AS inv_sunday
	,cumu_qty_pct                   AS label_cumu_qty_pct
	,avg_store_inv                  AS label_avg_store_inv
	,cnt_store_main_size_less_2_pct AS label_cnt_store_main_size_less_2_pct
	,sku_cover_rate                 AS label_sku_cover_rate
	,in_inv_offset
	,in_inv_offset_more
	,in_inv_offset_first
	,(CASE
	  WHEN avg_store_inv>4 AND sku_cover_rate>=0.8 AND in_inv_offset<3 AND cnt_store_main_size_less_2_pct<0.2
	  THEN 1
	  WHEN avg_store_inv>4 AND sku_cover_rate>=0.8 AND in_inv_offset<3 AND cnt_store_main_size_less_2_pct>=0.2 AND cumu_qty_pct<=0.8
	  THEN 2
	  WHEN avg_store_inv>4 AND sku_cover_rate>=0.8 AND in_inv_offset<3 AND cnt_store_main_size_less_2_pct>=0.2 AND cumu_qty_pct>0.8 AND cumu_qty_pct<1
	  THEN 3
	  WHEN avg_store_inv>4 AND sku_cover_rate>=0.8 AND in_inv_offset<3 AND cnt_store_main_size_less_2_pct>=0.2 AND cumu_qty_pct>=1
	  THEN 4
	  WHEN avg_store_inv>4 AND (sku_cover_rate<0.8 OR in_inv_offset>=3)
	  THEN (CASE
	        WHEN in_inv_offset>=10
		    THEN 5
	        WHEN cnt_store_main_size_less_2_pct<0.2
		    THEN 1
		    WHEN cnt_store_main_size_less_2_pct>=0.2 AND cumu_qty_pct<=0.8
		    THEN 2
		    WHEN cnt_store_main_size_less_2_pct>=0.2 AND cumu_qty_pct>0.8 AND cumu_qty_pct<1
		    THEN 3
		    WHEN cnt_store_main_size_less_2_pct>=0.2 AND cumu_qty_pct>=1
		    THEN 4
		    ELSE 96
            END
		   )
	  WHEN avg_store_inv<=4 AND in_inv_offset<=3 AND in_inv_offset_more>=3 AND avg_store_inv<=2 AND cumu_qty_pct<1
	  THEN 6
	  WHEN avg_store_inv<=4 AND in_inv_offset<=3 AND in_inv_offset_more>=3 AND avg_store_inv<=2 AND cumu_qty_pct>=1
	  THEN 12
	  WHEN avg_store_inv<=4 AND in_inv_offset<=3 AND in_inv_offset_more>=3 AND avg_store_inv>2 AND cumu_qty_pct<1
	  THEN 7
	  WHEN avg_store_inv<=4 AND in_inv_offset<=3 AND in_inv_offset_more>=3 AND avg_store_inv>2 AND cumu_qty_pct>=1
	  THEN 12
	  WHEN avg_store_inv<=4 AND in_inv_offset<=3 AND in_inv_offset_first>=3 AND avg_store_inv<=2 AND cumu_qty_pct<1
	  THEN 6
	  WHEN avg_store_inv<=4 AND in_inv_offset<=3 AND in_inv_offset_first>=3 AND avg_store_inv<=2 AND cumu_qty_pct>=1
	  THEN 12
	  WHEN avg_store_inv<=4 AND in_inv_offset<=3 AND in_inv_offset_first>=3 AND avg_store_inv>2 AND cumu_qty_pct<1
	  THEN 7
	  WHEN avg_store_inv<=4 AND in_inv_offset<=3 AND in_inv_offset_first>=3 AND avg_store_inv>2 AND cumu_qty_pct>=1
	  THEN 12
	  WHEN avg_store_inv<=4 AND in_inv_offset<=3 AND in_inv_offset_more<3 AND in_inv_offset_first<3 AND avg_store_inv<=2 AND cumu_qty_pct<=0.8
	  THEN 8
	  WHEN avg_store_inv<=4 AND in_inv_offset<=3 AND in_inv_offset_more<3 AND in_inv_offset_first<3 AND avg_store_inv<=2 AND cumu_qty_pct>0.8
	  THEN 9
	  WHEN avg_store_inv<=4 AND in_inv_offset<=3 AND in_inv_offset_more<3 AND in_inv_offset_first<3 AND avg_store_inv>2 AND cumu_qty_pct<=0.8
	  THEN 10
	  WHEN avg_store_inv<=4 AND in_inv_offset<=3 AND in_inv_offset_more<3 AND in_inv_offset_first<3 AND avg_store_inv>2 AND cumu_qty_pct>0.8
	  THEN 11
	  WHEN avg_store_inv<=4 AND in_inv_offset>=3 AND avg_store_inv_v2>=4
	  THEN (CASE
	        WHEN sku_cover_rate>=0.8 AND cnt_store_main_size_less_2_pct<0.2
	        THEN 1
	        WHEN sku_cover_rate>=0.8 AND cnt_store_main_size_less_2_pct>=0.2 AND cumu_qty_pct<=0.8
	        THEN 2
	        WHEN sku_cover_rate>=0.8 AND cnt_store_main_size_less_2_pct>=0.2 AND cumu_qty_pct>0.8 AND cumu_qty_pct<1
	        THEN 3
	        WHEN sku_cover_rate>=0.8 AND cnt_store_main_size_less_2_pct>=0.2 AND cumu_qty_pct>=1
	        THEN 4
	        WHEN sku_cover_rate<0.8
	        THEN (CASE
	              WHEN in_inv_offset>=10
		          THEN 5
	              WHEN cnt_store_main_size_less_2_pct<0.2
		          THEN 1
		          WHEN cnt_store_main_size_less_2_pct>=0.2 AND cumu_qty_pct<=0.8
		          THEN 2
		          WHEN cnt_store_main_size_less_2_pct>=0.2 AND cumu_qty_pct>0.8 AND cumu_qty_pct<1
		          THEN 3
		          WHEN cnt_store_main_size_less_2_pct>=0.2 AND cumu_qty_pct>=1
		          THEN 4
		          ELSE 96
                  END
		          )
			ELSE 98
			END
	       )
	  WHEN avg_store_inv<=4 AND in_inv_offset>=3 AND avg_store_inv_v2<4
	  THEN (CASE
	        WHEN in_inv_offset_more>=3 AND avg_store_inv<=2 AND cumu_qty_pct<1
	        THEN 6
	        WHEN in_inv_offset_more>=3 AND avg_store_inv<=2 AND cumu_qty_pct>=1
	        THEN 12
	        WHEN in_inv_offset_more>=3 AND avg_store_inv>2 AND cumu_qty_pct<1
	        THEN 7
	        WHEN in_inv_offset_more>=3 AND avg_store_inv>2 AND cumu_qty_pct>=1
	        THEN 12
	        WHEN in_inv_offset_first>=3 AND avg_store_inv<=2 AND cumu_qty_pct<1
	        THEN 6
	        WHEN in_inv_offset_first>=3 AND avg_store_inv<=2 AND cumu_qty_pct>=1
	        THEN 12
	        WHEN in_inv_offset_first>=3 AND avg_store_inv>2 AND cumu_qty_pct<1
	        THEN 7
	        WHEN in_inv_offset_first>=3 AND avg_store_inv>2 AND cumu_qty_pct>=1
	        THEN 12
	        WHEN in_inv_offset_more<3 AND in_inv_offset_first<3 AND avg_store_inv<=2 AND cumu_qty_pct<=0.8
	        THEN 8
	        WHEN in_inv_offset_more<3 AND in_inv_offset_first<3 AND avg_store_inv<=2 AND cumu_qty_pct>0.8
	        THEN 9
	        WHEN in_inv_offset_more<3 AND in_inv_offset_first<3 AND avg_store_inv>2 AND cumu_qty_pct<=0.8
	        THEN 10
	        WHEN in_inv_offset_more<3 AND in_inv_offset_first<3 AND avg_store_inv>2 AND cumu_qty_pct>0.8
	        THEN 11
			ELSE 97
			END
	       )
	  ELSE 99
	  END
	 ) AS label_sku_logic
	,(CASE
	  WHEN avg_store_inv<=4 AND in_inv_offset>=3 AND avg_store_inv_v2>=4
	  THEN 'A'
	  WHEN avg_store_inv<=4 AND in_inv_offset>=3 AND avg_store_inv_v2<4
	  THEN 'B'
	  ELSE ''
	  END
	 ) AS label_offset
FROM belle_sh.yl_sku_filter_to_allocate_tmp1_19_{invsunday}
;
SET mapred.reduce.tasks=10;
SET hive.merge.mapredfiles = TRUE;
SET invsunday=regexp_replace(date_add(from_unixtime(unix_timestamp()),1 - dayofweek(from_unixtime(unix_timestamp()))),'-','');""".format(**self.params)
        self.call()

if __name__ == '__main__':
    Ylskufiltertoallocate19v2().run_command()
