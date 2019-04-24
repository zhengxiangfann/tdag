# -*- coding: utf-8 -*-
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from datetime import datetime

from zmm.zbt.mz_sku_store_size_week import Mzskustoresizeweek
from zmm.zbt.mz_cat_size_stat import Mzcatsizestat
from zmm.zbt.mz_store_week_season_sale_full import Mzstoreweekseasonsalefull
from zmm.zbt.mz_sku_store_week_sale_pred import Mzskustoreweeksalepred
from zmm.zbt.mz_sku_start_week import Mzskustartweek
from zmm.zbt.mz_sku_week_2018_cpz import Mzskuweek2018cpz
from zmm.zbt.mz_store_week_season_sale import Mzstoreweekseasonsale
from zmm.zbt.mz_store_week_sale import Mzstoreweeksale
from zmm.zbt.mz_sku_week_2018_xjh import Mzskuweek2018xjh
from zmm.zbt.mz_sale_inv_st_2017_2019 import Mzsaleinvst20172019
from zmm.zbt.mz_week_season_area_sale_full import Mzweekseasonareasalefull
from zmm.zbt.mz_store_week_sale_full import Mzstoreweeksalefull
from zmm.zbt.mz_week_sku_area_sale import Mzweekskuareasale
from zmm.zbt.mz_cat_size_dist import Mzcatsizedist
from zmm.zbt.mz_sku_store_week_sale import Mzskustoreweeksale
from zmm.zbt.mz_week_season_area_sale import Mzweekseasonareasale
from zmm.zbt.mz_sku_size_store_week_sale_pred import Mzskusizestoreweeksalepred
from zmm.zbt.mz_sku_cat_map import Mzskucatmap

#-------------------------------------------------------------------------------
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':datetime(2019,01,01),
    'email': ['329472010@qq.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

#-------------------------------------------------------------------------------
# DAG id '' 必须是unique的, 一般与文件名相同
dag = DAG(
    'zbt_2019042219',
    default_args=args,
    schedule_interval='0 12 * * 2'
    )

# -------------------------------------------------------------------------------
# operator 1: ('mz_sku_store_size_week', 'Mzskustoresizeweek')
mz_sku_store_size_week = PythonOperator(
    task_id='mz_sku_store_size_week',
    python_callable=Mzskustoresizeweek().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 2: ('mz_cat_size_stat', 'Mzcatsizestat')
mz_cat_size_stat = PythonOperator(
    task_id='mz_cat_size_stat',
    python_callable=Mzcatsizestat().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 3: ('mz_store_week_season_sale_full', 'Mzstoreweekseasonsalefull')
mz_store_week_season_sale_full = PythonOperator(
    task_id='mz_store_week_season_sale_full',
    python_callable=Mzstoreweekseasonsalefull().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 4: ('mz_sku_store_week_sale_pred', 'Mzskustoreweeksalepred')
mz_sku_store_week_sale_pred = PythonOperator(
    task_id='mz_sku_store_week_sale_pred',
    python_callable=Mzskustoreweeksalepred().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 5: ('mz_sku_start_week', 'Mzskustartweek')
mz_sku_start_week = PythonOperator(
    task_id='mz_sku_start_week',
    python_callable=Mzskustartweek().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 6: ('mz_sku_week_2018_cpz', 'Mzskuweek2018cpz')
mz_sku_week_2018_cpz = PythonOperator(
    task_id='mz_sku_week_2018_cpz',
    python_callable=Mzskuweek2018cpz().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 7: ('mz_store_week_season_sale', 'Mzstoreweekseasonsale')
mz_store_week_season_sale = PythonOperator(
    task_id='mz_store_week_season_sale',
    python_callable=Mzstoreweekseasonsale().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 8: ('mz_store_week_sale', 'Mzstoreweeksale')
mz_store_week_sale = PythonOperator(
    task_id='mz_store_week_sale',
    python_callable=Mzstoreweeksale().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 9: ('mz_sku_week_2018_xjh', 'Mzskuweek2018xjh')
mz_sku_week_2018_xjh = PythonOperator(
    task_id='mz_sku_week_2018_xjh',
    python_callable=Mzskuweek2018xjh().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 10: ('mz_sale_inv_st_2017_2019', 'Mzsaleinvst20172019')
mz_sale_inv_st_2017_2019 = PythonOperator(
    task_id='mz_sale_inv_st_2017_2019',
    python_callable=Mzsaleinvst20172019().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 11: ('mz_week_season_area_sale_full', 'Mzweekseasonareasalefull')
mz_week_season_area_sale_full = PythonOperator(
    task_id='mz_week_season_area_sale_full',
    python_callable=Mzweekseasonareasalefull().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 12: ('mz_store_week_sale_full', 'Mzstoreweeksalefull')
mz_store_week_sale_full = PythonOperator(
    task_id='mz_store_week_sale_full',
    python_callable=Mzstoreweeksalefull().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 13: ('mz_week_sku_area_sale', 'Mzweekskuareasale')
mz_week_sku_area_sale = PythonOperator(
    task_id='mz_week_sku_area_sale',
    python_callable=Mzweekskuareasale().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 14: ('mz_cat_size_dist', 'Mzcatsizedist')
mz_cat_size_dist = PythonOperator(
    task_id='mz_cat_size_dist',
    python_callable=Mzcatsizedist().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 15: ('mz_sku_store_week_sale', 'Mzskustoreweeksale')
mz_sku_store_week_sale = PythonOperator(
    task_id='mz_sku_store_week_sale',
    python_callable=Mzskustoreweeksale().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 16: ('mz_week_season_area_sale', 'Mzweekseasonareasale')
mz_week_season_area_sale = PythonOperator(
    task_id='mz_week_season_area_sale',
    python_callable=Mzweekseasonareasale().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 17: ('mz_sku_size_store_week_sale_pred', 'Mzskusizestoreweeksalepred')
mz_sku_size_store_week_sale_pred = PythonOperator(
    task_id='mz_sku_size_store_week_sale_pred',
    python_callable=Mzskusizestoreweeksalepred().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 18: ('mz_sku_cat_map', 'Mzskucatmap')
mz_sku_cat_map = PythonOperator(
    task_id='mz_sku_cat_map',
    python_callable=Mzskucatmap().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
#  依赖关系 
# start
mz_sale_inv_st_2017_2019 >> mz_sku_store_size_week
mz_sku_store_size_week >> mz_sku_week_2018_cpz
mz_sku_store_size_week >> mz_sku_week_2018_xjh
mz_sku_store_size_week >> mz_sku_start_week
mz_sku_store_size_week >> mz_store_week_sale
mz_store_week_sale >> mz_store_week_sale_full
mz_sku_store_size_week >> mz_store_week_season_sale
mz_store_week_season_sale >> mz_store_week_season_sale_full
mz_sku_store_size_week >> mz_week_season_area_sale
mz_week_season_area_sale >> mz_week_season_area_sale_full
mz_sku_store_size_week >> mz_week_sku_area_sale
mz_sku_store_size_week >> mz_sku_store_week_sale
mz_store_week_season_sale_full >> mz_sku_store_week_sale
mz_store_week_sale_full >> mz_sku_store_week_sale
mz_week_sku_area_sale >> mz_sku_store_week_sale
mz_week_season_area_sale_full >> mz_sku_store_week_sale
mz_sku_store_week_sale >> mz_sku_store_week_sale_pred
mz_sku_store_size_week >> mz_cat_size_stat
mz_sku_cat_map >> mz_cat_size_stat
mz_cat_size_stat >> mz_cat_size_dist
mz_cat_size_dist >> mz_sku_size_store_week_sale_pred
mz_sku_store_week_sale_pred >> mz_sku_size_store_week_sale_pred
mz_sku_cat_map >> mz_sku_size_store_week_sale_pred

# end
