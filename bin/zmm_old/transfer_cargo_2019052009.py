# -*- coding: utf-8 -*-
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from datetime import datetime

from zmm.tcargo.yl_trans_by_SKU_b_spr_prepare2_20190421_v10 import Yltransbyskubsprprepare220190421v10
from zmm.tcargo.xw_trans_by_SKU_b_dist1_20190421_spr_size2_v11a import Xwtransbyskubdist120190421sprsize2v11a
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare2_20190421_v11a import Xwtransbyskubsprprepare220190421v11a
from zmm.tcargo.xw_trans_by_SKU_b_dist2_20190421_spr_size5_v11a import Xwtransbyskubdist220190421sprsize5v11a
from zmm.tcargo.xw_trans_by_SKU_temp_b_v11a import Xwtransbyskutempbv11a
from zmm.tcargo.yl_trans_by_SKU_dist2_20190421_spr_test_v10 import Yltransbyskudist220190421sprtestv10
from zmm.tcargo.yl_trans_by_SKU_b_dist2_20190421_spr_size2_v10 import Yltransbyskubdist220190421sprsize2v10
from zmm.tcargo.xw_trans_by_sku_final_20190421_spr_v11a import Xwtransbyskufinal20190421sprv11a
from zmm.tcargo.yl_trans_by_SKU_size_rank_20190421_spr_v10 import Yltransbyskusizerank20190421sprv10
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare_20190421_v11a import Xwtransbyskubsprprepare20190421v11a
from zmm.tcargo.yl_trans_by_SKU_b_spr_prepare_20190421_size1_v10 import Yltransbyskubsprprepare20190421size1v10
from zmm.tcargo.yl_trans_by_SKU_b_spr_prepare_20190421_size4_v10 import Yltransbyskubsprprepare20190421size4v10
from zmm.tcargo.xw_trans_by_SKU_b_dist2_20190421_spr_size4_v11a import Xwtransbyskubdist220190421sprsize4v11a
from zmm.tcargo.yl_trans_by_SKU_b_spr_prepare2_20190421_size2_v10 import Yltransbyskubsprprepare220190421size2v10
from zmm.tcargo.yl_trans_by_SKU_dist2_temp_1st_v10 import Yltransbyskudist2temp1stv10
from zmm.tcargo.yl_trans_by_SKU_temp_b_v10 import Yltransbyskutempbv10
from zmm.tcargo.yl_trans_by_SKU_b_dist2_20190421_spr_size4_v10 import Yltransbyskubdist220190421sprsize4v10
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare_20190421_size1_v11a import Xwtransbyskubsprprepare20190421size1v11a
from zmm.tcargo.yl_trans_by_SKU_dist1_20190421_spr_test_v10 import Yltransbyskudist120190421sprtestv10
from zmm.tcargo.yl_trans_by_SKU_master20190421_spr_test_v10 import Yltransbyskumaster20190421sprtestv10
from zmm.tcargo.yl_trans_by_SKU_b_spr_prepare_20190421_size2_v10 import Yltransbyskubsprprepare20190421size2v10
from zmm.tcargo.xw_trans_by_SKU_b_dist1_20190421_spr_size4_v11a import Xwtransbyskubdist120190421sprsize4v11a
from zmm.tcargo.yl_trans_by_SKU_b_dist3_20190421_spr_v10 import Yltransbyskubdist320190421sprv10
from zmm.tcargo.yl_trans_by_SKU_score_adjust_20190421_spr_v10 import Yltransbyskuscoreadjust20190421sprv10
from zmm.tcargo.yl_trans_by_SKU_b_dist2_20190421_spr_size3_v10 import Yltransbyskubdist220190421sprsize3v10
from zmm.tcargo.yl_trans_by_SKU_temp_v10 import Yltransbyskutempv10
from zmm.tcargo.yl_trans_by_SKU_b_dist2_20190421_spr_size5_v10 import Yltransbyskubdist220190421sprsize5v10
from zmm.tcargo.yl_trans_by_SKU_ad_no_out_test_v10 import Yltransbyskuadnoouttestv10
from zmm.tcargo.yl_trans_by_SKU_b_spr_prepare_20190421_size3_v10 import Yltransbyskubsprprepare20190421size3v10
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare_20190421_size3_v11a import Xwtransbyskubsprprepare20190421size3v11a
from zmm.tcargo.yl_trans_by_SKU_dist2_temp_v10 import Yltransbyskudist2tempv10
from zmm.tcargo.xw_trans_by_SKU_b_dist1_20190421_spr_size3_v11a import Xwtransbyskubdist120190421sprsize3v11a
from zmm.tcargo.xw_trans_by_SKU_b_dist2_20190421_spr_size2_v11a import Xwtransbyskubdist220190421sprsize2v11a
from zmm.tcargo.yl_trans_by_SKU_b_dist1_20190421_spr_size1_v10 import Yltransbyskubdist120190421sprsize1v10
from zmm.tcargo.xw_trans_by_SKU_b_dist3_20190421_spr_v11a import Xwtransbyskubdist320190421sprv11a
from zmm.tcargo.yl_trans_by_SKU_b_dist2_20190421_spr_size1_v10 import Yltransbyskubdist220190421sprsize1v10
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare2_20190421_size1_v11a import Xwtransbyskubsprprepare220190421size1v11a
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare_20190421_size2_v11a import Xwtransbyskubsprprepare20190421size2v11a
from zmm.tcargo.yl_trans_by_SKU_b_spr_prepare2_20190421_size3_v10 import Yltransbyskubsprprepare220190421size3v10
from zmm.tcargo.yl_trans_by_SKU_b_spr_prepare_20190421_v10 import Yltransbyskubsprprepare20190421v10
from zmm.tcargo.xw_trans_by_SKU_b_dist2_20190421_spr_size1_v11a import Xwtransbyskubdist220190421sprsize1v11a
from zmm.tcargo.xw_trans_by_SKU_b_dist2_20190421_spr_size3_v11a import Xwtransbyskubdist220190421sprsize3v11a
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare2_20190421_size3_v11a import Xwtransbyskubsprprepare220190421size3v11a
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare2_20190421_size2_v11a import Xwtransbyskubsprprepare220190421size2v11a
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare2_20190421_size4_v11a import Xwtransbyskubsprprepare220190421size4v11a
from zmm.tcargo.xw_trans_by_SKU_b_dist1_20190421_spr_size1_v11a import Xwtransbyskubdist120190421sprsize1v11a
from zmm.tcargo.yl_trans_by_sku_final_20190421_spr_v10 import Yltransbyskufinal20190421sprv10
from zmm.tcargo.yl_trans_by_SKU_b_spr_prepare2_20190421_size4_v10 import Yltransbyskubsprprepare220190421size4v10
from zmm.tcargo.op_trans_by_sku_final_20190421_spr import Optransbyskufinal20190421spr
from zmm.tcargo.yl_trans_by_SKU_dist3_20190421_spr_v10 import Yltransbyskudist320190421sprv10
from zmm.tcargo.yl_trans_by_SKU_b_dist1_20190421_spr_size3_v10 import Yltransbyskubdist120190421sprsize3v10
from zmm.tcargo.yl_trans_by_SKU_b_spr_prepare2_20190421_size1_v10 import Yltransbyskubsprprepare220190421size1v10
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare_20190421_size4_v11a import Xwtransbyskubsprprepare20190421size4v11a
from zmm.tcargo.yl_trans_by_SKU_b_dist1_20190421_spr_size2_v10 import Yltransbyskubdist120190421sprsize2v10
from zmm.tcargo.yl_trans_by_SKU_b_dist1_20190421_spr_size4_v10 import Yltransbyskubdist120190421sprsize4v10

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
    'transfer_cargo_2019052009',
    default_args=args,
    schedule_interval='0 12 * * * 2'
    )

# -------------------------------------------------------------------------------
# operator 1: ('yl_trans_by_SKU_b_spr_prepare2_20190421_v10', 'Yltransbyskubsprprepare220190421v10')
yl_trans_by_SKU_b_spr_prepare2_20190421_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2_20190421_v10',
    python_callable=Yltransbyskubsprprepare220190421v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 2: ('xw_trans_by_SKU_b_dist1_20190421_spr_size2_v11a', 'Xwtransbyskubdist120190421sprsize2v11a')
xw_trans_by_SKU_b_dist1_20190421_spr_size2_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1_20190421_spr_size2_v11a',
    python_callable=Xwtransbyskubdist120190421sprsize2v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 3: ('xw_trans_by_SKU_b_spr_prepare2_20190421_v11a', 'Xwtransbyskubsprprepare220190421v11a')
xw_trans_by_SKU_b_spr_prepare2_20190421_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2_20190421_v11a',
    python_callable=Xwtransbyskubsprprepare220190421v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 4: ('xw_trans_by_SKU_b_dist2_20190421_spr_size5_v11a', 'Xwtransbyskubdist220190421sprsize5v11a')
xw_trans_by_SKU_b_dist2_20190421_spr_size5_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2_20190421_spr_size5_v11a',
    python_callable=Xwtransbyskubdist220190421sprsize5v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 5: ('xw_trans_by_SKU_temp_b_v11a', 'Xwtransbyskutempbv11a')
xw_trans_by_SKU_temp_b_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_temp_b_v11a',
    python_callable=Xwtransbyskutempbv11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 6: ('yl_trans_by_SKU_dist2_20190421_spr_test_v10', 'Yltransbyskudist220190421sprtestv10')
yl_trans_by_SKU_dist2_20190421_spr_test_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_dist2_20190421_spr_test_v10',
    python_callable=Yltransbyskudist220190421sprtestv10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 7: ('yl_trans_by_SKU_b_dist2_20190421_spr_size2_v10', 'Yltransbyskubdist220190421sprsize2v10')
yl_trans_by_SKU_b_dist2_20190421_spr_size2_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2_20190421_spr_size2_v10',
    python_callable=Yltransbyskubdist220190421sprsize2v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 8: ('xw_trans_by_sku_final_20190421_spr_v11a', 'Xwtransbyskufinal20190421sprv11a')
xw_trans_by_sku_final_20190421_spr_v11a = PythonOperator(
    task_id='xw_trans_by_sku_final_20190421_spr_v11a',
    python_callable=Xwtransbyskufinal20190421sprv11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 9: ('yl_trans_by_SKU_size_rank_20190421_spr_v10', 'Yltransbyskusizerank20190421sprv10')
yl_trans_by_SKU_size_rank_20190421_spr_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_size_rank_20190421_spr_v10',
    python_callable=Yltransbyskusizerank20190421sprv10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 10: ('xw_trans_by_SKU_b_spr_prepare_20190421_v11a', 'Xwtransbyskubsprprepare20190421v11a')
xw_trans_by_SKU_b_spr_prepare_20190421_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare_20190421_v11a',
    python_callable=Xwtransbyskubsprprepare20190421v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 11: ('yl_trans_by_SKU_b_spr_prepare_20190421_size1_v10', 'Yltransbyskubsprprepare20190421size1v10')
yl_trans_by_SKU_b_spr_prepare_20190421_size1_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare_20190421_size1_v10',
    python_callable=Yltransbyskubsprprepare20190421size1v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 12: ('yl_trans_by_SKU_b_spr_prepare_20190421_size4_v10', 'Yltransbyskubsprprepare20190421size4v10')
yl_trans_by_SKU_b_spr_prepare_20190421_size4_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare_20190421_size4_v10',
    python_callable=Yltransbyskubsprprepare20190421size4v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 13: ('xw_trans_by_SKU_b_dist2_20190421_spr_size4_v11a', 'Xwtransbyskubdist220190421sprsize4v11a')
xw_trans_by_SKU_b_dist2_20190421_spr_size4_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2_20190421_spr_size4_v11a',
    python_callable=Xwtransbyskubdist220190421sprsize4v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 14: ('yl_trans_by_SKU_b_spr_prepare2_20190421_size2_v10', 'Yltransbyskubsprprepare220190421size2v10')
yl_trans_by_SKU_b_spr_prepare2_20190421_size2_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2_20190421_size2_v10',
    python_callable=Yltransbyskubsprprepare220190421size2v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 15: ('yl_trans_by_SKU_dist2_temp_1st_v10', 'Yltransbyskudist2temp1stv10')
yl_trans_by_SKU_dist2_temp_1st_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_dist2_temp_1st_v10',
    python_callable=Yltransbyskudist2temp1stv10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 16: ('yl_trans_by_SKU_temp_b_v10', 'Yltransbyskutempbv10')
yl_trans_by_SKU_temp_b_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_temp_b_v10',
    python_callable=Yltransbyskutempbv10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 17: ('yl_trans_by_SKU_b_dist2_20190421_spr_size4_v10', 'Yltransbyskubdist220190421sprsize4v10')
yl_trans_by_SKU_b_dist2_20190421_spr_size4_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2_20190421_spr_size4_v10',
    python_callable=Yltransbyskubdist220190421sprsize4v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 18: ('xw_trans_by_SKU_b_spr_prepare_20190421_size1_v11a', 'Xwtransbyskubsprprepare20190421size1v11a')
xw_trans_by_SKU_b_spr_prepare_20190421_size1_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare_20190421_size1_v11a',
    python_callable=Xwtransbyskubsprprepare20190421size1v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 19: ('yl_trans_by_SKU_dist1_20190421_spr_test_v10', 'Yltransbyskudist120190421sprtestv10')
yl_trans_by_SKU_dist1_20190421_spr_test_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_dist1_20190421_spr_test_v10',
    python_callable=Yltransbyskudist120190421sprtestv10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 20: ('yl_trans_by_SKU_master20190421_spr_test_v10', 'Yltransbyskumaster20190421sprtestv10')
yl_trans_by_SKU_master20190421_spr_test_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_master20190421_spr_test_v10',
    python_callable=Yltransbyskumaster20190421sprtestv10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 21: ('yl_trans_by_SKU_b_spr_prepare_20190421_size2_v10', 'Yltransbyskubsprprepare20190421size2v10')
yl_trans_by_SKU_b_spr_prepare_20190421_size2_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare_20190421_size2_v10',
    python_callable=Yltransbyskubsprprepare20190421size2v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 22: ('xw_trans_by_SKU_b_dist1_20190421_spr_size4_v11a', 'Xwtransbyskubdist120190421sprsize4v11a')
xw_trans_by_SKU_b_dist1_20190421_spr_size4_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1_20190421_spr_size4_v11a',
    python_callable=Xwtransbyskubdist120190421sprsize4v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 23: ('yl_trans_by_SKU_b_dist3_20190421_spr_v10', 'Yltransbyskubdist320190421sprv10')
yl_trans_by_SKU_b_dist3_20190421_spr_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist3_20190421_spr_v10',
    python_callable=Yltransbyskubdist320190421sprv10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 24: ('yl_trans_by_SKU_score_adjust_20190421_spr_v10', 'Yltransbyskuscoreadjust20190421sprv10')
yl_trans_by_SKU_score_adjust_20190421_spr_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_score_adjust_20190421_spr_v10',
    python_callable=Yltransbyskuscoreadjust20190421sprv10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 25: ('yl_trans_by_SKU_b_dist2_20190421_spr_size3_v10', 'Yltransbyskubdist220190421sprsize3v10')
yl_trans_by_SKU_b_dist2_20190421_spr_size3_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2_20190421_spr_size3_v10',
    python_callable=Yltransbyskubdist220190421sprsize3v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 26: ('yl_trans_by_SKU_temp_v10', 'Yltransbyskutempv10')
yl_trans_by_SKU_temp_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_temp_v10',
    python_callable=Yltransbyskutempv10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 27: ('yl_trans_by_SKU_b_dist2_20190421_spr_size5_v10', 'Yltransbyskubdist220190421sprsize5v10')
yl_trans_by_SKU_b_dist2_20190421_spr_size5_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2_20190421_spr_size5_v10',
    python_callable=Yltransbyskubdist220190421sprsize5v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 28: ('yl_trans_by_SKU_ad_no_out_test_v10', 'Yltransbyskuadnoouttestv10')
yl_trans_by_SKU_ad_no_out_test_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_ad_no_out_test_v10',
    python_callable=Yltransbyskuadnoouttestv10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 29: ('yl_trans_by_SKU_b_spr_prepare_20190421_size3_v10', 'Yltransbyskubsprprepare20190421size3v10')
yl_trans_by_SKU_b_spr_prepare_20190421_size3_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare_20190421_size3_v10',
    python_callable=Yltransbyskubsprprepare20190421size3v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 30: ('xw_trans_by_SKU_b_spr_prepare_20190421_size3_v11a', 'Xwtransbyskubsprprepare20190421size3v11a')
xw_trans_by_SKU_b_spr_prepare_20190421_size3_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare_20190421_size3_v11a',
    python_callable=Xwtransbyskubsprprepare20190421size3v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 31: ('yl_trans_by_SKU_dist2_temp_v10', 'Yltransbyskudist2tempv10')
yl_trans_by_SKU_dist2_temp_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_dist2_temp_v10',
    python_callable=Yltransbyskudist2tempv10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 32: ('xw_trans_by_SKU_b_dist1_20190421_spr_size3_v11a', 'Xwtransbyskubdist120190421sprsize3v11a')
xw_trans_by_SKU_b_dist1_20190421_spr_size3_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1_20190421_spr_size3_v11a',
    python_callable=Xwtransbyskubdist120190421sprsize3v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 33: ('xw_trans_by_SKU_b_dist2_20190421_spr_size2_v11a', 'Xwtransbyskubdist220190421sprsize2v11a')
xw_trans_by_SKU_b_dist2_20190421_spr_size2_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2_20190421_spr_size2_v11a',
    python_callable=Xwtransbyskubdist220190421sprsize2v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 34: ('yl_trans_by_SKU_b_dist1_20190421_spr_size1_v10', 'Yltransbyskubdist120190421sprsize1v10')
yl_trans_by_SKU_b_dist1_20190421_spr_size1_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist1_20190421_spr_size1_v10',
    python_callable=Yltransbyskubdist120190421sprsize1v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 35: ('xw_trans_by_SKU_b_dist3_20190421_spr_v11a', 'Xwtransbyskubdist320190421sprv11a')
xw_trans_by_SKU_b_dist3_20190421_spr_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist3_20190421_spr_v11a',
    python_callable=Xwtransbyskubdist320190421sprv11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 36: ('yl_trans_by_SKU_b_dist2_20190421_spr_size1_v10', 'Yltransbyskubdist220190421sprsize1v10')
yl_trans_by_SKU_b_dist2_20190421_spr_size1_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2_20190421_spr_size1_v10',
    python_callable=Yltransbyskubdist220190421sprsize1v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 37: ('xw_trans_by_SKU_b_spr_prepare2_20190421_size1_v11a', 'Xwtransbyskubsprprepare220190421size1v11a')
xw_trans_by_SKU_b_spr_prepare2_20190421_size1_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2_20190421_size1_v11a',
    python_callable=Xwtransbyskubsprprepare220190421size1v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 38: ('xw_trans_by_SKU_b_spr_prepare_20190421_size2_v11a', 'Xwtransbyskubsprprepare20190421size2v11a')
xw_trans_by_SKU_b_spr_prepare_20190421_size2_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare_20190421_size2_v11a',
    python_callable=Xwtransbyskubsprprepare20190421size2v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 39: ('yl_trans_by_SKU_b_spr_prepare2_20190421_size3_v10', 'Yltransbyskubsprprepare220190421size3v10')
yl_trans_by_SKU_b_spr_prepare2_20190421_size3_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2_20190421_size3_v10',
    python_callable=Yltransbyskubsprprepare220190421size3v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 40: ('yl_trans_by_SKU_b_spr_prepare_20190421_v10', 'Yltransbyskubsprprepare20190421v10')
yl_trans_by_SKU_b_spr_prepare_20190421_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare_20190421_v10',
    python_callable=Yltransbyskubsprprepare20190421v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 41: ('xw_trans_by_SKU_b_dist2_20190421_spr_size1_v11a', 'Xwtransbyskubdist220190421sprsize1v11a')
xw_trans_by_SKU_b_dist2_20190421_spr_size1_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2_20190421_spr_size1_v11a',
    python_callable=Xwtransbyskubdist220190421sprsize1v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 42: ('xw_trans_by_SKU_b_dist2_20190421_spr_size3_v11a', 'Xwtransbyskubdist220190421sprsize3v11a')
xw_trans_by_SKU_b_dist2_20190421_spr_size3_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2_20190421_spr_size3_v11a',
    python_callable=Xwtransbyskubdist220190421sprsize3v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 43: ('xw_trans_by_SKU_b_spr_prepare2_20190421_size3_v11a', 'Xwtransbyskubsprprepare220190421size3v11a')
xw_trans_by_SKU_b_spr_prepare2_20190421_size3_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2_20190421_size3_v11a',
    python_callable=Xwtransbyskubsprprepare220190421size3v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 44: ('xw_trans_by_SKU_b_spr_prepare2_20190421_size2_v11a', 'Xwtransbyskubsprprepare220190421size2v11a')
xw_trans_by_SKU_b_spr_prepare2_20190421_size2_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2_20190421_size2_v11a',
    python_callable=Xwtransbyskubsprprepare220190421size2v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 45: ('xw_trans_by_SKU_b_spr_prepare2_20190421_size4_v11a', 'Xwtransbyskubsprprepare220190421size4v11a')
xw_trans_by_SKU_b_spr_prepare2_20190421_size4_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2_20190421_size4_v11a',
    python_callable=Xwtransbyskubsprprepare220190421size4v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 46: ('xw_trans_by_SKU_b_dist1_20190421_spr_size1_v11a', 'Xwtransbyskubdist120190421sprsize1v11a')
xw_trans_by_SKU_b_dist1_20190421_spr_size1_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1_20190421_spr_size1_v11a',
    python_callable=Xwtransbyskubdist120190421sprsize1v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 47: ('yl_trans_by_sku_final_20190421_spr_v10', 'Yltransbyskufinal20190421sprv10')
yl_trans_by_sku_final_20190421_spr_v10 = PythonOperator(
    task_id='yl_trans_by_sku_final_20190421_spr_v10',
    python_callable=Yltransbyskufinal20190421sprv10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 48: ('yl_trans_by_SKU_b_spr_prepare2_20190421_size4_v10', 'Yltransbyskubsprprepare220190421size4v10')
yl_trans_by_SKU_b_spr_prepare2_20190421_size4_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2_20190421_size4_v10',
    python_callable=Yltransbyskubsprprepare220190421size4v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 49: ('op_trans_by_sku_final_20190421_spr', 'Optransbyskufinal20190421spr')
op_trans_by_sku_final_20190421_spr = PythonOperator(
    task_id='op_trans_by_sku_final_20190421_spr',
    python_callable=Optransbyskufinal20190421spr().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 50: ('yl_trans_by_SKU_dist3_20190421_spr_v10', 'Yltransbyskudist320190421sprv10')
yl_trans_by_SKU_dist3_20190421_spr_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_dist3_20190421_spr_v10',
    python_callable=Yltransbyskudist320190421sprv10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 51: ('yl_trans_by_SKU_b_dist1_20190421_spr_size3_v10', 'Yltransbyskubdist120190421sprsize3v10')
yl_trans_by_SKU_b_dist1_20190421_spr_size3_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist1_20190421_spr_size3_v10',
    python_callable=Yltransbyskubdist120190421sprsize3v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 52: ('yl_trans_by_SKU_b_spr_prepare2_20190421_size1_v10', 'Yltransbyskubsprprepare220190421size1v10')
yl_trans_by_SKU_b_spr_prepare2_20190421_size1_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2_20190421_size1_v10',
    python_callable=Yltransbyskubsprprepare220190421size1v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 53: ('xw_trans_by_SKU_b_spr_prepare_20190421_size4_v11a', 'Xwtransbyskubsprprepare20190421size4v11a')
xw_trans_by_SKU_b_spr_prepare_20190421_size4_v11a = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare_20190421_size4_v11a',
    python_callable=Xwtransbyskubsprprepare20190421size4v11a().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 54: ('yl_trans_by_SKU_b_dist1_20190421_spr_size2_v10', 'Yltransbyskubdist120190421sprsize2v10')
yl_trans_by_SKU_b_dist1_20190421_spr_size2_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist1_20190421_spr_size2_v10',
    python_callable=Yltransbyskubdist120190421sprsize2v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 55: ('yl_trans_by_SKU_b_dist1_20190421_spr_size4_v10', 'Yltransbyskubdist120190421sprsize4v10')
yl_trans_by_SKU_b_dist1_20190421_spr_size4_v10 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist1_20190421_spr_size4_v10',
    python_callable=Yltransbyskubdist120190421sprsize4v10().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
#  依赖关系 
# start
yl_trans_by_SKU_master20190421_spr_test_v10 >> yl_trans_by_SKU_dist1_20190421_spr_test_v10
yl_trans_by_SKU_dist1_20190421_spr_test_v10 >> yl_trans_by_SKU_ad_no_out_test_v10
yl_trans_by_SKU_ad_no_out_test_v10 >> yl_trans_by_SKU_dist2_20190421_spr_test_v10
yl_trans_by_SKU_dist2_20190421_spr_test_v10 >> yl_trans_by_SKU_dist2_temp_v10
yl_trans_by_SKU_dist2_temp_v10 >> yl_trans_by_SKU_dist2_temp_1st_v10
yl_trans_by_SKU_dist2_temp_1st_v10 >> yl_trans_by_SKU_temp_v10
yl_trans_by_SKU_temp_v10 >> yl_trans_by_SKU_dist3_20190421_spr_v10
yl_trans_by_SKU_dist2_20190421_spr_test_v10 >> yl_trans_by_SKU_dist3_20190421_spr_v10
yl_trans_by_SKU_size_rank_20190421_spr_v10 >> yl_trans_by_SKU_score_adjust_20190421_spr_v10
yl_trans_by_SKU_dist2_20190421_spr_test_v10 >> yl_trans_by_SKU_score_adjust_20190421_spr_v10
yl_trans_by_SKU_score_adjust_20190421_spr_v10 >> yl_trans_by_SKU_b_spr_prepare_20190421_v10
yl_trans_by_SKU_b_spr_prepare_20190421_v10 >> yl_trans_by_SKU_b_spr_prepare2_20190421_v10
yl_trans_by_SKU_b_spr_prepare2_20190421_v10 >> yl_trans_by_SKU_b_dist1_20190421_spr_size1_v10
yl_trans_by_SKU_size_rank_20190421_spr_v10 >> yl_trans_by_SKU_b_dist1_20190421_spr_size1_v10
yl_trans_by_SKU_b_dist1_20190421_spr_size1_v10 >> yl_trans_by_SKU_b_dist2_20190421_spr_size1_v10
yl_trans_by_SKU_b_spr_prepare_20190421_v10 >> yl_trans_by_SKU_b_spr_prepare_20190421_size1_v10
yl_trans_by_SKU_b_dist2_20190421_spr_size1_v10 >> yl_trans_by_SKU_b_spr_prepare_20190421_size1_v10
yl_trans_by_SKU_b_spr_prepare_20190421_size1_v10 >> yl_trans_by_SKU_b_spr_prepare2_20190421_size1_v10
yl_trans_by_SKU_size_rank_20190421_spr_v10 >> yl_trans_by_SKU_b_dist1_20190421_spr_size2_v10
yl_trans_by_SKU_b_spr_prepare2_20190421_size1_v10 >> yl_trans_by_SKU_b_dist1_20190421_spr_size2_v10
yl_trans_by_SKU_b_dist1_20190421_spr_size2_v10 >> yl_trans_by_SKU_b_dist2_20190421_spr_size2_v10
yl_trans_by_SKU_b_dist2_20190421_spr_size2_v10 >> yl_trans_by_SKU_b_spr_prepare_20190421_size2_v10
yl_trans_by_SKU_b_spr_prepare_20190421_size1_v10 >> yl_trans_by_SKU_b_spr_prepare_20190421_size2_v10
yl_trans_by_SKU_b_spr_prepare_20190421_size2_v10 >> yl_trans_by_SKU_b_spr_prepare2_20190421_size2_v10
yl_trans_by_SKU_b_spr_prepare2_20190421_size2_v10 >> yl_trans_by_SKU_b_dist1_20190421_spr_size3_v10
yl_trans_by_SKU_size_rank_20190421_spr_v10 >> yl_trans_by_SKU_b_dist1_20190421_spr_size3_v10
yl_trans_by_SKU_b_dist1_20190421_spr_size3_v10 >> yl_trans_by_SKU_b_dist2_20190421_spr_size3_v10
yl_trans_by_SKU_b_dist2_20190421_spr_size3_v10 >> yl_trans_by_SKU_b_spr_prepare_20190421_size3_v10
yl_trans_by_SKU_b_spr_prepare_20190421_size2_v10 >> yl_trans_by_SKU_b_spr_prepare_20190421_size3_v10
yl_trans_by_SKU_b_spr_prepare_20190421_size3_v10 >> yl_trans_by_SKU_b_spr_prepare2_20190421_size3_v10
yl_trans_by_SKU_b_spr_prepare2_20190421_size3_v10 >> yl_trans_by_SKU_b_dist1_20190421_spr_size4_v10
yl_trans_by_SKU_size_rank_20190421_spr_v10 >> yl_trans_by_SKU_b_dist1_20190421_spr_size4_v10
yl_trans_by_SKU_b_dist1_20190421_spr_size4_v10 >> yl_trans_by_SKU_b_dist2_20190421_spr_size4_v10
yl_trans_by_SKU_b_dist2_20190421_spr_size4_v10 >> yl_trans_by_SKU_b_spr_prepare_20190421_size4_v10
yl_trans_by_SKU_b_spr_prepare_20190421_size3_v10 >> yl_trans_by_SKU_b_spr_prepare_20190421_size4_v10
yl_trans_by_SKU_b_spr_prepare_20190421_size4_v10 >> yl_trans_by_SKU_b_spr_prepare2_20190421_size4_v10
yl_trans_by_SKU_b_spr_prepare2_20190421_size4_v10 >> yl_trans_by_SKU_b_dist2_20190421_spr_size5_v10
yl_trans_by_SKU_b_dist2_20190421_spr_size5_v10 >> yl_trans_by_SKU_temp_b_v10
yl_trans_by_SKU_temp_b_v10 >> yl_trans_by_SKU_b_dist3_20190421_spr_v10
yl_trans_by_SKU_dist3_20190421_spr_v10 >> yl_trans_by_sku_final_20190421_spr_v10
yl_trans_by_SKU_b_dist3_20190421_spr_v10 >> yl_trans_by_sku_final_20190421_spr_v10
yl_trans_by_SKU_dist2_20190421_spr_test_v10 >> xw_trans_by_SKU_b_spr_prepare_20190421_v11a
xw_trans_by_SKU_b_spr_prepare_20190421_v11a >> xw_trans_by_SKU_b_spr_prepare2_20190421_v11a
xw_trans_by_SKU_b_spr_prepare2_20190421_v11a >> xw_trans_by_SKU_b_dist1_20190421_spr_size1_v11a
yl_trans_by_SKU_size_rank_20190421_spr_v10 >> xw_trans_by_SKU_b_dist1_20190421_spr_size1_v11a
xw_trans_by_SKU_b_dist1_20190421_spr_size1_v11a >> xw_trans_by_SKU_b_dist2_20190421_spr_size1_v11a
xw_trans_by_SKU_b_dist2_20190421_spr_size1_v11a >> xw_trans_by_SKU_b_spr_prepare_20190421_size1_v11a
xw_trans_by_SKU_b_spr_prepare_20190421_v11a >> xw_trans_by_SKU_b_spr_prepare_20190421_size1_v11a
xw_trans_by_SKU_b_spr_prepare_20190421_size1_v11a >> xw_trans_by_SKU_b_spr_prepare2_20190421_size1_v11a
yl_trans_by_SKU_size_rank_20190421_spr_v10 >> xw_trans_by_SKU_b_dist1_20190421_spr_size2_v11a
xw_trans_by_SKU_b_spr_prepare2_20190421_size1_v11a >> xw_trans_by_SKU_b_dist1_20190421_spr_size2_v11a
xw_trans_by_SKU_b_dist1_20190421_spr_size2_v11a >> xw_trans_by_SKU_b_dist2_20190421_spr_size2_v11a
xw_trans_by_SKU_b_dist2_20190421_spr_size2_v11a >> xw_trans_by_SKU_b_spr_prepare_20190421_size2_v11a
xw_trans_by_SKU_b_spr_prepare_20190421_size1_v11a >> xw_trans_by_SKU_b_spr_prepare_20190421_size2_v11a
xw_trans_by_SKU_b_spr_prepare_20190421_size2_v11a >> xw_trans_by_SKU_b_spr_prepare2_20190421_size2_v11a
yl_trans_by_SKU_size_rank_20190421_spr_v10 >> xw_trans_by_SKU_b_dist1_20190421_spr_size3_v11a
xw_trans_by_SKU_b_spr_prepare2_20190421_size2_v11a >> xw_trans_by_SKU_b_dist1_20190421_spr_size3_v11a
xw_trans_by_SKU_b_dist1_20190421_spr_size3_v11a >> xw_trans_by_SKU_b_dist2_20190421_spr_size3_v11a
xw_trans_by_SKU_b_dist2_20190421_spr_size3_v11a >> xw_trans_by_SKU_b_spr_prepare_20190421_size3_v11a
xw_trans_by_SKU_b_spr_prepare_20190421_size2_v11a >> xw_trans_by_SKU_b_spr_prepare_20190421_size3_v11a
xw_trans_by_SKU_b_spr_prepare_20190421_size3_v11a >> xw_trans_by_SKU_b_spr_prepare2_20190421_size3_v11a
yl_trans_by_SKU_size_rank_20190421_spr_v10 >> xw_trans_by_SKU_b_dist1_20190421_spr_size4_v11a
xw_trans_by_SKU_b_spr_prepare2_20190421_size3_v11a >> xw_trans_by_SKU_b_dist1_20190421_spr_size4_v11a
xw_trans_by_SKU_b_dist1_20190421_spr_size4_v11a >> xw_trans_by_SKU_b_dist2_20190421_spr_size4_v11a
xw_trans_by_SKU_b_spr_prepare_20190421_size3_v11a >> xw_trans_by_SKU_b_spr_prepare_20190421_size4_v11a
xw_trans_by_SKU_b_dist2_20190421_spr_size4_v11a >> xw_trans_by_SKU_b_spr_prepare_20190421_size4_v11a
xw_trans_by_SKU_b_spr_prepare_20190421_size4_v11a >> xw_trans_by_SKU_b_spr_prepare2_20190421_size4_v11a
xw_trans_by_SKU_b_spr_prepare2_20190421_size4_v11a >> xw_trans_by_SKU_b_dist2_20190421_spr_size5_v11a
xw_trans_by_SKU_b_dist2_20190421_spr_size5_v11a >> xw_trans_by_SKU_temp_b_v11a
xw_trans_by_SKU_temp_b_v11a >> xw_trans_by_SKU_b_dist3_20190421_spr_v11a
xw_trans_by_SKU_b_dist3_20190421_spr_v11a >> xw_trans_by_sku_final_20190421_spr_v11a
yl_trans_by_SKU_dist3_20190421_spr_v10 >> xw_trans_by_sku_final_20190421_spr_v11a
xw_trans_by_sku_final_20190421_spr_v11a >> op_trans_by_sku_final_20190421_spr
yl_trans_by_sku_final_20190421_spr_v10 >> op_trans_by_sku_final_20190421_spr

# end
