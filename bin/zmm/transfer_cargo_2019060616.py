# -*- coding: utf-8 -*-
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from datetime import datetime

from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare2__size2_vt12 import Xwtransbyskubsprprepare2size2vt12
from zmm.tcargo.xw_trans_by_sku_final__spr_vt12 import Xwtransbyskufinalsprvt12
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare__size3_vt12 import Xwtransbyskubsprpreparesize3vt12
from zmm.tcargo.xw_trans_by_SKU_b_dist1__spr_size1_vt12 import Xwtransbyskubdist1sprsize1vt12
from zmm.tcargo.xw_trans_by_SKU_b_dist1__spr_size2_vt12 import Xwtransbyskubdist1sprsize2vt12
from zmm.tcargo.yl_temp_size_combine import Yltempsizecombine
from zmm.tcargo.yl_trans_by_SKU_b__spr_only_v2 import Yltransbyskubspronlyv2
from zmm.tcargo.xw_trans_by_SKU_b_dist1__spr_size4_vt12 import Xwtransbyskubdist1sprsize4vt12
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare__size1_vt12 import Xwtransbyskubsprpreparesize1vt12
from zmm.tcargo.op_trans_by_sku_final__spr import Optransbyskufinalspr
from zmm.tcargo.xw_trans_by_SKU_b_dist2__spr_size3_vt12 import Xwtransbyskubdist2sprsize3vt12
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare2__size4_vt12 import Xwtransbyskubsprprepare2size4vt12
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare2__size3_vt12 import Xwtransbyskubsprprepare2size3vt12
from zmm.tcargo.xw_trans_by_SKU_b_dist2__spr_size1_vt12 import Xwtransbyskubdist2sprsize1vt12
from zmm.tcargo.xw_trans_by_SKU_b_dist2__spr_size2_vt12 import Xwtransbyskubdist2sprsize2vt12
from zmm.tcargo.yl_trans_by_SKU_b__spr_only_v1 import Yltransbyskubspronlyv1
from zmm.tcargo.op_trans_by_SKU_b__spr_only_final import Optransbyskubspronlyfinal
from zmm.tcargo.xw_trans_by_SKU_b_dist1__spr_size3_vt12 import Xwtransbyskubdist1sprsize3vt12
from zmm.tcargo.xw_trans_by_SKU_temp_b_vt12 import Xwtransbyskutempbvt12
from zmm.tcargo.xw_trans_by_SKU_b_dist2__spr_size4_vt12 import Xwtransbyskubdist2sprsize4vt12
from zmm.tcargo.xw_trans_by_SKU_b_dist3__spr_vt12 import Xwtransbyskubdist3sprvt12
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare__size4_vt12 import Xwtransbyskubsprpreparesize4vt12
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare__vt12 import Xwtransbyskubsprpreparevt12
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare2__vt12 import Xwtransbyskubsprprepare2vt12
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare2__size1_vt12 import Xwtransbyskubsprprepare2size1vt12
from zmm.tcargo.xw_trans_by_SKU_b_dist2__spr_size5_vt12 import Xwtransbyskubdist2sprsize5vt12
from zmm.tcargo.xw_trans_by_SKU_b_spr_prepare__size2_vt12 import Xwtransbyskubsprpreparesize2vt12

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
    'transfer_cargo_2019060616',
    default_args=args,
    schedule_interval='0 15 * * 1'
    )

# -------------------------------------------------------------------------------
# operator 1: ('xw_trans_by_SKU_b_spr_prepare2__size2_vt12', 'Xwtransbyskubsprprepare2size2vt12')
xw_trans_by_SKU_b_spr_prepare2__size2_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2__size2_vt12',
    python_callable=Xwtransbyskubsprprepare2size2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 2: ('xw_trans_by_sku_final__spr_vt12', 'Xwtransbyskufinalsprvt12')
xw_trans_by_sku_final__spr_vt12 = PythonOperator(
    task_id='xw_trans_by_sku_final__spr_vt12',
    python_callable=Xwtransbyskufinalsprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 3: ('xw_trans_by_SKU_b_spr_prepare__size3_vt12', 'Xwtransbyskubsprpreparesize3vt12')
xw_trans_by_SKU_b_spr_prepare__size3_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare__size3_vt12',
    python_callable=Xwtransbyskubsprpreparesize3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 4: ('xw_trans_by_SKU_b_dist1__spr_size1_vt12', 'Xwtransbyskubdist1sprsize1vt12')
xw_trans_by_SKU_b_dist1__spr_size1_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1__spr_size1_vt12',
    python_callable=Xwtransbyskubdist1sprsize1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 5: ('xw_trans_by_SKU_b_dist1__spr_size2_vt12', 'Xwtransbyskubdist1sprsize2vt12')
xw_trans_by_SKU_b_dist1__spr_size2_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1__spr_size2_vt12',
    python_callable=Xwtransbyskubdist1sprsize2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 6: ('yl_temp_size_combine', 'Yltempsizecombine')
yl_temp_size_combine = PythonOperator(
    task_id='yl_temp_size_combine',
    python_callable=Yltempsizecombine().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 7: ('yl_trans_by_SKU_b__spr_only_v2', 'Yltransbyskubspronlyv2')
yl_trans_by_SKU_b__spr_only_v2 = PythonOperator(
    task_id='yl_trans_by_SKU_b__spr_only_v2',
    python_callable=Yltransbyskubspronlyv2().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 8: ('xw_trans_by_SKU_b_dist1__spr_size4_vt12', 'Xwtransbyskubdist1sprsize4vt12')
xw_trans_by_SKU_b_dist1__spr_size4_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1__spr_size4_vt12',
    python_callable=Xwtransbyskubdist1sprsize4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 9: ('xw_trans_by_SKU_b_spr_prepare__size1_vt12', 'Xwtransbyskubsprpreparesize1vt12')
xw_trans_by_SKU_b_spr_prepare__size1_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare__size1_vt12',
    python_callable=Xwtransbyskubsprpreparesize1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 10: ('op_trans_by_sku_final__spr', 'Optransbyskufinalspr')
op_trans_by_sku_final__spr = PythonOperator(
    task_id='op_trans_by_sku_final__spr',
    python_callable=Optransbyskufinalspr().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 11: ('xw_trans_by_SKU_b_dist2__spr_size3_vt12', 'Xwtransbyskubdist2sprsize3vt12')
xw_trans_by_SKU_b_dist2__spr_size3_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2__spr_size3_vt12',
    python_callable=Xwtransbyskubdist2sprsize3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 12: ('xw_trans_by_SKU_b_spr_prepare2__size4_vt12', 'Xwtransbyskubsprprepare2size4vt12')
xw_trans_by_SKU_b_spr_prepare2__size4_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2__size4_vt12',
    python_callable=Xwtransbyskubsprprepare2size4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 13: ('xw_trans_by_SKU_b_spr_prepare2__size3_vt12', 'Xwtransbyskubsprprepare2size3vt12')
xw_trans_by_SKU_b_spr_prepare2__size3_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2__size3_vt12',
    python_callable=Xwtransbyskubsprprepare2size3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 14: ('xw_trans_by_SKU_b_dist2__spr_size1_vt12', 'Xwtransbyskubdist2sprsize1vt12')
xw_trans_by_SKU_b_dist2__spr_size1_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2__spr_size1_vt12',
    python_callable=Xwtransbyskubdist2sprsize1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 15: ('xw_trans_by_SKU_b_dist2__spr_size2_vt12', 'Xwtransbyskubdist2sprsize2vt12')
xw_trans_by_SKU_b_dist2__spr_size2_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2__spr_size2_vt12',
    python_callable=Xwtransbyskubdist2sprsize2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 16: ('yl_trans_by_SKU_b__spr_only_v1', 'Yltransbyskubspronlyv1')
yl_trans_by_SKU_b__spr_only_v1 = PythonOperator(
    task_id='yl_trans_by_SKU_b__spr_only_v1',
    python_callable=Yltransbyskubspronlyv1().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 17: ('op_trans_by_SKU_b__spr_only_final', 'Optransbyskubspronlyfinal')
op_trans_by_SKU_b__spr_only_final = PythonOperator(
    task_id='op_trans_by_SKU_b__spr_only_final',
    python_callable=Optransbyskubspronlyfinal().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 18: ('xw_trans_by_SKU_b_dist1__spr_size3_vt12', 'Xwtransbyskubdist1sprsize3vt12')
xw_trans_by_SKU_b_dist1__spr_size3_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1__spr_size3_vt12',
    python_callable=Xwtransbyskubdist1sprsize3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 19: ('xw_trans_by_SKU_temp_b_vt12', 'Xwtransbyskutempbvt12')
xw_trans_by_SKU_temp_b_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_temp_b_vt12',
    python_callable=Xwtransbyskutempbvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 20: ('xw_trans_by_SKU_b_dist2__spr_size4_vt12', 'Xwtransbyskubdist2sprsize4vt12')
xw_trans_by_SKU_b_dist2__spr_size4_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2__spr_size4_vt12',
    python_callable=Xwtransbyskubdist2sprsize4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 21: ('xw_trans_by_SKU_b_dist3__spr_vt12', 'Xwtransbyskubdist3sprvt12')
xw_trans_by_SKU_b_dist3__spr_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist3__spr_vt12',
    python_callable=Xwtransbyskubdist3sprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 22: ('xw_trans_by_SKU_b_spr_prepare__size4_vt12', 'Xwtransbyskubsprpreparesize4vt12')
xw_trans_by_SKU_b_spr_prepare__size4_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare__size4_vt12',
    python_callable=Xwtransbyskubsprpreparesize4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 23: ('xw_trans_by_SKU_b_spr_prepare__vt12', 'Xwtransbyskubsprpreparevt12')
xw_trans_by_SKU_b_spr_prepare__vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare__vt12',
    python_callable=Xwtransbyskubsprpreparevt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 24: ('xw_trans_by_SKU_b_spr_prepare2__vt12', 'Xwtransbyskubsprprepare2vt12')
xw_trans_by_SKU_b_spr_prepare2__vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2__vt12',
    python_callable=Xwtransbyskubsprprepare2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 25: ('xw_trans_by_SKU_b_spr_prepare2__size1_vt12', 'Xwtransbyskubsprprepare2size1vt12')
xw_trans_by_SKU_b_spr_prepare2__size1_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2__size1_vt12',
    python_callable=Xwtransbyskubsprprepare2size1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 26: ('xw_trans_by_SKU_b_dist2__spr_size5_vt12', 'Xwtransbyskubdist2sprsize5vt12')
xw_trans_by_SKU_b_dist2__spr_size5_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2__spr_size5_vt12',
    python_callable=Xwtransbyskubdist2sprsize5vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 27: ('xw_trans_by_SKU_b_spr_prepare__size2_vt12', 'Xwtransbyskubsprpreparesize2vt12')
xw_trans_by_SKU_b_spr_prepare__size2_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare__size2_vt12',
    python_callable=Xwtransbyskubsprpreparesize2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
#  依赖关系 
# start
xw_trans_by_SKU_b_spr_prepare__vt12 >> xw_trans_by_SKU_b_spr_prepare2__vt12
xw_trans_by_SKU_b_spr_prepare2__vt12 >> xw_trans_by_SKU_b_dist1__spr_size1_vt12
xw_trans_by_SKU_b_dist1__spr_size1_vt12 >> xw_trans_by_SKU_b_dist2__spr_size1_vt12
xw_trans_by_SKU_b_spr_prepare__vt12 >> xw_trans_by_SKU_b_spr_prepare__size1_vt12
xw_trans_by_SKU_b_dist2__spr_size1_vt12 >> xw_trans_by_SKU_b_spr_prepare__size1_vt12
xw_trans_by_SKU_b_spr_prepare__size1_vt12 >> xw_trans_by_SKU_b_spr_prepare2__size1_vt12
xw_trans_by_SKU_b_spr_prepare2__size1_vt12 >> xw_trans_by_SKU_b_dist1__spr_size2_vt12
xw_trans_by_SKU_b_dist1__spr_size2_vt12 >> xw_trans_by_SKU_b_dist2__spr_size2_vt12
xw_trans_by_SKU_b_spr_prepare__size1_vt12 >> xw_trans_by_SKU_b_spr_prepare__size2_vt12
xw_trans_by_SKU_b_dist2__spr_size2_vt12 >> xw_trans_by_SKU_b_spr_prepare__size2_vt12
xw_trans_by_SKU_b_spr_prepare__size2_vt12 >> xw_trans_by_SKU_b_spr_prepare2__size2_vt12
xw_trans_by_SKU_b_spr_prepare2__size2_vt12 >> xw_trans_by_SKU_b_dist1__spr_size3_vt12
xw_trans_by_SKU_b_dist1__spr_size3_vt12 >> xw_trans_by_SKU_b_dist2__spr_size3_vt12
xw_trans_by_SKU_b_spr_prepare__size2_vt12 >> xw_trans_by_SKU_b_spr_prepare__size3_vt12
xw_trans_by_SKU_b_dist2__spr_size3_vt12 >> xw_trans_by_SKU_b_spr_prepare__size3_vt12
xw_trans_by_SKU_b_spr_prepare__size3_vt12 >> xw_trans_by_SKU_b_spr_prepare2__size3_vt12
xw_trans_by_SKU_b_spr_prepare2__size3_vt12 >> xw_trans_by_SKU_b_dist1__spr_size4_vt12
xw_trans_by_SKU_b_dist1__spr_size4_vt12 >> xw_trans_by_SKU_b_dist2__spr_size4_vt12
xw_trans_by_SKU_b_spr_prepare__size3_vt12 >> xw_trans_by_SKU_b_spr_prepare__size4_vt12
xw_trans_by_SKU_b_dist2__spr_size4_vt12 >> xw_trans_by_SKU_b_spr_prepare__size4_vt12
xw_trans_by_SKU_b_spr_prepare__size4_vt12 >> xw_trans_by_SKU_b_spr_prepare2__size4_vt12
xw_trans_by_SKU_b_spr_prepare2__size4_vt12 >> xw_trans_by_SKU_b_dist2__spr_size5_vt12
xw_trans_by_SKU_b_dist2__spr_size5_vt12 >> xw_trans_by_SKU_temp_b_vt12
xw_trans_by_SKU_temp_b_vt12 >> xw_trans_by_SKU_b_dist3__spr_vt12
xw_trans_by_SKU_b_dist3__spr_vt12 >> xw_trans_by_sku_final__spr_vt12
xw_trans_by_sku_final__spr_vt12 >> op_trans_by_sku_final__spr
op_trans_by_sku_final__spr >> yl_temp_size_combine
yl_temp_size_combine >> yl_trans_by_SKU_b__spr_only_v1
op_trans_by_sku_final__spr >> yl_trans_by_SKU_b__spr_only_v1
op_trans_by_sku_final__spr >> yl_trans_by_SKU_b__spr_only_v2
yl_trans_by_SKU_b__spr_only_v1 >> op_trans_by_SKU_b__spr_only_final
yl_trans_by_SKU_b__spr_only_v2 >> op_trans_by_SKU_b__spr_only_final

# end
