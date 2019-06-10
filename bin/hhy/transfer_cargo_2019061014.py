# -*- coding: utf-8 -*-
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from datetime import datetime

from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare2_$_vt12 import Yltransbyskubsprprepare2$vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare2__size4_vt12 import Xwtransbyskubsprprepare2size4vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist1__spr_size2_vt12 import Xwtransbyskubdist1sprsize2vt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare_$_size1_vt12 import Yltransbyskubsprprepare$size1vt12
from hhy.tcargo.yl_trans_by_SKU_b_dist2__spr_size1_vt12 import Yltransbyskubdist2sprsize1vt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare2__size1_vt12 import Yltransbyskubsprprepare2size1vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist2__spr_size5_vt12 import Xwtransbyskubdist2sprsize5vt12
from hhy.tcargo.yl_trans_by_SKU_b_dist2_$_spr_size2_vt12 import Yltransbyskubdist2$sprsize2vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare2__size3_vt12 import Xwtransbyskubsprprepare2size3vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare2_$_vt12 import Xwtransbyskubsprprepare2$vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare2__vt12 import Xwtransbyskubsprprepare2vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist2_$_spr_size2_vt12 import Xwtransbyskubdist2$sprsize2vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare_$_size3_vt12 import Xwtransbyskubsprprepare$size3vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare__size1_vt12 import Xwtransbyskubsprpreparesize1vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare__vt12 import Xwtransbyskubsprpreparevt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare_$_size2_vt12 import Yltransbyskubsprprepare$size2vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare__size4_vt12 import Xwtransbyskubsprpreparesize4vt12
from hhy.tcargo.yl_temp_size_combine import Yltempsizecombine
from hhy.tcargo.yl_trans_by_SKU_dist2_temp_1st_vt12 import Yltransbyskudist2temp1stvt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare_$_vt12 import Yltransbyskubsprprepare$vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist1_$_spr_size2_vt12 import Xwtransbyskubdist1$sprsize2vt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare2__size3_vt12 import Yltransbyskubsprprepare2size3vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist1_$_spr_size3_vt12 import Xwtransbyskubdist1$sprsize3vt12
from hhy.tcargo.yl_trans_by_SKU_b_dist1_$_spr_size4_vt12 import Yltransbyskubdist1$sprsize4vt12
from hhy.tcargo.yl_trans_by_SKU_b_dist3_$_spr_vt12 import Yltransbyskubdist3$sprvt12
from hhy.tcargo.yl_sku_filter_to_allocate_tmp1_19_$ import Ylskufiltertoallocatetmp119$
from hhy.tcargo.xw_trans_by_sku_final_$_spr_vt12 import Xwtransbyskufinal$sprvt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare_$_size3_vt12 import Yltransbyskubsprprepare$size3vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist2__spr_size2_vt12 import Xwtransbyskubdist2sprsize2vt12
from hhy.tcargo.yl_trans_by_SKU_b_dist2_$_spr_size5_vt12 import Yltransbyskubdist2$sprsize5vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist2_$_spr_size5_vt12 import Xwtransbyskubdist2$sprsize5vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist3__spr_vt12 import Xwtransbyskubdist3sprvt12
from hhy.tcargo.op_trans_by_sku_final_$_spr import Optransbyskufinal$spr
from hhy.tcargo.yl_trans_by_sku_final_$_spr_vt12 import Yltransbyskufinal$sprvt12
from hhy.tcargo.xw_trans_by_SKU_b_dist2_$_spr_size3_vt12 import Xwtransbyskubdist2$sprsize3vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare2__size1_vt12 import Xwtransbyskubsprprepare2size1vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist2__spr_size4_vt12 import Xwtransbyskubdist2sprsize4vt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare2_$_size2_vt12 import Yltransbyskubsprprepare2$size2vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare2_$_size1_vt12 import Xwtransbyskubsprprepare2$size1vt12
from hhy.tcargo.yl_sku_filter_to_allocate_tmp1_19_ import Ylskufiltertoallocatetmp119
from hhy.tcargo.yl_trans_by_SKU_b_dist1__spr_size2_vt12 import Yltransbyskubdist1sprsize2vt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare_$_size4_vt12 import Yltransbyskubsprprepare$size4vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare_$_size1_vt12 import Xwtransbyskubsprprepare$size1vt12
from hhy.tcargo.yl_trans_inv_more_next1mth_ import Yltransinvmorenext1mth
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare2_$_size4_vt12 import Xwtransbyskubsprprepare2$size4vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist2__spr_size3_vt12 import Xwtransbyskubdist2sprsize3vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist1__spr_size4_vt12 import Xwtransbyskubdist1sprsize4vt12
from hhy.tcargo.yl_trans_by_SKU_b_dist3__spr_vt12 import Yltransbyskubdist3sprvt12
from hhy.tcargo.yl_trans_by_SKU_size_rank_$_spr_vt12 import Yltransbyskusizerank$sprvt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare__size2_vt12 import Yltransbyskubsprpreparesize2vt12
from hhy.tcargo.yl_trans_by_SKU_b_dist2__spr_size4_vt12 import Yltransbyskubdist2sprsize4vt12
from hhy.tcargo.op_trans_by_SKU_b__spr_only_final import Optransbyskubspronlyfinal
from hhy.tcargo.xw_trans_by_SKU_b_dist1_$_spr_size4_vt12 import Xwtransbyskubdist1$sprsize4vt12
from hhy.tcargo.yl_sku_filter_to_allocate_19__v2 import Ylskufiltertoallocate19v2
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare__size4_vt12 import Yltransbyskubsprpreparesize4vt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare__vt12 import Yltransbyskubsprpreparevt12
from hhy.tcargo.yl_trans_by_SKU_b_dist2__spr_size2_vt12 import Yltransbyskubdist2sprsize2vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist3_$_spr_vt12 import Xwtransbyskubdist3$sprvt12
from hhy.tcargo.yl_trans_by_SKU_master_spr_test_vt12 import Yltransbyskumastersprtestvt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare2__vt12 import Yltransbyskubsprprepare2vt12
from hhy.tcargo.yl_trans_by_SKU_dist2__spr_test_vt12 import Yltransbyskudist2sprtestvt12
from hhy.tcargo.xw_trans_by_SKU_temp_b_vt12 import Xwtransbyskutempbvt12
from hhy.tcargo.yl_trans_by_SKU_b_dist1__spr_size3_vt12 import Yltransbyskubdist1sprsize3vt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare2_$_size1_vt12 import Yltransbyskubsprprepare2$size1vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist2_$_spr_size4_vt12 import Xwtransbyskubdist2$sprsize4vt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare2__size4_vt12 import Yltransbyskubsprprepare2size4vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare__size3_vt12 import Xwtransbyskubsprpreparesize3vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare2_$_size2_vt12 import Xwtransbyskubsprprepare2$size2vt12
from hhy.tcargo.yl_trans_by_SKU_score_adjust__spr_vt12 import Yltransbyskuscoreadjustsprvt12
from hhy.tcargo.yl_trans_by_SKU_dist2_temp_vt12 import Yltransbyskudist2tempvt12
from hhy.tcargo.xw_trans_by_sku_final__spr_vt12 import Xwtransbyskufinalsprvt12
from hhy.tcargo.xw_trans_by_SKU_b_dist2_$_spr_size1_vt12 import Xwtransbyskubdist2$sprsize1vt12
from hhy.tcargo.yl_trans_by_SKU_dist2_$_spr_test_vt12 import Yltransbyskudist2$sprtestvt12
from hhy.tcargo.yl_trans_by_SKU_b_dist1__spr_size4_vt12 import Yltransbyskubdist1sprsize4vt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare2_$_size3_vt12 import Yltransbyskubsprprepare2$size3vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist1_$_spr_size1_vt12 import Xwtransbyskubdist1$sprsize1vt12
from hhy.tcargo.yl_trans_by_SKU_b_$_spr_only_v1 import Yltransbyskub$spronlyv1
from hhy.tcargo.yl_trans_by_SKU_b__spr_only_v2 import Yltransbyskubspronlyv2
from hhy.tcargo.yl_trans_by_SKU_temp_vt12 import Yltransbyskutempvt12
from hhy.tcargo.yl_trans_by_SKU_b_dist1_$_spr_size2_vt12 import Yltransbyskubdist1$sprsize2vt12
from hhy.tcargo.op_trans_by_sku_final__spr import Optransbyskufinalspr
from hhy.tcargo.yl_trans_by_SKU_dist1_$_spr_test_vt12 import Yltransbyskudist1$sprtestvt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare_$_vt12 import Xwtransbyskubsprprepare$vt12
from hhy.tcargo.yl_trans_by_SKU_b_dist2_$_spr_size3_vt12 import Yltransbyskubdist2$sprsize3vt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare2__size2_vt12 import Yltransbyskubsprprepare2size2vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare2_$_size3_vt12 import Xwtransbyskubsprprepare2$size3vt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare_$_size2_vt12 import Xwtransbyskubsprprepare$size2vt12
from hhy.tcargo.yl_trans_by_SKU_b_dist2_$_spr_size1_vt12 import Yltransbyskubdist2$sprsize1vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist2__spr_size1_vt12 import Xwtransbyskubdist2sprsize1vt12
from hhy.tcargo.yl_trans_by_SKU_ad_no_out_test_vt12 import Yltransbyskuadnoouttestvt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare__size3_vt12 import Yltransbyskubsprpreparesize3vt12
from hhy.tcargo.yl_trans_inv_more_next1mth_$ import Yltransinvmorenext1mth$
from hhy.tcargo.yl_trans_by_SKU_b__spr_only_v1 import Yltransbyskubspronlyv1
from hhy.tcargo.yl_trans_by_SKU_b_dist2__spr_size3_vt12 import Yltransbyskubdist2sprsize3vt12
from hhy.tcargo.yl_trans_by_sku_final__spr_vt12 import Yltransbyskufinalsprvt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare_$_size4_vt12 import Xwtransbyskubsprprepare$size4vt12
from hhy.tcargo.yl_trans_by_SKU_b_dist1_$_spr_size1_vt12 import Yltransbyskubdist1$sprsize1vt12
from hhy.tcargo.yl_trans_by_SKU_b_dist1__spr_size1_vt12 import Yltransbyskubdist1sprsize1vt12
from hhy.tcargo.yl_trans_by_SKU_score_adjust_$_spr_vt12 import Yltransbyskuscoreadjust$sprvt12
from hhy.tcargo.yl_trans_by_SKU_dist3__spr_vt12 import Yltransbyskudist3sprvt12
from hhy.tcargo.yl_trans_by_SKU_temp_b_vt12 import Yltransbyskutempbvt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare__size2_vt12 import Xwtransbyskubsprpreparesize2vt12
from hhy.tcargo.yl_trans_by_SKU_dist1__spr_test_vt12 import Yltransbyskudist1sprtestvt12
from hhy.tcargo.yl_trans_by_SKU_b_dist2__spr_size5_vt12 import Yltransbyskubdist2sprsize5vt12
from hhy.tcargo.yl_trans_by_SKU_size_rank__spr_vt12 import Yltransbyskusizeranksprvt12
from hhy.tcargo.yl_sku_filter_to_allocate_19_$_v2 import Ylskufiltertoallocate19$v2
from hhy.tcargo.yl_trans_by_SKU_b_dist2_$_spr_size4_vt12 import Yltransbyskubdist2$sprsize4vt12
from hhy.tcargo.xw_trans_by_SKU_b_dist1__spr_size3_vt12 import Xwtransbyskubdist1sprsize3vt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare__size1_vt12 import Yltransbyskubsprpreparesize1vt12
from hhy.tcargo.yl_trans_by_SKU_b_spr_prepare2_$_size4_vt12 import Yltransbyskubsprprepare2$size4vt12
from hhy.tcargo.yl_trans_by_SKU_master$_spr_test_vt12 import Yltransbyskumaster$sprtestvt12
from hhy.tcargo.yl_trans_by_SKU_b_$_spr_only_v2 import Yltransbyskub$spronlyv2
from hhy.tcargo.xw_trans_by_SKU_b_dist1__spr_size1_vt12 import Xwtransbyskubdist1sprsize1vt12
from hhy.tcargo.yl_trans_by_SKU_b_dist1_$_spr_size3_vt12 import Yltransbyskubdist1$sprsize3vt12
from hhy.tcargo.op_trans_by_SKU_b_$_spr_only_final import Optransbyskub$spronlyfinal
from hhy.tcargo.yl_trans_by_SKU_dist3_$_spr_vt12 import Yltransbyskudist3$sprvt12
from hhy.tcargo.xw_trans_by_SKU_b_spr_prepare2__size2_vt12 import Xwtransbyskubsprprepare2size2vt12

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
    'transfer_cargo_2019061014',
    default_args=args,
    schedule_interval='0 12 * * 1'
    )

# -------------------------------------------------------------------------------
# operator 1: ('yl_trans_by_SKU_b_spr_prepare2_$_vt12', 'Yltransbyskubsprprepare2$vt12')
yl_trans_by_SKU_b_spr_prepare2_$_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2_$_vt12',
    python_callable=Yltransbyskubsprprepare2$vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 2: ('xw_trans_by_SKU_b_spr_prepare2__size4_vt12', 'Xwtransbyskubsprprepare2size4vt12')
xw_trans_by_SKU_b_spr_prepare2__size4_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2__size4_vt12',
    python_callable=Xwtransbyskubsprprepare2size4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 3: ('xw_trans_by_SKU_b_dist1__spr_size2_vt12', 'Xwtransbyskubdist1sprsize2vt12')
xw_trans_by_SKU_b_dist1__spr_size2_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1__spr_size2_vt12',
    python_callable=Xwtransbyskubdist1sprsize2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 4: ('yl_trans_by_SKU_b_spr_prepare_$_size1_vt12', 'Yltransbyskubsprprepare$size1vt12')
yl_trans_by_SKU_b_spr_prepare_$_size1_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare_$_size1_vt12',
    python_callable=Yltransbyskubsprprepare$size1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 5: ('yl_trans_by_SKU_b_dist2__spr_size1_vt12', 'Yltransbyskubdist2sprsize1vt12')
yl_trans_by_SKU_b_dist2__spr_size1_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2__spr_size1_vt12',
    python_callable=Yltransbyskubdist2sprsize1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 6: ('yl_trans_by_SKU_b_spr_prepare2__size1_vt12', 'Yltransbyskubsprprepare2size1vt12')
yl_trans_by_SKU_b_spr_prepare2__size1_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2__size1_vt12',
    python_callable=Yltransbyskubsprprepare2size1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 7: ('xw_trans_by_SKU_b_dist2__spr_size5_vt12', 'Xwtransbyskubdist2sprsize5vt12')
xw_trans_by_SKU_b_dist2__spr_size5_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2__spr_size5_vt12',
    python_callable=Xwtransbyskubdist2sprsize5vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 8: ('yl_trans_by_SKU_b_dist2_$_spr_size2_vt12', 'Yltransbyskubdist2$sprsize2vt12')
yl_trans_by_SKU_b_dist2_$_spr_size2_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2_$_spr_size2_vt12',
    python_callable=Yltransbyskubdist2$sprsize2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 9: ('xw_trans_by_SKU_b_spr_prepare2__size3_vt12', 'Xwtransbyskubsprprepare2size3vt12')
xw_trans_by_SKU_b_spr_prepare2__size3_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2__size3_vt12',
    python_callable=Xwtransbyskubsprprepare2size3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 10: ('xw_trans_by_SKU_b_spr_prepare2_$_vt12', 'Xwtransbyskubsprprepare2$vt12')
xw_trans_by_SKU_b_spr_prepare2_$_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2_$_vt12',
    python_callable=Xwtransbyskubsprprepare2$vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 11: ('xw_trans_by_SKU_b_spr_prepare2__vt12', 'Xwtransbyskubsprprepare2vt12')
xw_trans_by_SKU_b_spr_prepare2__vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2__vt12',
    python_callable=Xwtransbyskubsprprepare2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 12: ('xw_trans_by_SKU_b_dist2_$_spr_size2_vt12', 'Xwtransbyskubdist2$sprsize2vt12')
xw_trans_by_SKU_b_dist2_$_spr_size2_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2_$_spr_size2_vt12',
    python_callable=Xwtransbyskubdist2$sprsize2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 13: ('xw_trans_by_SKU_b_spr_prepare_$_size3_vt12', 'Xwtransbyskubsprprepare$size3vt12')
xw_trans_by_SKU_b_spr_prepare_$_size3_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare_$_size3_vt12',
    python_callable=Xwtransbyskubsprprepare$size3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 14: ('xw_trans_by_SKU_b_spr_prepare__size1_vt12', 'Xwtransbyskubsprpreparesize1vt12')
xw_trans_by_SKU_b_spr_prepare__size1_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare__size1_vt12',
    python_callable=Xwtransbyskubsprpreparesize1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 15: ('xw_trans_by_SKU_b_spr_prepare__vt12', 'Xwtransbyskubsprpreparevt12')
xw_trans_by_SKU_b_spr_prepare__vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare__vt12',
    python_callable=Xwtransbyskubsprpreparevt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 16: ('yl_trans_by_SKU_b_spr_prepare_$_size2_vt12', 'Yltransbyskubsprprepare$size2vt12')
yl_trans_by_SKU_b_spr_prepare_$_size2_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare_$_size2_vt12',
    python_callable=Yltransbyskubsprprepare$size2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 17: ('xw_trans_by_SKU_b_spr_prepare__size4_vt12', 'Xwtransbyskubsprpreparesize4vt12')
xw_trans_by_SKU_b_spr_prepare__size4_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare__size4_vt12',
    python_callable=Xwtransbyskubsprpreparesize4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 18: ('yl_temp_size_combine', 'Yltempsizecombine')
yl_temp_size_combine = PythonOperator(
    task_id='yl_temp_size_combine',
    python_callable=Yltempsizecombine().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 19: ('yl_trans_by_SKU_dist2_temp_1st_vt12', 'Yltransbyskudist2temp1stvt12')
yl_trans_by_SKU_dist2_temp_1st_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_dist2_temp_1st_vt12',
    python_callable=Yltransbyskudist2temp1stvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 20: ('yl_trans_by_SKU_b_spr_prepare_$_vt12', 'Yltransbyskubsprprepare$vt12')
yl_trans_by_SKU_b_spr_prepare_$_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare_$_vt12',
    python_callable=Yltransbyskubsprprepare$vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 21: ('xw_trans_by_SKU_b_dist1_$_spr_size2_vt12', 'Xwtransbyskubdist1$sprsize2vt12')
xw_trans_by_SKU_b_dist1_$_spr_size2_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1_$_spr_size2_vt12',
    python_callable=Xwtransbyskubdist1$sprsize2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 22: ('yl_trans_by_SKU_b_spr_prepare2__size3_vt12', 'Yltransbyskubsprprepare2size3vt12')
yl_trans_by_SKU_b_spr_prepare2__size3_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2__size3_vt12',
    python_callable=Yltransbyskubsprprepare2size3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 23: ('xw_trans_by_SKU_b_dist1_$_spr_size3_vt12', 'Xwtransbyskubdist1$sprsize3vt12')
xw_trans_by_SKU_b_dist1_$_spr_size3_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1_$_spr_size3_vt12',
    python_callable=Xwtransbyskubdist1$sprsize3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 24: ('yl_trans_by_SKU_b_dist1_$_spr_size4_vt12', 'Yltransbyskubdist1$sprsize4vt12')
yl_trans_by_SKU_b_dist1_$_spr_size4_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist1_$_spr_size4_vt12',
    python_callable=Yltransbyskubdist1$sprsize4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 25: ('yl_trans_by_SKU_b_dist3_$_spr_vt12', 'Yltransbyskubdist3$sprvt12')
yl_trans_by_SKU_b_dist3_$_spr_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist3_$_spr_vt12',
    python_callable=Yltransbyskubdist3$sprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 26: ('yl_sku_filter_to_allocate_tmp1_19_$', 'Ylskufiltertoallocatetmp119$')
yl_sku_filter_to_allocate_tmp1_19_$ = PythonOperator(
    task_id='yl_sku_filter_to_allocate_tmp1_19_$',
    python_callable=Ylskufiltertoallocatetmp119$().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 27: ('xw_trans_by_sku_final_$_spr_vt12', 'Xwtransbyskufinal$sprvt12')
xw_trans_by_sku_final_$_spr_vt12 = PythonOperator(
    task_id='xw_trans_by_sku_final_$_spr_vt12',
    python_callable=Xwtransbyskufinal$sprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 28: ('yl_trans_by_SKU_b_spr_prepare_$_size3_vt12', 'Yltransbyskubsprprepare$size3vt12')
yl_trans_by_SKU_b_spr_prepare_$_size3_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare_$_size3_vt12',
    python_callable=Yltransbyskubsprprepare$size3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 29: ('xw_trans_by_SKU_b_dist2__spr_size2_vt12', 'Xwtransbyskubdist2sprsize2vt12')
xw_trans_by_SKU_b_dist2__spr_size2_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2__spr_size2_vt12',
    python_callable=Xwtransbyskubdist2sprsize2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 30: ('yl_trans_by_SKU_b_dist2_$_spr_size5_vt12', 'Yltransbyskubdist2$sprsize5vt12')
yl_trans_by_SKU_b_dist2_$_spr_size5_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2_$_spr_size5_vt12',
    python_callable=Yltransbyskubdist2$sprsize5vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 31: ('xw_trans_by_SKU_b_dist2_$_spr_size5_vt12', 'Xwtransbyskubdist2$sprsize5vt12')
xw_trans_by_SKU_b_dist2_$_spr_size5_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2_$_spr_size5_vt12',
    python_callable=Xwtransbyskubdist2$sprsize5vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 32: ('xw_trans_by_SKU_b_dist3__spr_vt12', 'Xwtransbyskubdist3sprvt12')
xw_trans_by_SKU_b_dist3__spr_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist3__spr_vt12',
    python_callable=Xwtransbyskubdist3sprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 33: ('op_trans_by_sku_final_$_spr', 'Optransbyskufinal$spr')
op_trans_by_sku_final_$_spr = PythonOperator(
    task_id='op_trans_by_sku_final_$_spr',
    python_callable=Optransbyskufinal$spr().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 34: ('yl_trans_by_sku_final_$_spr_vt12', 'Yltransbyskufinal$sprvt12')
yl_trans_by_sku_final_$_spr_vt12 = PythonOperator(
    task_id='yl_trans_by_sku_final_$_spr_vt12',
    python_callable=Yltransbyskufinal$sprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 35: ('xw_trans_by_SKU_b_dist2_$_spr_size3_vt12', 'Xwtransbyskubdist2$sprsize3vt12')
xw_trans_by_SKU_b_dist2_$_spr_size3_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2_$_spr_size3_vt12',
    python_callable=Xwtransbyskubdist2$sprsize3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 36: ('xw_trans_by_SKU_b_spr_prepare2__size1_vt12', 'Xwtransbyskubsprprepare2size1vt12')
xw_trans_by_SKU_b_spr_prepare2__size1_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2__size1_vt12',
    python_callable=Xwtransbyskubsprprepare2size1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 37: ('xw_trans_by_SKU_b_dist2__spr_size4_vt12', 'Xwtransbyskubdist2sprsize4vt12')
xw_trans_by_SKU_b_dist2__spr_size4_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2__spr_size4_vt12',
    python_callable=Xwtransbyskubdist2sprsize4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 38: ('yl_trans_by_SKU_b_spr_prepare2_$_size2_vt12', 'Yltransbyskubsprprepare2$size2vt12')
yl_trans_by_SKU_b_spr_prepare2_$_size2_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2_$_size2_vt12',
    python_callable=Yltransbyskubsprprepare2$size2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 39: ('xw_trans_by_SKU_b_spr_prepare2_$_size1_vt12', 'Xwtransbyskubsprprepare2$size1vt12')
xw_trans_by_SKU_b_spr_prepare2_$_size1_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2_$_size1_vt12',
    python_callable=Xwtransbyskubsprprepare2$size1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 40: ('yl_sku_filter_to_allocate_tmp1_19_', 'Ylskufiltertoallocatetmp119')
yl_sku_filter_to_allocate_tmp1_19_ = PythonOperator(
    task_id='yl_sku_filter_to_allocate_tmp1_19_',
    python_callable=Ylskufiltertoallocatetmp119().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 41: ('yl_trans_by_SKU_b_dist1__spr_size2_vt12', 'Yltransbyskubdist1sprsize2vt12')
yl_trans_by_SKU_b_dist1__spr_size2_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist1__spr_size2_vt12',
    python_callable=Yltransbyskubdist1sprsize2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 42: ('yl_trans_by_SKU_b_spr_prepare_$_size4_vt12', 'Yltransbyskubsprprepare$size4vt12')
yl_trans_by_SKU_b_spr_prepare_$_size4_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare_$_size4_vt12',
    python_callable=Yltransbyskubsprprepare$size4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 43: ('xw_trans_by_SKU_b_spr_prepare_$_size1_vt12', 'Xwtransbyskubsprprepare$size1vt12')
xw_trans_by_SKU_b_spr_prepare_$_size1_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare_$_size1_vt12',
    python_callable=Xwtransbyskubsprprepare$size1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 44: ('yl_trans_inv_more_next1mth_', 'Yltransinvmorenext1mth')
yl_trans_inv_more_next1mth_ = PythonOperator(
    task_id='yl_trans_inv_more_next1mth_',
    python_callable=Yltransinvmorenext1mth().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 45: ('xw_trans_by_SKU_b_spr_prepare2_$_size4_vt12', 'Xwtransbyskubsprprepare2$size4vt12')
xw_trans_by_SKU_b_spr_prepare2_$_size4_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2_$_size4_vt12',
    python_callable=Xwtransbyskubsprprepare2$size4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 46: ('xw_trans_by_SKU_b_dist2__spr_size3_vt12', 'Xwtransbyskubdist2sprsize3vt12')
xw_trans_by_SKU_b_dist2__spr_size3_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2__spr_size3_vt12',
    python_callable=Xwtransbyskubdist2sprsize3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 47: ('xw_trans_by_SKU_b_dist1__spr_size4_vt12', 'Xwtransbyskubdist1sprsize4vt12')
xw_trans_by_SKU_b_dist1__spr_size4_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1__spr_size4_vt12',
    python_callable=Xwtransbyskubdist1sprsize4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 48: ('yl_trans_by_SKU_b_dist3__spr_vt12', 'Yltransbyskubdist3sprvt12')
yl_trans_by_SKU_b_dist3__spr_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist3__spr_vt12',
    python_callable=Yltransbyskubdist3sprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 49: ('yl_trans_by_SKU_size_rank_$_spr_vt12', 'Yltransbyskusizerank$sprvt12')
yl_trans_by_SKU_size_rank_$_spr_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_size_rank_$_spr_vt12',
    python_callable=Yltransbyskusizerank$sprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 50: ('yl_trans_by_SKU_b_spr_prepare__size2_vt12', 'Yltransbyskubsprpreparesize2vt12')
yl_trans_by_SKU_b_spr_prepare__size2_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare__size2_vt12',
    python_callable=Yltransbyskubsprpreparesize2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 51: ('yl_trans_by_SKU_b_dist2__spr_size4_vt12', 'Yltransbyskubdist2sprsize4vt12')
yl_trans_by_SKU_b_dist2__spr_size4_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2__spr_size4_vt12',
    python_callable=Yltransbyskubdist2sprsize4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 52: ('op_trans_by_SKU_b__spr_only_final', 'Optransbyskubspronlyfinal')
op_trans_by_SKU_b__spr_only_final = PythonOperator(
    task_id='op_trans_by_SKU_b__spr_only_final',
    python_callable=Optransbyskubspronlyfinal().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 53: ('xw_trans_by_SKU_b_dist1_$_spr_size4_vt12', 'Xwtransbyskubdist1$sprsize4vt12')
xw_trans_by_SKU_b_dist1_$_spr_size4_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1_$_spr_size4_vt12',
    python_callable=Xwtransbyskubdist1$sprsize4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 54: ('yl_sku_filter_to_allocate_19__v2', 'Ylskufiltertoallocate19v2')
yl_sku_filter_to_allocate_19__v2 = PythonOperator(
    task_id='yl_sku_filter_to_allocate_19__v2',
    python_callable=Ylskufiltertoallocate19v2().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 55: ('yl_trans_by_SKU_b_spr_prepare__size4_vt12', 'Yltransbyskubsprpreparesize4vt12')
yl_trans_by_SKU_b_spr_prepare__size4_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare__size4_vt12',
    python_callable=Yltransbyskubsprpreparesize4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 56: ('yl_trans_by_SKU_b_spr_prepare__vt12', 'Yltransbyskubsprpreparevt12')
yl_trans_by_SKU_b_spr_prepare__vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare__vt12',
    python_callable=Yltransbyskubsprpreparevt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 57: ('yl_trans_by_SKU_b_dist2__spr_size2_vt12', 'Yltransbyskubdist2sprsize2vt12')
yl_trans_by_SKU_b_dist2__spr_size2_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2__spr_size2_vt12',
    python_callable=Yltransbyskubdist2sprsize2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 58: ('xw_trans_by_SKU_b_dist3_$_spr_vt12', 'Xwtransbyskubdist3$sprvt12')
xw_trans_by_SKU_b_dist3_$_spr_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist3_$_spr_vt12',
    python_callable=Xwtransbyskubdist3$sprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 59: ('yl_trans_by_SKU_master_spr_test_vt12', 'Yltransbyskumastersprtestvt12')
yl_trans_by_SKU_master_spr_test_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_master_spr_test_vt12',
    python_callable=Yltransbyskumastersprtestvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 60: ('yl_trans_by_SKU_b_spr_prepare2__vt12', 'Yltransbyskubsprprepare2vt12')
yl_trans_by_SKU_b_spr_prepare2__vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2__vt12',
    python_callable=Yltransbyskubsprprepare2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 61: ('yl_trans_by_SKU_dist2__spr_test_vt12', 'Yltransbyskudist2sprtestvt12')
yl_trans_by_SKU_dist2__spr_test_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_dist2__spr_test_vt12',
    python_callable=Yltransbyskudist2sprtestvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 62: ('xw_trans_by_SKU_temp_b_vt12', 'Xwtransbyskutempbvt12')
xw_trans_by_SKU_temp_b_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_temp_b_vt12',
    python_callable=Xwtransbyskutempbvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 63: ('yl_trans_by_SKU_b_dist1__spr_size3_vt12', 'Yltransbyskubdist1sprsize3vt12')
yl_trans_by_SKU_b_dist1__spr_size3_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist1__spr_size3_vt12',
    python_callable=Yltransbyskubdist1sprsize3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 64: ('yl_trans_by_SKU_b_spr_prepare2_$_size1_vt12', 'Yltransbyskubsprprepare2$size1vt12')
yl_trans_by_SKU_b_spr_prepare2_$_size1_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2_$_size1_vt12',
    python_callable=Yltransbyskubsprprepare2$size1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 65: ('xw_trans_by_SKU_b_dist2_$_spr_size4_vt12', 'Xwtransbyskubdist2$sprsize4vt12')
xw_trans_by_SKU_b_dist2_$_spr_size4_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2_$_spr_size4_vt12',
    python_callable=Xwtransbyskubdist2$sprsize4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 66: ('yl_trans_by_SKU_b_spr_prepare2__size4_vt12', 'Yltransbyskubsprprepare2size4vt12')
yl_trans_by_SKU_b_spr_prepare2__size4_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2__size4_vt12',
    python_callable=Yltransbyskubsprprepare2size4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 67: ('xw_trans_by_SKU_b_spr_prepare__size3_vt12', 'Xwtransbyskubsprpreparesize3vt12')
xw_trans_by_SKU_b_spr_prepare__size3_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare__size3_vt12',
    python_callable=Xwtransbyskubsprpreparesize3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 68: ('xw_trans_by_SKU_b_spr_prepare2_$_size2_vt12', 'Xwtransbyskubsprprepare2$size2vt12')
xw_trans_by_SKU_b_spr_prepare2_$_size2_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2_$_size2_vt12',
    python_callable=Xwtransbyskubsprprepare2$size2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 69: ('yl_trans_by_SKU_score_adjust__spr_vt12', 'Yltransbyskuscoreadjustsprvt12')
yl_trans_by_SKU_score_adjust__spr_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_score_adjust__spr_vt12',
    python_callable=Yltransbyskuscoreadjustsprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 70: ('yl_trans_by_SKU_dist2_temp_vt12', 'Yltransbyskudist2tempvt12')
yl_trans_by_SKU_dist2_temp_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_dist2_temp_vt12',
    python_callable=Yltransbyskudist2tempvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 71: ('xw_trans_by_sku_final__spr_vt12', 'Xwtransbyskufinalsprvt12')
xw_trans_by_sku_final__spr_vt12 = PythonOperator(
    task_id='xw_trans_by_sku_final__spr_vt12',
    python_callable=Xwtransbyskufinalsprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 72: ('xw_trans_by_SKU_b_dist2_$_spr_size1_vt12', 'Xwtransbyskubdist2$sprsize1vt12')
xw_trans_by_SKU_b_dist2_$_spr_size1_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2_$_spr_size1_vt12',
    python_callable=Xwtransbyskubdist2$sprsize1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 73: ('yl_trans_by_SKU_dist2_$_spr_test_vt12', 'Yltransbyskudist2$sprtestvt12')
yl_trans_by_SKU_dist2_$_spr_test_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_dist2_$_spr_test_vt12',
    python_callable=Yltransbyskudist2$sprtestvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 74: ('yl_trans_by_SKU_b_dist1__spr_size4_vt12', 'Yltransbyskubdist1sprsize4vt12')
yl_trans_by_SKU_b_dist1__spr_size4_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist1__spr_size4_vt12',
    python_callable=Yltransbyskubdist1sprsize4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 75: ('yl_trans_by_SKU_b_spr_prepare2_$_size3_vt12', 'Yltransbyskubsprprepare2$size3vt12')
yl_trans_by_SKU_b_spr_prepare2_$_size3_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2_$_size3_vt12',
    python_callable=Yltransbyskubsprprepare2$size3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 76: ('xw_trans_by_SKU_b_dist1_$_spr_size1_vt12', 'Xwtransbyskubdist1$sprsize1vt12')
xw_trans_by_SKU_b_dist1_$_spr_size1_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1_$_spr_size1_vt12',
    python_callable=Xwtransbyskubdist1$sprsize1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 77: ('yl_trans_by_SKU_b_$_spr_only_v1', 'Yltransbyskub$spronlyv1')
yl_trans_by_SKU_b_$_spr_only_v1 = PythonOperator(
    task_id='yl_trans_by_SKU_b_$_spr_only_v1',
    python_callable=Yltransbyskub$spronlyv1().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 78: ('yl_trans_by_SKU_b__spr_only_v2', 'Yltransbyskubspronlyv2')
yl_trans_by_SKU_b__spr_only_v2 = PythonOperator(
    task_id='yl_trans_by_SKU_b__spr_only_v2',
    python_callable=Yltransbyskubspronlyv2().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 79: ('yl_trans_by_SKU_temp_vt12', 'Yltransbyskutempvt12')
yl_trans_by_SKU_temp_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_temp_vt12',
    python_callable=Yltransbyskutempvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 80: ('yl_trans_by_SKU_b_dist1_$_spr_size2_vt12', 'Yltransbyskubdist1$sprsize2vt12')
yl_trans_by_SKU_b_dist1_$_spr_size2_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist1_$_spr_size2_vt12',
    python_callable=Yltransbyskubdist1$sprsize2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 81: ('op_trans_by_sku_final__spr', 'Optransbyskufinalspr')
op_trans_by_sku_final__spr = PythonOperator(
    task_id='op_trans_by_sku_final__spr',
    python_callable=Optransbyskufinalspr().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 82: ('yl_trans_by_SKU_dist1_$_spr_test_vt12', 'Yltransbyskudist1$sprtestvt12')
yl_trans_by_SKU_dist1_$_spr_test_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_dist1_$_spr_test_vt12',
    python_callable=Yltransbyskudist1$sprtestvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 83: ('xw_trans_by_SKU_b_spr_prepare_$_vt12', 'Xwtransbyskubsprprepare$vt12')
xw_trans_by_SKU_b_spr_prepare_$_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare_$_vt12',
    python_callable=Xwtransbyskubsprprepare$vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 84: ('yl_trans_by_SKU_b_dist2_$_spr_size3_vt12', 'Yltransbyskubdist2$sprsize3vt12')
yl_trans_by_SKU_b_dist2_$_spr_size3_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2_$_spr_size3_vt12',
    python_callable=Yltransbyskubdist2$sprsize3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 85: ('yl_trans_by_SKU_b_spr_prepare2__size2_vt12', 'Yltransbyskubsprprepare2size2vt12')
yl_trans_by_SKU_b_spr_prepare2__size2_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2__size2_vt12',
    python_callable=Yltransbyskubsprprepare2size2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 86: ('xw_trans_by_SKU_b_spr_prepare2_$_size3_vt12', 'Xwtransbyskubsprprepare2$size3vt12')
xw_trans_by_SKU_b_spr_prepare2_$_size3_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2_$_size3_vt12',
    python_callable=Xwtransbyskubsprprepare2$size3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 87: ('xw_trans_by_SKU_b_spr_prepare_$_size2_vt12', 'Xwtransbyskubsprprepare$size2vt12')
xw_trans_by_SKU_b_spr_prepare_$_size2_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare_$_size2_vt12',
    python_callable=Xwtransbyskubsprprepare$size2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 88: ('yl_trans_by_SKU_b_dist2_$_spr_size1_vt12', 'Yltransbyskubdist2$sprsize1vt12')
yl_trans_by_SKU_b_dist2_$_spr_size1_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2_$_spr_size1_vt12',
    python_callable=Yltransbyskubdist2$sprsize1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 89: ('xw_trans_by_SKU_b_dist2__spr_size1_vt12', 'Xwtransbyskubdist2sprsize1vt12')
xw_trans_by_SKU_b_dist2__spr_size1_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist2__spr_size1_vt12',
    python_callable=Xwtransbyskubdist2sprsize1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 90: ('yl_trans_by_SKU_ad_no_out_test_vt12', 'Yltransbyskuadnoouttestvt12')
yl_trans_by_SKU_ad_no_out_test_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_ad_no_out_test_vt12',
    python_callable=Yltransbyskuadnoouttestvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 91: ('yl_trans_by_SKU_b_spr_prepare__size3_vt12', 'Yltransbyskubsprpreparesize3vt12')
yl_trans_by_SKU_b_spr_prepare__size3_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare__size3_vt12',
    python_callable=Yltransbyskubsprpreparesize3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 92: ('yl_trans_inv_more_next1mth_$', 'Yltransinvmorenext1mth$')
yl_trans_inv_more_next1mth_$ = PythonOperator(
    task_id='yl_trans_inv_more_next1mth_$',
    python_callable=Yltransinvmorenext1mth$().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 93: ('yl_trans_by_SKU_b__spr_only_v1', 'Yltransbyskubspronlyv1')
yl_trans_by_SKU_b__spr_only_v1 = PythonOperator(
    task_id='yl_trans_by_SKU_b__spr_only_v1',
    python_callable=Yltransbyskubspronlyv1().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 94: ('yl_trans_by_SKU_b_dist2__spr_size3_vt12', 'Yltransbyskubdist2sprsize3vt12')
yl_trans_by_SKU_b_dist2__spr_size3_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2__spr_size3_vt12',
    python_callable=Yltransbyskubdist2sprsize3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 95: ('yl_trans_by_sku_final__spr_vt12', 'Yltransbyskufinalsprvt12')
yl_trans_by_sku_final__spr_vt12 = PythonOperator(
    task_id='yl_trans_by_sku_final__spr_vt12',
    python_callable=Yltransbyskufinalsprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 96: ('xw_trans_by_SKU_b_spr_prepare_$_size4_vt12', 'Xwtransbyskubsprprepare$size4vt12')
xw_trans_by_SKU_b_spr_prepare_$_size4_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare_$_size4_vt12',
    python_callable=Xwtransbyskubsprprepare$size4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 97: ('yl_trans_by_SKU_b_dist1_$_spr_size1_vt12', 'Yltransbyskubdist1$sprsize1vt12')
yl_trans_by_SKU_b_dist1_$_spr_size1_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist1_$_spr_size1_vt12',
    python_callable=Yltransbyskubdist1$sprsize1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 98: ('yl_trans_by_SKU_b_dist1__spr_size1_vt12', 'Yltransbyskubdist1sprsize1vt12')
yl_trans_by_SKU_b_dist1__spr_size1_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist1__spr_size1_vt12',
    python_callable=Yltransbyskubdist1sprsize1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 99: ('yl_trans_by_SKU_score_adjust_$_spr_vt12', 'Yltransbyskuscoreadjust$sprvt12')
yl_trans_by_SKU_score_adjust_$_spr_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_score_adjust_$_spr_vt12',
    python_callable=Yltransbyskuscoreadjust$sprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 100: ('yl_trans_by_SKU_dist3__spr_vt12', 'Yltransbyskudist3sprvt12')
yl_trans_by_SKU_dist3__spr_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_dist3__spr_vt12',
    python_callable=Yltransbyskudist3sprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 101: ('yl_trans_by_SKU_temp_b_vt12', 'Yltransbyskutempbvt12')
yl_trans_by_SKU_temp_b_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_temp_b_vt12',
    python_callable=Yltransbyskutempbvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 102: ('xw_trans_by_SKU_b_spr_prepare__size2_vt12', 'Xwtransbyskubsprpreparesize2vt12')
xw_trans_by_SKU_b_spr_prepare__size2_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare__size2_vt12',
    python_callable=Xwtransbyskubsprpreparesize2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 103: ('yl_trans_by_SKU_dist1__spr_test_vt12', 'Yltransbyskudist1sprtestvt12')
yl_trans_by_SKU_dist1__spr_test_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_dist1__spr_test_vt12',
    python_callable=Yltransbyskudist1sprtestvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 104: ('yl_trans_by_SKU_b_dist2__spr_size5_vt12', 'Yltransbyskubdist2sprsize5vt12')
yl_trans_by_SKU_b_dist2__spr_size5_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2__spr_size5_vt12',
    python_callable=Yltransbyskubdist2sprsize5vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 105: ('yl_trans_by_SKU_size_rank__spr_vt12', 'Yltransbyskusizeranksprvt12')
yl_trans_by_SKU_size_rank__spr_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_size_rank__spr_vt12',
    python_callable=Yltransbyskusizeranksprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 106: ('yl_sku_filter_to_allocate_19_$_v2', 'Ylskufiltertoallocate19$v2')
yl_sku_filter_to_allocate_19_$_v2 = PythonOperator(
    task_id='yl_sku_filter_to_allocate_19_$_v2',
    python_callable=Ylskufiltertoallocate19$v2().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 107: ('yl_trans_by_SKU_b_dist2_$_spr_size4_vt12', 'Yltransbyskubdist2$sprsize4vt12')
yl_trans_by_SKU_b_dist2_$_spr_size4_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist2_$_spr_size4_vt12',
    python_callable=Yltransbyskubdist2$sprsize4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 108: ('xw_trans_by_SKU_b_dist1__spr_size3_vt12', 'Xwtransbyskubdist1sprsize3vt12')
xw_trans_by_SKU_b_dist1__spr_size3_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1__spr_size3_vt12',
    python_callable=Xwtransbyskubdist1sprsize3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 109: ('yl_trans_by_SKU_b_spr_prepare__size1_vt12', 'Yltransbyskubsprpreparesize1vt12')
yl_trans_by_SKU_b_spr_prepare__size1_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare__size1_vt12',
    python_callable=Yltransbyskubsprpreparesize1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 110: ('yl_trans_by_SKU_b_spr_prepare2_$_size4_vt12', 'Yltransbyskubsprprepare2$size4vt12')
yl_trans_by_SKU_b_spr_prepare2_$_size4_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_spr_prepare2_$_size4_vt12',
    python_callable=Yltransbyskubsprprepare2$size4vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 111: ('yl_trans_by_SKU_master$_spr_test_vt12', 'Yltransbyskumaster$sprtestvt12')
yl_trans_by_SKU_master$_spr_test_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_master$_spr_test_vt12',
    python_callable=Yltransbyskumaster$sprtestvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 112: ('yl_trans_by_SKU_b_$_spr_only_v2', 'Yltransbyskub$spronlyv2')
yl_trans_by_SKU_b_$_spr_only_v2 = PythonOperator(
    task_id='yl_trans_by_SKU_b_$_spr_only_v2',
    python_callable=Yltransbyskub$spronlyv2().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 113: ('xw_trans_by_SKU_b_dist1__spr_size1_vt12', 'Xwtransbyskubdist1sprsize1vt12')
xw_trans_by_SKU_b_dist1__spr_size1_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_dist1__spr_size1_vt12',
    python_callable=Xwtransbyskubdist1sprsize1vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 114: ('yl_trans_by_SKU_b_dist1_$_spr_size3_vt12', 'Yltransbyskubdist1$sprsize3vt12')
yl_trans_by_SKU_b_dist1_$_spr_size3_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_b_dist1_$_spr_size3_vt12',
    python_callable=Yltransbyskubdist1$sprsize3vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 115: ('op_trans_by_SKU_b_$_spr_only_final', 'Optransbyskub$spronlyfinal')
op_trans_by_SKU_b_$_spr_only_final = PythonOperator(
    task_id='op_trans_by_SKU_b_$_spr_only_final',
    python_callable=Optransbyskub$spronlyfinal().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 116: ('yl_trans_by_SKU_dist3_$_spr_vt12', 'Yltransbyskudist3$sprvt12')
yl_trans_by_SKU_dist3_$_spr_vt12 = PythonOperator(
    task_id='yl_trans_by_SKU_dist3_$_spr_vt12',
    python_callable=Yltransbyskudist3$sprvt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 117: ('xw_trans_by_SKU_b_spr_prepare2__size2_vt12', 'Xwtransbyskubsprprepare2size2vt12')
xw_trans_by_SKU_b_spr_prepare2__size2_vt12 = PythonOperator(
    task_id='xw_trans_by_SKU_b_spr_prepare2__size2_vt12',
    python_callable=Xwtransbyskubsprprepare2size2vt12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
#  依赖关系 
# start
yl_trans_inv_more_next1mth_ >> yl_sku_filter_to_allocate_tmp1_19_
yl_sku_filter_to_allocate_tmp1_19_ >> yl_sku_filter_to_allocate_19__v2
yl_sku_filter_to_allocate_19__v2 >> yl_trans_by_SKU_master_spr_test_vt12
yl_trans_by_SKU_master_spr_test_vt12 >> yl_trans_by_SKU_dist1__spr_test_vt12
yl_trans_by_SKU_dist1__spr_test_vt12 >> yl_trans_by_SKU_ad_no_out_test_vt12
yl_trans_by_SKU_ad_no_out_test_vt12 >> yl_trans_by_SKU_dist2__spr_test_vt12
yl_trans_by_SKU_dist2__spr_test_vt12 >> yl_trans_by_SKU_dist2_temp_vt12
yl_trans_by_SKU_dist2_temp_vt12 >> yl_trans_by_SKU_dist2_temp_1st_vt12
yl_trans_by_SKU_dist2_temp_1st_vt12 >> yl_trans_by_SKU_temp_vt12
yl_trans_by_SKU_temp_vt12 >> yl_trans_by_SKU_dist3__spr_vt12
yl_trans_by_SKU_dist2__spr_test_vt12 >> yl_trans_by_SKU_dist3__spr_vt12
yl_trans_by_SKU_dist2__spr_test_vt12 >> yl_trans_by_SKU_score_adjust__spr_vt12
yl_trans_by_SKU_size_rank__spr_vt12 >> yl_trans_by_SKU_score_adjust__spr_vt12
yl_trans_by_SKU_score_adjust__spr_vt12 >> xw_trans_by_SKU_b_spr_prepare__vt12
xw_trans_by_SKU_b_spr_prepare__vt12 >> xw_trans_by_SKU_b_spr_prepare2__vt12
yl_trans_by_SKU_size_rank__spr_vt12 >> xw_trans_by_SKU_b_dist1__spr_size1_vt12
xw_trans_by_SKU_b_spr_prepare2__vt12 >> xw_trans_by_SKU_b_dist1__spr_size1_vt12
xw_trans_by_SKU_b_dist1__spr_size1_vt12 >> xw_trans_by_SKU_b_dist2__spr_size1_vt12
xw_trans_by_SKU_b_dist2__spr_size1_vt12 >> xw_trans_by_SKU_b_spr_prepare__size1_vt12
xw_trans_by_SKU_b_spr_prepare__vt12 >> xw_trans_by_SKU_b_spr_prepare__size1_vt12
xw_trans_by_SKU_b_spr_prepare__size1_vt12 >> xw_trans_by_SKU_b_spr_prepare2__size1_vt12
xw_trans_by_SKU_b_spr_prepare2__size1_vt12 >> xw_trans_by_SKU_b_dist1__spr_size2_vt12
yl_trans_by_SKU_size_rank__spr_vt12 >> xw_trans_by_SKU_b_dist1__spr_size2_vt12
xw_trans_by_SKU_b_dist1__spr_size2_vt12 >> xw_trans_by_SKU_b_dist2__spr_size2_vt12
xw_trans_by_SKU_b_spr_prepare__size1_vt12 >> xw_trans_by_SKU_b_spr_prepare__size2_vt12
xw_trans_by_SKU_b_dist2__spr_size2_vt12 >> xw_trans_by_SKU_b_spr_prepare__size2_vt12
xw_trans_by_SKU_b_spr_prepare__size2_vt12 >> xw_trans_by_SKU_b_spr_prepare2__size2_vt12
yl_trans_by_SKU_size_rank__spr_vt12 >> xw_trans_by_SKU_b_dist1__spr_size3_vt12
xw_trans_by_SKU_b_spr_prepare2__size2_vt12 >> xw_trans_by_SKU_b_dist1__spr_size3_vt12
xw_trans_by_SKU_b_dist1__spr_size3_vt12 >> xw_trans_by_SKU_b_dist2__spr_size3_vt12
xw_trans_by_SKU_b_dist2__spr_size3_vt12 >> xw_trans_by_SKU_b_spr_prepare__size3_vt12
xw_trans_by_SKU_b_spr_prepare__size2_vt12 >> xw_trans_by_SKU_b_spr_prepare__size3_vt12
xw_trans_by_SKU_b_spr_prepare__size3_vt12 >> xw_trans_by_SKU_b_spr_prepare2__size3_vt12
yl_trans_by_SKU_size_rank__spr_vt12 >> xw_trans_by_SKU_b_dist1__spr_size4_vt12
xw_trans_by_SKU_b_spr_prepare2__size3_vt12 >> xw_trans_by_SKU_b_dist1__spr_size4_vt12
xw_trans_by_SKU_b_dist1__spr_size4_vt12 >> xw_trans_by_SKU_b_dist2__spr_size4_vt12
xw_trans_by_SKU_b_dist2__spr_size4_vt12 >> xw_trans_by_SKU_b_spr_prepare__size4_vt12
xw_trans_by_SKU_b_spr_prepare__size3_vt12 >> xw_trans_by_SKU_b_spr_prepare__size4_vt12
xw_trans_by_SKU_b_spr_prepare__size4_vt12 >> xw_trans_by_SKU_b_spr_prepare2__size4_vt12
xw_trans_by_SKU_b_spr_prepare2__size4_vt12 >> xw_trans_by_SKU_b_dist2__spr_size5_vt12
xw_trans_by_SKU_b_dist2__spr_size5_vt12 >> xw_trans_by_SKU_temp_b_vt12
xw_trans_by_SKU_temp_b_vt12 >> xw_trans_by_SKU_b_dist3__spr_vt12
xw_trans_by_SKU_b_dist3__spr_vt12 >> xw_trans_by_sku_final__spr_vt12
yl_trans_by_SKU_dist3__spr_vt12 >> xw_trans_by_sku_final__spr_vt12
yl_sku_filter_to_allocate_19__v2 >> xw_trans_by_sku_final__spr_vt12
xw_trans_by_sku_final__spr_vt12 >> op_trans_by_sku_final__spr
yl_trans_by_sku_final__spr_vt12 >> op_trans_by_sku_final__spr
op_trans_by_sku_final__spr >> yl_temp_size_combine
op_trans_by_sku_final__spr >> yl_trans_by_SKU_b__spr_only_v1
yl_temp_size_combine >> yl_trans_by_SKU_b__spr_only_v1
op_trans_by_sku_final__spr >> yl_trans_by_SKU_b__spr_only_v2
yl_trans_by_SKU_b__spr_only_v1 >> op_trans_by_SKU_b__spr_only_final
yl_trans_by_SKU_b__spr_only_v2 >> op_trans_by_SKU_b__spr_only_final
yl_trans_by_SKU_score_adjust__spr_vt12 >> yl_trans_by_SKU_b_spr_prepare__vt12
yl_trans_by_SKU_b_spr_prepare__vt12 >> yl_trans_by_SKU_b_spr_prepare2__vt12
yl_trans_by_SKU_b_spr_prepare2__vt12 >> yl_trans_by_SKU_b_dist1__spr_size1_vt12
yl_trans_by_SKU_size_rank__spr_vt12 >> yl_trans_by_SKU_b_dist1__spr_size1_vt12
yl_trans_by_SKU_b_dist1__spr_size1_vt12 >> yl_trans_by_SKU_b_dist2__spr_size1_vt12
yl_trans_by_SKU_b_dist2__spr_size1_vt12 >> yl_trans_by_SKU_b_spr_prepare__size1_vt12
yl_trans_by_SKU_b_spr_prepare__vt12 >> yl_trans_by_SKU_b_spr_prepare__size1_vt12
yl_trans_by_SKU_b_spr_prepare__size1_vt12 >> yl_trans_by_SKU_b_spr_prepare2__size1_vt12
yl_trans_by_SKU_size_rank__spr_vt12 >> yl_trans_by_SKU_b_dist1__spr_size2_vt12
yl_trans_by_SKU_b_spr_prepare2__size1_vt12 >> yl_trans_by_SKU_b_dist1__spr_size2_vt12
yl_trans_by_SKU_b_dist1__spr_size2_vt12 >> yl_trans_by_SKU_b_dist2__spr_size2_vt12
yl_trans_by_SKU_b_dist2__spr_size2_vt12 >> yl_trans_by_SKU_b_spr_prepare__size2_vt12
yl_trans_by_SKU_b_spr_prepare__size1_vt12 >> yl_trans_by_SKU_b_spr_prepare__size2_vt12
yl_trans_by_SKU_b_spr_prepare__size2_vt12 >> yl_trans_by_SKU_b_spr_prepare2__size2_vt12
yl_trans_by_SKU_b_spr_prepare2__size2_vt12 >> yl_trans_by_SKU_b_dist1__spr_size3_vt12
yl_trans_by_SKU_size_rank__spr_vt12 >> yl_trans_by_SKU_b_dist1__spr_size3_vt12
yl_trans_by_SKU_b_dist1__spr_size3_vt12 >> yl_trans_by_SKU_b_dist2__spr_size3_vt12
yl_trans_by_SKU_b_spr_prepare__size2_vt12 >> yl_trans_by_SKU_b_spr_prepare__size3_vt12
yl_trans_by_SKU_b_dist2__spr_size3_vt12 >> yl_trans_by_SKU_b_spr_prepare__size3_vt12
yl_trans_by_SKU_b_spr_prepare__size3_vt12 >> yl_trans_by_SKU_b_spr_prepare2__size3_vt12
yl_trans_by_SKU_b_spr_prepare2__size3_vt12 >> yl_trans_by_SKU_b_dist1__spr_size4_vt12
yl_trans_by_SKU_size_rank__spr_vt12 >> yl_trans_by_SKU_b_dist1__spr_size4_vt12
yl_trans_by_SKU_b_dist1__spr_size4_vt12 >> yl_trans_by_SKU_b_dist2__spr_size4_vt12
yl_trans_by_SKU_b_spr_prepare__size3_vt12 >> yl_trans_by_SKU_b_spr_prepare__size4_vt12
yl_trans_by_SKU_b_dist2__spr_size4_vt12 >> yl_trans_by_SKU_b_spr_prepare__size4_vt12
yl_trans_by_SKU_b_spr_prepare__size4_vt12 >> yl_trans_by_SKU_b_spr_prepare2__size4_vt12
yl_trans_by_SKU_b_spr_prepare2__size4_vt12 >> yl_trans_by_SKU_b_dist2__spr_size5_vt12
yl_trans_by_SKU_b_dist2__spr_size5_vt12 >> yl_trans_by_SKU_temp_b_vt12
yl_trans_by_SKU_temp_b_vt12 >> yl_trans_by_SKU_b_dist3__spr_vt12
yl_trans_by_SKU_b_dist3__spr_vt12 >> yl_trans_by_sku_final__spr_vt12
yl_trans_by_SKU_dist3__spr_vt12 >> yl_trans_by_sku_final__spr_vt12
yl_sku_filter_to_allocate_19__v2 >> yl_trans_by_sku_final__spr_vt12
yl_trans_by_SKU_score_adjust_$_spr_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_vt12
yl_trans_by_SKU_b_spr_prepare_$_vt12 >> yl_trans_by_SKU_b_spr_prepare2_$_vt12
yl_trans_by_SKU_b_spr_prepare2_$_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size1_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size1_vt12
yl_trans_by_SKU_b_dist1_$_spr_size1_vt12 >> yl_trans_by_SKU_b_dist2_$_spr_size1_vt12
yl_trans_by_SKU_b_dist2_$_spr_size1_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size1_vt12
yl_trans_by_SKU_b_spr_prepare_$_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size1_vt12
yl_trans_by_SKU_b_spr_prepare_$_size1_vt12 >> yl_trans_by_SKU_b_spr_prepare2_$_size1_vt12
yl_trans_by_SKU_b_spr_prepare2_$_size1_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size2_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size2_vt12
yl_trans_by_SKU_b_dist1_$_spr_size2_vt12 >> yl_trans_by_SKU_b_dist2_$_spr_size2_vt12
yl_trans_by_SKU_b_spr_prepare_$_size1_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size2_vt12
yl_trans_by_SKU_b_dist2_$_spr_size2_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size2_vt12
yl_trans_by_SKU_b_spr_prepare_$_size2_vt12 >> yl_trans_by_SKU_b_spr_prepare2_$_size2_vt12
yl_trans_by_SKU_b_spr_prepare2_$_size2_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size3_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size3_vt12
yl_trans_by_SKU_b_dist1_$_spr_size3_vt12 >> yl_trans_by_SKU_b_dist2_$_spr_size3_vt12
yl_trans_by_SKU_b_spr_prepare_$_size2_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size3_vt12
yl_trans_by_SKU_b_dist2_$_spr_size3_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size3_vt12
yl_trans_by_SKU_b_spr_prepare_$_size3_vt12 >> yl_trans_by_SKU_b_spr_prepare2_$_size3_vt12
yl_trans_by_SKU_b_spr_prepare2_$_size3_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size4_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size4_vt12
yl_trans_by_SKU_b_dist1_$_spr_size4_vt12 >> yl_trans_by_SKU_b_dist2_$_spr_size4_vt12
yl_trans_by_SKU_b_dist2_$_spr_size4_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size4_vt12
yl_trans_by_SKU_b_spr_prepare_$_size3_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size4_vt12
yl_trans_by_SKU_b_spr_prepare_$_size4_vt12 >> yl_trans_by_SKU_b_spr_prepare2_$_size4_vt12
yl_trans_by_SKU_b_spr_prepare2_$_size4_vt12 >> yl_trans_by_SKU_b_dist2_$_spr_size5_vt12
yl_trans_by_SKU_b_dist2_$_spr_size5_vt12 >> yl_trans_by_SKU_temp_b_vt12
yl_trans_by_SKU_temp_b_vt12 >> yl_trans_by_SKU_b_dist3_$_spr_vt12
yl_sku_filter_to_allocate_19_$_v2 >> yl_trans_by_sku_final_$_spr_vt12
yl_trans_by_SKU_b_dist3_$_spr_vt12 >> yl_trans_by_sku_final_$_spr_vt12
yl_trans_by_SKU_dist3_$_spr_vt12 >> yl_trans_by_sku_final_$_spr_vt12
yl_trans_inv_more_next1mth_$ >> yl_sku_filter_to_allocate_tmp1_19_$
yl_sku_filter_to_allocate_tmp1_19_$ >> yl_sku_filter_to_allocate_19_$_v2
yl_sku_filter_to_allocate_19_$_v2 >> yl_trans_by_SKU_master$_spr_test_vt12
yl_trans_by_SKU_master$_spr_test_vt12 >> yl_trans_by_SKU_dist1_$_spr_test_vt12
yl_trans_by_SKU_dist1_$_spr_test_vt12 >> yl_trans_by_SKU_ad_no_out_test_vt12
yl_trans_by_SKU_ad_no_out_test_vt12 >> yl_trans_by_SKU_dist2_$_spr_test_vt12
yl_trans_by_SKU_dist2_$_spr_test_vt12 >> yl_trans_by_SKU_dist2_temp_vt12
yl_trans_by_SKU_dist2_temp_vt12 >> yl_trans_by_SKU_dist2_temp_1st_vt12
yl_trans_by_SKU_dist2_temp_1st_vt12 >> yl_trans_by_SKU_temp_vt12
yl_trans_by_SKU_temp_vt12 >> yl_trans_by_SKU_dist3_$_spr_vt12
yl_trans_by_SKU_dist2_$_spr_test_vt12 >> yl_trans_by_SKU_dist3_$_spr_vt12
yl_trans_by_SKU_dist2_$_spr_test_vt12 >> yl_trans_by_SKU_score_adjust_$_spr_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> yl_trans_by_SKU_score_adjust_$_spr_vt12
yl_trans_by_SKU_score_adjust_$_spr_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_vt12
xw_trans_by_SKU_b_spr_prepare_$_vt12 >> xw_trans_by_SKU_b_spr_prepare2_$_vt12
xw_trans_by_SKU_b_spr_prepare2_$_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size1_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size1_vt12
xw_trans_by_SKU_b_dist1_$_spr_size1_vt12 >> xw_trans_by_SKU_b_dist2_$_spr_size1_vt12
xw_trans_by_SKU_b_spr_prepare_$_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size1_vt12
xw_trans_by_SKU_b_dist2_$_spr_size1_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size1_vt12
xw_trans_by_SKU_b_spr_prepare_$_size1_vt12 >> xw_trans_by_SKU_b_spr_prepare2_$_size1_vt12
xw_trans_by_SKU_b_spr_prepare2_$_size1_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size2_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size2_vt12
xw_trans_by_SKU_b_dist1_$_spr_size2_vt12 >> xw_trans_by_SKU_b_dist2_$_spr_size2_vt12
xw_trans_by_SKU_b_spr_prepare_$_size1_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size2_vt12
xw_trans_by_SKU_b_dist2_$_spr_size2_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size2_vt12
xw_trans_by_SKU_b_spr_prepare_$_size2_vt12 >> xw_trans_by_SKU_b_spr_prepare2_$_size2_vt12
xw_trans_by_SKU_b_spr_prepare2_$_size2_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size3_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size3_vt12
xw_trans_by_SKU_b_dist1_$_spr_size3_vt12 >> xw_trans_by_SKU_b_dist2_$_spr_size3_vt12
xw_trans_by_SKU_b_dist2_$_spr_size3_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size3_vt12
xw_trans_by_SKU_b_spr_prepare_$_size2_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size3_vt12
xw_trans_by_SKU_b_spr_prepare_$_size3_vt12 >> xw_trans_by_SKU_b_spr_prepare2_$_size3_vt12
xw_trans_by_SKU_b_spr_prepare2_$_size3_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size4_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size4_vt12
xw_trans_by_SKU_b_dist1_$_spr_size4_vt12 >> xw_trans_by_SKU_b_dist2_$_spr_size4_vt12
xw_trans_by_SKU_b_dist2_$_spr_size4_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size4_vt12
xw_trans_by_SKU_b_spr_prepare_$_size3_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size4_vt12
xw_trans_by_SKU_b_spr_prepare_$_size4_vt12 >> xw_trans_by_SKU_b_spr_prepare2_$_size4_vt12
xw_trans_by_SKU_b_spr_prepare2_$_size4_vt12 >> xw_trans_by_SKU_b_dist2_$_spr_size5_vt12
xw_trans_by_SKU_b_dist2_$_spr_size5_vt12 >> xw_trans_by_SKU_temp_b_vt12
xw_trans_by_SKU_temp_b_vt12 >> xw_trans_by_SKU_b_dist3_$_spr_vt12
yl_sku_filter_to_allocate_19_$_v2 >> xw_trans_by_sku_final_$_spr_vt12
xw_trans_by_SKU_b_dist3_$_spr_vt12 >> xw_trans_by_sku_final_$_spr_vt12
yl_trans_by_SKU_dist3_$_spr_vt12 >> xw_trans_by_sku_final_$_spr_vt12
yl_trans_by_sku_final_$_spr_vt12 >> op_trans_by_sku_final_$_spr
xw_trans_by_sku_final_$_spr_vt12 >> op_trans_by_sku_final_$_spr
op_trans_by_sku_final_$_spr >> yl_temp_size_combine
op_trans_by_sku_final_$_spr >> yl_trans_by_SKU_b_$_spr_only_v1
yl_temp_size_combine >> yl_trans_by_SKU_b_$_spr_only_v1
op_trans_by_sku_final_$_spr >> yl_trans_by_SKU_b_$_spr_only_v2
yl_trans_by_SKU_b_$_spr_only_v2 >> op_trans_by_SKU_b_$_spr_only_final
yl_trans_by_SKU_b_$_spr_only_v1 >> op_trans_by_SKU_b_$_spr_only_final
yl_trans_by_SKU_score_adjust_$_spr_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_vt12
yl_trans_by_SKU_b_spr_prepare_$_vt12 >> yl_trans_by_SKU_b_spr_prepare2_$_vt12
yl_trans_by_SKU_b_spr_prepare2_$_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size1_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size1_vt12
yl_trans_by_SKU_b_dist1_$_spr_size1_vt12 >> yl_trans_by_SKU_b_dist2_$_spr_size1_vt12
yl_trans_by_SKU_b_dist2_$_spr_size1_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size1_vt12
yl_trans_by_SKU_b_spr_prepare_$_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size1_vt12
yl_trans_by_SKU_b_spr_prepare_$_size1_vt12 >> yl_trans_by_SKU_b_spr_prepare2_$_size1_vt12
yl_trans_by_SKU_b_spr_prepare2_$_size1_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size2_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size2_vt12
yl_trans_by_SKU_b_dist1_$_spr_size2_vt12 >> yl_trans_by_SKU_b_dist2_$_spr_size2_vt12
yl_trans_by_SKU_b_spr_prepare_$_size1_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size2_vt12
yl_trans_by_SKU_b_dist2_$_spr_size2_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size2_vt12
yl_trans_by_SKU_b_spr_prepare_$_size2_vt12 >> yl_trans_by_SKU_b_spr_prepare2_$_size2_vt12
yl_trans_by_SKU_b_spr_prepare2_$_size2_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size3_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size3_vt12
yl_trans_by_SKU_b_dist1_$_spr_size3_vt12 >> yl_trans_by_SKU_b_dist2_$_spr_size3_vt12
yl_trans_by_SKU_b_spr_prepare_$_size2_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size3_vt12
yl_trans_by_SKU_b_dist2_$_spr_size3_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size3_vt12
yl_trans_by_SKU_b_spr_prepare_$_size3_vt12 >> yl_trans_by_SKU_b_spr_prepare2_$_size3_vt12
yl_trans_by_SKU_b_spr_prepare2_$_size3_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size4_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> yl_trans_by_SKU_b_dist1_$_spr_size4_vt12
yl_trans_by_SKU_b_dist1_$_spr_size4_vt12 >> yl_trans_by_SKU_b_dist2_$_spr_size4_vt12
yl_trans_by_SKU_b_dist2_$_spr_size4_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size4_vt12
yl_trans_by_SKU_b_spr_prepare_$_size3_vt12 >> yl_trans_by_SKU_b_spr_prepare_$_size4_vt12
yl_trans_by_SKU_b_spr_prepare_$_size4_vt12 >> yl_trans_by_SKU_b_spr_prepare2_$_size4_vt12
yl_trans_by_SKU_b_spr_prepare2_$_size4_vt12 >> yl_trans_by_SKU_b_dist2_$_spr_size5_vt12
yl_trans_by_SKU_b_dist2_$_spr_size5_vt12 >> yl_trans_by_SKU_temp_b_vt12
yl_trans_by_SKU_temp_b_vt12 >> yl_trans_by_SKU_b_dist3_$_spr_vt12
yl_sku_filter_to_allocate_19_$_v2 >> yl_trans_by_sku_final_$_spr_vt12
yl_trans_by_SKU_b_dist3_$_spr_vt12 >> yl_trans_by_sku_final_$_spr_vt12
yl_trans_by_SKU_dist3_$_spr_vt12 >> yl_trans_by_sku_final_$_spr_vt12
yl_trans_by_SKU_score_adjust_$_spr_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_vt12
xw_trans_by_SKU_b_spr_prepare_$_vt12 >> xw_trans_by_SKU_b_spr_prepare2_$_vt12
xw_trans_by_SKU_b_spr_prepare2_$_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size1_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size1_vt12
xw_trans_by_SKU_b_dist1_$_spr_size1_vt12 >> xw_trans_by_SKU_b_dist2_$_spr_size1_vt12
xw_trans_by_SKU_b_spr_prepare_$_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size1_vt12
xw_trans_by_SKU_b_dist2_$_spr_size1_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size1_vt12
xw_trans_by_SKU_b_spr_prepare_$_size1_vt12 >> xw_trans_by_SKU_b_spr_prepare2_$_size1_vt12
xw_trans_by_SKU_b_spr_prepare2_$_size1_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size2_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size2_vt12
xw_trans_by_SKU_b_dist1_$_spr_size2_vt12 >> xw_trans_by_SKU_b_dist2_$_spr_size2_vt12
xw_trans_by_SKU_b_spr_prepare_$_size1_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size2_vt12
xw_trans_by_SKU_b_dist2_$_spr_size2_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size2_vt12
xw_trans_by_SKU_b_spr_prepare_$_size2_vt12 >> xw_trans_by_SKU_b_spr_prepare2_$_size2_vt12
xw_trans_by_SKU_b_spr_prepare2_$_size2_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size3_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size3_vt12
xw_trans_by_SKU_b_dist1_$_spr_size3_vt12 >> xw_trans_by_SKU_b_dist2_$_spr_size3_vt12
xw_trans_by_SKU_b_dist2_$_spr_size3_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size3_vt12
xw_trans_by_SKU_b_spr_prepare_$_size2_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size3_vt12
xw_trans_by_SKU_b_spr_prepare_$_size3_vt12 >> xw_trans_by_SKU_b_spr_prepare2_$_size3_vt12
xw_trans_by_SKU_b_spr_prepare2_$_size3_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size4_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> xw_trans_by_SKU_b_dist1_$_spr_size4_vt12
xw_trans_by_SKU_b_dist1_$_spr_size4_vt12 >> xw_trans_by_SKU_b_dist2_$_spr_size4_vt12
xw_trans_by_SKU_b_dist2_$_spr_size4_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size4_vt12
xw_trans_by_SKU_b_spr_prepare_$_size3_vt12 >> xw_trans_by_SKU_b_spr_prepare_$_size4_vt12
xw_trans_by_SKU_b_spr_prepare_$_size4_vt12 >> xw_trans_by_SKU_b_spr_prepare2_$_size4_vt12
xw_trans_by_SKU_b_spr_prepare2_$_size4_vt12 >> xw_trans_by_SKU_b_dist2_$_spr_size5_vt12
xw_trans_by_SKU_b_dist2_$_spr_size5_vt12 >> xw_trans_by_SKU_temp_b_vt12
xw_trans_by_SKU_temp_b_vt12 >> xw_trans_by_SKU_b_dist3_$_spr_vt12
yl_sku_filter_to_allocate_19_$_v2 >> xw_trans_by_sku_final_$_spr_vt12
xw_trans_by_SKU_b_dist3_$_spr_vt12 >> xw_trans_by_sku_final_$_spr_vt12
yl_trans_by_SKU_dist3_$_spr_vt12 >> xw_trans_by_sku_final_$_spr_vt12
yl_trans_by_sku_final_$_spr_vt12 >> op_trans_by_sku_final_$_spr
xw_trans_by_sku_final_$_spr_vt12 >> op_trans_by_sku_final_$_spr
op_trans_by_sku_final_$_spr >> yl_temp_size_combine
op_trans_by_sku_final_$_spr >> yl_trans_by_SKU_b_$_spr_only_v1
yl_temp_size_combine >> yl_trans_by_SKU_b_$_spr_only_v1
op_trans_by_sku_final_$_spr >> yl_trans_by_SKU_b_$_spr_only_v2
yl_trans_by_SKU_b_$_spr_only_v2 >> op_trans_by_SKU_b_$_spr_only_final
yl_trans_by_SKU_b_$_spr_only_v1 >> op_trans_by_SKU_b_$_spr_only_final
yl_sku_filter_to_allocate_19_$_v2 >> yl_trans_by_SKU_master$_spr_test_vt12
yl_trans_by_SKU_master$_spr_test_vt12 >> yl_trans_by_SKU_dist1_$_spr_test_vt12
yl_trans_by_SKU_dist1_$_spr_test_vt12 >> yl_trans_by_SKU_ad_no_out_test_vt12
yl_trans_by_SKU_ad_no_out_test_vt12 >> yl_trans_by_SKU_dist2_$_spr_test_vt12
yl_trans_by_SKU_dist2_$_spr_test_vt12 >> yl_trans_by_SKU_dist2_temp_vt12
yl_trans_by_SKU_dist2_temp_vt12 >> yl_trans_by_SKU_dist2_temp_1st_vt12
yl_trans_by_SKU_dist2_temp_1st_vt12 >> yl_trans_by_SKU_temp_vt12
yl_trans_by_SKU_temp_vt12 >> yl_trans_by_SKU_dist3_$_spr_vt12
yl_trans_by_SKU_dist2_$_spr_test_vt12 >> yl_trans_by_SKU_dist3_$_spr_vt12
yl_trans_by_SKU_dist2_$_spr_test_vt12 >> yl_trans_by_SKU_score_adjust_$_spr_vt12
yl_trans_by_SKU_size_rank_$_spr_vt12 >> yl_trans_by_SKU_score_adjust_$_spr_vt12

# end
