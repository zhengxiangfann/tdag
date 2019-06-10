# -*- coding: utf-8 -*-
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from datetime import datetime

from zxf.plan.hg_auto_online_rank_pred_master_spuLvl_step0_1 import Hgautoonlinerankpredmasterspulvlstep01
from zxf.plan.hg_auto_online_rank_pred_master_spuLvl_step1_2 import Hgautoonlinerankpredmasterspulvlstep12
from zxf.plan.hg_auto_online_rank_pred_master_spuLvl_step3 import Hgautoonlinerankpredmasterspulvlstep3
from zxf.plan.hg_auto_online_rank_pred_master_v3_train_spuLvl import Hgautoonlinerankpredmasterv3trainspulvl
from zxf.plan.hg_auto_online_rank_pred_master_v3_test_spuLvl import Hgautoonlinerankpredmasterv3testspulvl
from zxf.plan.hg_auto_online_rank_pred_master_spuLvl_step1_3 import Hgautoonlinerankpredmasterspulvlstep13
from zxf.plan.hg_auto_online_rank_pred_master_spuLvl_step2 import Hgautoonlinerankpredmasterspulvlstep2
from zxf.plan.hg_auto_online_rank_pred_master_spuLvl_step1_1 import Hgautoonlinerankpredmasterspulvlstep11

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
    'pro_plan_2019052113',
    default_args=args,
    schedule_interval='0 10 * * 1'
    )

# -------------------------------------------------------------------------------
# operator 1: ('hg_auto_online_rank_pred_master_spuLvl_step0_1', 'Hgautoonlinerankpredmasterspulvlstep01')
hg_auto_online_rank_pred_master_spuLvl_step0_1 = PythonOperator(
    task_id='hg_auto_online_rank_pred_master_spuLvl_step0_1',
    python_callable=Hgautoonlinerankpredmasterspulvlstep01().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 2: ('hg_auto_online_rank_pred_master_spuLvl_step1_2', 'Hgautoonlinerankpredmasterspulvlstep12')
hg_auto_online_rank_pred_master_spuLvl_step1_2 = PythonOperator(
    task_id='hg_auto_online_rank_pred_master_spuLvl_step1_2',
    python_callable=Hgautoonlinerankpredmasterspulvlstep12().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 3: ('hg_auto_online_rank_pred_master_spuLvl_step3', 'Hgautoonlinerankpredmasterspulvlstep3')
hg_auto_online_rank_pred_master_spuLvl_step3 = PythonOperator(
    task_id='hg_auto_online_rank_pred_master_spuLvl_step3',
    python_callable=Hgautoonlinerankpredmasterspulvlstep3().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 4: ('hg_auto_online_rank_pred_master_v3_train_spuLvl', 'Hgautoonlinerankpredmasterv3trainspulvl')
hg_auto_online_rank_pred_master_v3_train_spuLvl = PythonOperator(
    task_id='hg_auto_online_rank_pred_master_v3_train_spuLvl',
    python_callable=Hgautoonlinerankpredmasterv3trainspulvl().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 5: ('hg_auto_online_rank_pred_master_v3_test_spuLvl', 'Hgautoonlinerankpredmasterv3testspulvl')
hg_auto_online_rank_pred_master_v3_test_spuLvl = PythonOperator(
    task_id='hg_auto_online_rank_pred_master_v3_test_spuLvl',
    python_callable=Hgautoonlinerankpredmasterv3testspulvl().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 6: ('hg_auto_online_rank_pred_master_spuLvl_step1_3', 'Hgautoonlinerankpredmasterspulvlstep13')
hg_auto_online_rank_pred_master_spuLvl_step1_3 = PythonOperator(
    task_id='hg_auto_online_rank_pred_master_spuLvl_step1_3',
    python_callable=Hgautoonlinerankpredmasterspulvlstep13().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 7: ('hg_auto_online_rank_pred_master_spuLvl_step2', 'Hgautoonlinerankpredmasterspulvlstep2')
hg_auto_online_rank_pred_master_spuLvl_step2 = PythonOperator(
    task_id='hg_auto_online_rank_pred_master_spuLvl_step2',
    python_callable=Hgautoonlinerankpredmasterspulvlstep2().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 8: ('hg_auto_online_rank_pred_master_spuLvl_step1_1', 'Hgautoonlinerankpredmasterspulvlstep11')
hg_auto_online_rank_pred_master_spuLvl_step1_1 = PythonOperator(
    task_id='hg_auto_online_rank_pred_master_spuLvl_step1_1',
    python_callable=Hgautoonlinerankpredmasterspulvlstep11().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
#  依赖关系 
# start
hg_auto_online_rank_pred_master_spuLvl_step0_1 >> hg_auto_online_rank_pred_master_spuLvl_step1_1
hg_auto_online_rank_pred_master_spuLvl_step1_1 >> hg_auto_online_rank_pred_master_spuLvl_step1_2
hg_auto_online_rank_pred_master_spuLvl_step1_1 >> hg_auto_online_rank_pred_master_spuLvl_step1_3
hg_auto_online_rank_pred_master_spuLvl_step1_3 >> hg_auto_online_rank_pred_master_spuLvl_step2
hg_auto_online_rank_pred_master_spuLvl_step1_2 >> hg_auto_online_rank_pred_master_spuLvl_step2
hg_auto_online_rank_pred_master_spuLvl_step2 >> hg_auto_online_rank_pred_master_spuLvl_step3
hg_auto_online_rank_pred_master_spuLvl_step3 >> hg_auto_online_rank_pred_master_v3_train_spuLvl
hg_auto_online_rank_pred_master_spuLvl_step3 >> hg_auto_online_rank_pred_master_v3_test_spuLvl

# end
