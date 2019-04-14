# -*- coding: utf-8 -*-
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from datetime import datetime

from test.test.pro_month_sku_sal_18 import Promonthskusal18
from test.test.pro_month_sal_rank_final_18 import Promonthsalrankfinal18

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
    'dag_id_2019041409',
    default_args=args,
    schedule_interval='0 9 * * *'
    )

# -------------------------------------------------------------------------------
# operator 1: ('pro_month_sku_sal_18', 'Promonthskusal18')
pro_month_sku_sal_18 = PythonOperator(
    task_id='pro_month_sku_sal_18',
    python_callable=Promonthskusal18().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 2: ('pro_month_sal_rank_final_18', 'Promonthsalrankfinal18')
pro_month_sal_rank_final_18 = PythonOperator(
    task_id='pro_month_sal_rank_final_18',
    python_callable=Promonthsalrankfinal18().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
#  依赖关系 
# start

# end
