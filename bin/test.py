# -*- coding: utf-8 -*-
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from datetime import datetime

from xf.abc import abc
from xf.abc import bcd
from xf.abc import dee
from xf.abc import efg

#-------------------------------------------------------------------------------
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':datetime(2019,03,28),
    'email': ['329472010@qq.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

#-------------------------------------------------------------------------------
# DAG id '' 必须是unique的, 一般与文件名相同
dag = DAG(
    'test_2019032917',
    default_args=args,
    schedule_interval='0 10 * * *'
    )

# -------------------------------------------------------------------------------
# operator 1: abc
abc = PythonOperator(
    task_id='abc',
    python_callable=abc().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 2: bcd
bcd = PythonOperator(
    task_id='bcd',
    python_callable=bcd().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 3: dee
dee = PythonOperator(
    task_id='dee',
    python_callable=dee().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
# operator 4: efg
efg = PythonOperator(
    task_id='efg',
    python_callable=efg().run_command,
    dag=dag)

# -------------------------------------------------------------------------------
#  依赖关系 
# start
abc >> bcd >>
dee >> efg

# end
