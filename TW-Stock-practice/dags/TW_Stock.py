import os
import time
import logging
import pandas as pd
import requests

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator

from TW_Stock_subdags.clean_data_subdag import clean_data_subdag
from TW_Stock_subdags.get_metadata_subdag import get_metadata_subdag


default_args = {
    'owner': 'AxotZero',
    'start_date': datetime(2020, 8, 4),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


def check_weekday(date_stamp):
	today = datetime.strptime('20200805', '%Y%m%d')

	if today.isoweekday() <= 5:
		return 'is_workday'
	else:
		return 'is_holiday'


dag_id = 'TW_Stock'
with DAG(dag_id=dag_id, default_args=default_args) as dag:


	tw_stock_start = DummyOperator(
		task_id='tw_stock_start'
	)

	check_weekday = BranchPythonOperator(
		task_id='check_weekday',
		python_callable=check_weekday,
		op_args=['{{ ds_nodash }}']
	)

	is_holiday = DummyOperator(
		task_id='is_holiday'
	)

	is_workday = DummyOperator(
		task_id='is_workday'
	)

	get_metadata = SubDagOperator(
		task_id='get_metadata',
		subdag=get_metadata_subdag(dag_id, 'get_metadata', default_args),
	)

	clean_data = SubDagOperator(
		task_id='clean_data',
		subdag=clean_data_subdag(dag_id, 'clean_data', default_args),
	)

	tw_stock_end = DummyOperator(
		task_id='tw_stock_end',
		trigger_rule='one_success'
	)

	tw_stock_start >> check_weekday >> [is_workday, is_holiday]
	is_holiday >> tw_stock_end
	is_workday >> get_metadata >> clean_data >> tw_stock_end
	


