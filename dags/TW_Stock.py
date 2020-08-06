# import os
# import time
# import logging
# import pandas as pd
# import requests

# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.latest_only_operator import LatestOnlyOperator



# default_args = {
#     'owner': 'AxotZero',
#     'start_date': datetime(2100, 1, 1),
#     'schedule_interval': '@daily',
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5)
# }


# def check_weekday(date_stamp):
# 	today = datetime(date_stamp)
# 	if today.isoweekday() <= 5:
# 		return 'is_workday'
# 	else:
# 		return 'is_holiday'


# with DAG('TW_Stock', default_args=default_args) as dag:


# 	tw_stock_start = DummyOperator(
# 		task_id='tw_stock_start'
# 	)
# 	tw_stock_end = DummyOperator(
# 		task_id='tw_stock_end'
# 	)

# 	check_weekday = BranchPythonOperator(
# 		task_id='check_weekday',
# 		python_callable=check_weekday,
# 		op_args='{{ ds_nodash }}'
# 	)


# 	dag_start >> check_weekday >> [is_workday, is_holiday]
# 	is_holiday >> tw_stock_end
# 	is_wokday >> get_metadata >> clean_data >> tw_stock_end
	


