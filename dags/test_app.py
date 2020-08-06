from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
import time
from datetime import datetime, timedelta


default_args = {
    'owner': 'Axot',
    'start_date': datetime(2020, 7, 26, 0, 0),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(seconds=1)
}


def start():
	print('start')


def task_0():
	print('task_0')


def task_1():
	print('task_1')


def branch():
	print('branch')
	
	# rand = int(time.time()) % 2

	# if rand == 0:
	# 	return 'yes_task_0'
	# else:
	# 	return 'no_task_1'


def end():
	print('end')


with DAG('test_app', default_args=default_args) as dag:

	start = PythonOperator(
		task_id='start',
		python_callable=start
	)
	task_0 = PythonOperator(
		task_id='yes_task_0',
		python_callable=task_0
	)
	task_1 = PythonOperator(
		task_id='no_task_1',
		python_callable=task_1
	)
	branch = PythonOperator(
		task_id='branch',
		python_callable=branch
	)
	end = PythonOperator(
		task_id='end',
		python_callable=end
	)

	start  >> branch
	branch >> task_0
	branch >> task_1
	task_0 >> end
	task_1 >> end
