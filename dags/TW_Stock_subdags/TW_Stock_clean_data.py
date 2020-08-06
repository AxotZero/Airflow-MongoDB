import pymongo
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.mongo_hook import MongoHook
import logging

default_args = {
	'owner': 'AxotZero',
	'start_date': datetime(2020, 8, 3),
	'schedule_interval': '@daily',
	'retries': 2,
	'retry_delay': timedelta(minutes=1)
}

tmp_config = {
	'db': 'test',
	'collection': 'tmp'
}

metadata_config = {
	'db': 'test',
	'collection': 'metadata'
}

cleandata_config = {
	'db': 'test',
	'collection': 'cleandata'
}


def set_config(date_stamp):
	global tmp_config
	tmp_config['collection'] = 'tmp' + date_stamp


def update_tmp(query):
	MongoHook(conn_id='mongo_default').update_many(
		filter_doc={},
		update_doc=query,
		mongo_db=tmp_config['db'],
		mongo_collection=tmp_config['collection']
	)


def get_tmp_table(date_stamp):
	# get data
	data = list(
		MongoHook(conn_id='mongo_default').find(
			query={},
			mongo_db=metadata_config['db'], 
			mongo_collection=metadata_config['collection']
		)
	)

	# empty
	MongoHook(conn_id='mongo_default').delete_many(
		filter_doc={}, 
		mongo_db=tmp_config['db'], 
		mongo_collection=tmp_config['collection']
	)

	# insert data
	MongoHook(conn_id='mongo_default').insert_many(
		docs=data, 
		mongo_db=tmp_config['db'], 
		mongo_collection=tmp_config['collection']
	)


def rename_columns(origin_names, new_names):
	rename_dict = {}
	for i, (origin_name, new_name) in enumerate(zip(origin_names, new_names)):
		rename_dict[origin_name] = new_name
	
	query = {"$rename": rename_dict}

	# rename columns
	MongoHook(conn_id='mongo_default').update_many(
		filter_doc={},
		update_doc=query,
		mongo_db='test',
		mongo_collection='tmp'
	)


def keep_columns(columns):
	project_dict = {}
	for col in columns:
		project_dict[col] = 1

	query = [{"$project": project_dict}]

	update_tmp(query)
	
  
def convert_field_to_double(col):
	# remove comma
	remove_comma_query = [{
		'$set':{
			col:{
				'$replaceAll':{
					"input": "$"+col, 
					"find": ",",
					"replacement": ""
				}
			}
		}
	}]
	try:
		update_tmp(remove_comma_query)
	except pymongo.errors.WriteError:
		print("remove_field_comma on column '{}' encounter number input.".format(col))


	# convert to double
	convert_query = [{
		"$set":{
			col: {
				'$convert':{
					'input': "$"+col, 
					'to': 'double', 
					'onError': None
				}
			}
		}
	}]
	update_tmp(convert_query)


def compute_updown():
	query = [{
		'$set':{
			"up_down":{
				"$cond":{
					"if": { "$eq": ["$up_down(+/-)", "+"]},
					"then": "$up_down(diff)",
					"else": {"$multiply": [-1, "$up_down(diff)"]}
				}
			}
		}
	}]
	update_tmp(query)


def save_to_cleandata_table():

	tmp = MongoHook(conn_id='mongo_default').get_collection(
		mongo_db=tmp_config['db'], 
		mongo_collection=tmp_config['collection']
	)

	# get data
	tmp_data = list(tmp.find({}))

	# insert to cleandata_table
	MongoHook(conn_id='mongo_default').insert_many(
		docs=tmp_data, 
		mongo_db=cleandata_config['db'], 
		mongo_collection=cleandata_config['collection']
	)

	# drop tmp collection
	tmp.drop()



with DAG('TW_Stock_clean_data', default_args=default_args) as sub_dag:

	clean_data_start = DummyOperator(
		task_id='clean_data_start'
	)

	set_config = PythonOperator(
		task_id='set_config',
		python_callable=set_config,
		op_args=['{{ ds_nodash }}']
	)

	get_tmp_table = PythonOperator(
		task_id='get_tmp_table',
		python_callable=get_tmp_table,
		op_args=['{{ ds_nodash }}']
	)

	rename_columns = PythonOperator(
		task_id='rename_columns',
		python_callable=rename_columns,
		op_kwargs={
			'origin_names':['證券代號', '最高價', '最低價', '開盤價', '收盤價', '成交筆數', '漲跌(+/-)', '漲跌價差'],
			'new_names': ['code', 'day_high', 'day_low', 'open_price', 'close_price', 'trans_num', 'up_down(+/-)', 'up_down(diff)']
		}
	)

	keep_use_columns = PythonOperator(
		task_id='keep_use_columns',
		python_callable=keep_columns,
		op_args=[['code', 'date_stamp', 'trans_num', 'day_high', 'day_low', 'open_price', 'close_price' , 'up_down(+/-)', 'up_down(diff)']]
	)

	convert_columns_to_double = []

	for col in ['day_high', 'day_low', 'open_price', 'close_price', 'trans_num', 'up_down(diff)']:
		task_id = 'convert_{}_to_double'.format(col).replace('(', '').replace(')', '')

		t = PythonOperator(
			task_id=task_id,
			python_callable=convert_field_to_double,
			op_args=[col]
		)

		convert_columns_to_double.append(t)

	compute_updown = PythonOperator(
		task_id='compute_updown',
		python_callable=compute_updown,
	)

	keep_final_columns = PythonOperator(
		task_id='keep_final_columns',
		python_callable=keep_columns,
		op_args=[['code', 'date_stamp', 'trans_num', 'day_high', 'day_low', 'open_price', 'close_price', 'up_down']]
	)

	save_to_cleandata_table = PythonOperator(
		task_id='save_to_cleandata_table',
		python_callable=save_to_cleandata_table,
	)

	clean_data_end = DummyOperator(
		task_id='clean_data_end'
	)


	clean_data_start >> set_config >> get_tmp_table >> rename_columns >> keep_use_columns >> convert_columns_to_double
	convert_columns_to_double >> compute_updown >> keep_final_columns >> save_to_cleandata_table >> clean_data_end

