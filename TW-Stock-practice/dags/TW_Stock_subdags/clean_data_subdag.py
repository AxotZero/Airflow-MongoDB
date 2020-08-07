import pymongo
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.mongo_hook import MongoHook
import logging


metadata_config = {
	'db': 'test',
	'collection': 'metadata'
}

cleandata_config = {
	'db': 'test',
	'collection': 'cleandata'
}

def _get_tmp_config(date_stamp):
	tmp_config = {
		'db': 'test',
		'collection': 'tmp'+date_stamp
	}
	return tmp_config


def _get_tmp_table(date_stamp, **context):
	tmp_config = context['task_instance'].xcom_pull(task_ids='get_tmp_config')

	# get today's data from metadata
	data = list(
		MongoHook(conn_id='mongo_default').find(
			query={'date_stamp': date_stamp},
			mongo_db=metadata_config['db'], 
			mongo_collection=metadata_config['collection']
		)
	)
	if len(data) == 0:
		raise ValueError("Didn't get any data")

	# empty tmp
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

	logging.info('insert into db:{}, collection: {}'.format(tmp_config['db'], tmp_config['collection']))


def _rename_columns(origin_names, new_names, **context):
	tmp_config = context['task_instance'].xcom_pull(task_ids='get_tmp_config')

	rename_dict = {}
	for i, (origin_name, new_name) in enumerate(zip(origin_names, new_names)):
		rename_dict[origin_name] = new_name
	
	query = {"$rename": rename_dict}

	# rename columns
	MongoHook(conn_id='mongo_default').update_many(
		filter_doc={},
		update_doc=query,
		mongo_db=tmp_config['db'],
		mongo_collection=tmp_config['collection']
	)


def _keep_columns(columns, **context):
	tmp_config = context['task_instance'].xcom_pull(task_ids='get_tmp_config')

	project_dict = {}
	for col in columns:
		project_dict[col] = 1

	query = [{"$project": project_dict}]

	MongoHook(conn_id='mongo_default').update_many(
		filter_doc={},
		update_doc=query,
		mongo_db=tmp_config['db'],
		mongo_collection=tmp_config['collection']
	)
	
  
def _convert_field_to_double(col, **context):
	tmp_config = context['task_instance'].xcom_pull(task_ids='get_tmp_config')

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
		MongoHook(conn_id='mongo_default').update_many(
			filter_doc={},
			update_doc=remove_comma_query,
			mongo_db=tmp_config['db'],
			mongo_collection=tmp_config['collection']
		)
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
	MongoHook(conn_id='mongo_default').update_many(
		filter_doc={},
		update_doc=convert_query,
		mongo_db=tmp_config['db'],
		mongo_collection=tmp_config['collection']
	)


def _compute_updown(**context):
	tmp_config = context['task_instance'].xcom_pull(task_ids='get_tmp_config')

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
	MongoHook(conn_id='mongo_default').update_many(
		filter_doc={},
		update_doc=query,
		mongo_db=tmp_config['db'],
		mongo_collection=tmp_config['collection']
	)


def _save_to_cleandata_table(**context):
	tmp_config = context['task_instance'].xcom_pull(task_ids='get_tmp_config')

	tmp = MongoHook(conn_id='mongo_default').get_collection(
		mongo_db=tmp_config['db'], 
		mongo_collection=tmp_config['collection']
	)

	# get data
	tmp_data = list(tmp.find())

	# insert to cleandata_table
	MongoHook(conn_id='mongo_default').insert_many(
		docs=tmp_data, 
		mongo_db=cleandata_config['db'], 
		mongo_collection=cleandata_config['collection']
	)

	# drop tmp collection
	tmp.drop()


def clean_data_subdag(parent_dag_name, child_dag_name, args):
	
	with DAG('{}.{}'.format(parent_dag_name, child_dag_name), default_args=args) as subdag:

		clean_data_start = DummyOperator(
			task_id='clean_data_start'
		)

		get_tmp_config = PythonOperator(
			task_id='get_tmp_config',
			python_callable=_get_tmp_config,
			op_args=['{{ ds_nodash }}']
		)

		get_tmp_table = PythonOperator(
			task_id='get_tmp_table',
			python_callable=_get_tmp_table,
			provide_context=True,
			op_args=['{{ ds_nodash }}']
		)

		rename_columns = PythonOperator(
			task_id='rename_columns',
			python_callable=_rename_columns,
			provide_context=True,
			op_kwargs={
				'origin_names':['證券代號', '最高價', '最低價', '開盤價', '收盤價', '成交筆數', '漲跌(+/-)', '漲跌價差'],
				'new_names': ['code', 'day_high', 'day_low', 'open_price', 'close_price', 'trans_num', 'up_down(+/-)', 'up_down(diff)']
			}
		)

		keep_use_columns = PythonOperator(
			task_id='keep_use_columns',
			python_callable=_keep_columns,
			provide_context=True,
			op_args=[['code', 'date_stamp', 'trans_num', 'day_high', 'day_low', 'open_price', 'close_price' , 'up_down(+/-)', 'up_down(diff)']]
		)

		convert_columns_to_double = []

		for col in ['day_high', 'day_low', 'open_price', 'close_price', 'trans_num', 'up_down(diff)']:
			task_id = 'convert_{}_to_double'.format(col).replace('(', '').replace(')', '')

			t = PythonOperator(
				task_id=task_id,
				python_callable=_convert_field_to_double,
				provide_context=True,
				op_args=[col]
			)
			convert_columns_to_double.append(t)

		compute_updown = PythonOperator(
			task_id='compute_updown',
			python_callable=_compute_updown,
			provide_context=True,
		)

		keep_final_columns = PythonOperator(
			task_id='keep_final_columns',
			python_callable=_keep_columns,
			provide_context=True,
			op_args=[['code', 'date_stamp', 'trans_num', 'day_high', 'day_low', 'open_price', 'close_price', 'up_down']]
		)

		save_to_cleandata_table = PythonOperator(
			task_id='save_to_cleandata_table',
			provide_context=True,
			python_callable=_save_to_cleandata_table,
		)

		clean_data_end = DummyOperator(
			task_id='clean_data_end'
		)


		clean_data_start >> get_tmp_config >> get_tmp_table >> rename_columns >> keep_use_columns >> convert_columns_to_double
		convert_columns_to_double >> compute_updown >> keep_final_columns >> save_to_cleandata_table >> clean_data_end
		
	return subdag
