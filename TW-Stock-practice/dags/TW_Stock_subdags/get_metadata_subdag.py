import requests
import pandas as pd
from airflow.models import DAG
from datetime import datetime, timedelta
from io import StringIO

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.mongo_hook import MongoHook


def _requests_initialize():
    s = requests.session()
    s.keep_alive = False


def _get_response(datestr):

    headers = {
        'User-Agent': 'Googlebot',
        'From': 'asdasdasd@gmail.com'
    }
    response = requests.post('https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALL',
                            headers=headers)
    return response


def _response_to_df(datestr, **context):

    response = context['task_instance'].xcom_pull(task_ids='get_response')

    df = pd.read_csv(StringIO(response.text.replace("=", "")), 
        header=["證券代號" in l for l in response.text.split("\n")].index(True)-1)
    df['date_stamp'] = datestr

    return df


def _df_to_metadata(**context):
    df = context['task_instance'].xcom_pull(task_ids='response_to_df')
    metadata = df.to_dict('records')

    return metadata


def _insert_to_metadata_table(**context):
    metadata = context['task_instance'].xcom_pull(task_ids='df_to_metadata')
    MongoHook(conn_id='mongo_default').insert_many(docs=metadata, mongo_db='test', mongo_collection='metadata')



def get_metadata_subdag(parent_dag_name, child_dag_name, args):

    with DAG('{}.{}'.format(parent_dag_name, child_dag_name), default_args=args) as subdag:

        get_metadata_start = DummyOperator(
            task_id='get_metadata_start'
        )

        requests_initialize = PythonOperator(
            task_id='requests_initialize',
            python_callable=_requests_initialize,
        )

        get_response = PythonOperator(
            task_id='get_response',
            python_callable=_get_response,
            op_args=['{{ ds_nodash }}']
        )

        response_to_df = PythonOperator(
            task_id='response_to_df',
            python_callable=_response_to_df,
            provide_context=True,
            op_args=['{{ ds_nodash }}']
        )

        df_to_metadata = PythonOperator(
            task_id='df_to_metadata',
            python_callable=_df_to_metadata,
            provide_context=True
        )

        insert_to_metadata_table = PythonOperator(
            task_id='insert_to_metadata_table',
            python_callable=_insert_to_metadata_table,
            provide_context=True
        )

        get_metadata_end = DummyOperator(
            task_id='get_metadata_end'
        )

        get_metadata_start >> requests_initialize >> get_response >> response_to_df >> df_to_metadata >> insert_to_metadata_table >> get_metadata_end

    return subdag
