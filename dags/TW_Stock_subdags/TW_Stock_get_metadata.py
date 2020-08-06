import requests
import pandas as pd
from airflow.models import DAG
from datetime import datetime, timedelta
from io import StringIO

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.mongo_hook import MongoHook

default_args = {
    'owner': 'AxotZero',
    'start_date': datetime(2020, 8, 3),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}



def requests_initialize():

    s = requests.session()
    s.keep_alive = False

    print('requests_initialize.')


def get_response(datestr):

    headers = {
        'User-Agent': 'Googlebot',
        'From': 'asdasdasd@gmail.com'
    }
    response = requests.post('https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALL',
                            headers=headers)

    print('get_response')
    return response


def response_to_df(datestr, **context):

    response = context['task_instance'].xcom_pull(task_ids='get_response')

    df = pd.read_csv(StringIO(response.text.replace("=", "")), 
        header=["證券代號" in l for l in response.text.split("\n")].index(True)-1)
    df['date_stamp'] = datestr

    print('response_to_df')
    return df


def df_to_metadata(**context):
    df = context['task_instance'].xcom_pull(task_ids='response_to_df')
    metadata = df.to_dict('records')

    print('df_to_metadata')
    return metadata


def insert_to_metadata_table(**context):
    metadata = context['task_instance'].xcom_pull(task_ids='df_to_metadata')
    MongoHook(conn_id='mongo_default').insert_many(docs=metadata, mongo_db='test', mongo_collection='metadata')

    print('insert_to_metadata_table')



# def get_metadata_dag(args):
with DAG(dag_id='TW_Stock_get_metadata', default_args=default_args) as sub_dag:

    get_metadata_start = DummyOperator(
        task_id='get_metadata_start'
    )

    requests_initialize = PythonOperator(
        task_id='requests_initialize',
        python_callable=requests_initialize,
    )

    get_response = PythonOperator(
        task_id='get_response',
        python_callable=get_response,
        op_args=['{{ ds_nodash }}']
    )

    response_to_df = PythonOperator(
        task_id='response_to_df',
        python_callable=response_to_df,
        provide_context=True,
        op_args=['{{ ds_nodash }}']
    )

    df_to_metadata = PythonOperator(
        task_id='df_to_metadata',
        python_callable=df_to_metadata,
        provide_context=True
    )

    insert_to_metadata_table = PythonOperator(
        task_id='insert_to_metadata_table',
        python_callable=insert_to_metadata_table,
        provide_context=True
    )

    get_metadata_end = DummyOperator(
        task_id='get_metadata_end'
    )

    get_metadata_start >> requests_initialize >> get_response >> response_to_df >> df_to_metadata >> insert_to_metadata_table >> get_metadata_end

