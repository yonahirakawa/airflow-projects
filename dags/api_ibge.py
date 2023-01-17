from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator


def extract_data():
    import requests
    import json
    import pandas as pd

    UFs = [11,12,13,14,15,16,17,21,22,23,24,25,26,27,28,29,31,32,33,35,41,42,43,50,51,52,53]

    dfs = []

    for UF in UFs:
        url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{UF}/distritos"
        dados = requests.get(url).text
        distritos = json.loads(dados)
        dados_norm = pd.json_normalize(distritos)
        dfs.append(dados_norm)
        
    todos = pd.concat(dfs, ignore_index=True)

    return todos


def transf_data(ti):
    todos = ti.xcom_pull(task_ids = 'extract_data')

    todos.columns = todos.columns.str.replace('.', '_')
    todos.columns = todos.columns.str.replace('-', '_')

    return todos


def load_data(ti):
    todos = ti.xcom_pull(task_ids = 'transf_data')
    import sqlalchemy as db
    import mysql.connector
    from sqlalchemy import create_engine

    engine = create_engine("mysql+mysqlconnector://root:my-secret-pw@mysql:3306/airflow_db")
    todos.to_sql(name='dadosapi', con=engine, if_exists = 'replace', index=False)


default_arg = {'owner': 'airflow', 'start_date': '2023-01-16'}

dag = DAG('ibge-api-dag',
          default_args=default_arg,
          schedule_interval='0 0 * * *')


extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

transf_data = PythonOperator(
    task_id='transf_data',
    python_callable=transf_data,
    dag=dag
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

extract_data >> transf_data >> load_data

