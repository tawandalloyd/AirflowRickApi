from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from airflow.operators.python import PythonOperator
from airflow.sensors.base import PokeReturnValue
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from include.rick_morty_project.tasks import _get_rick_morty_characters, _store_characters, _process_json_to_csv, _get_latest_file

@dag(
    start_date= datetime(2025,1,1),
    schedule='@daily',
    catchup=False,
    tags=['rick_morty'],
    on_success_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG rickandmorty has succedded',
        channel='api-monitoring'
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG rickandmorty has failed',
        channel='api-monitoring'
    )


)

def rick_morty():

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests

        api = BaseHook.get_connection('rick_morty_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url,headers=api.extra_dejson['headers'])
        data = response.json()
        condition = bool(data)
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    get_characters = PythonOperator(
        task_id='get_characters',
        python_callable=_get_rick_morty_characters,
        op_kwargs={'url': '{{ti.xcom_pull(task_ids="is_api_available")}}'}
    )

    store_characters = PythonOperator(
        task_id='store_characters',
        python_callable= _store_characters,
        op_kwargs={'characters': '{{ti.xcom_pull(task_ids="get_characters")}}'}
    )

    transform_characters = PythonOperator(
        task_id='transform_characters',
        python_callable= _process_json_to_csv
    )
    
    get_latest_file = PythonOperator(
        task_id='get_latest_file',
        python_callable= _get_latest_file
    )


    is_api_available() >> get_characters >> store_characters >> transform_characters >> get_latest_file

rick_morty()