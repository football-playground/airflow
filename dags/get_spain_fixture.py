from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

# Format the previous day's date as 'Y-mm-dd'
formatted_previous_day = "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

default_args = {
    'owner': 'hanul',
    'depends_on_past': True,
    'start_date':datetime(2023, 8, 10)}

dag = DAG(
    'spain_fixture_data',
    default_args=default_args,
    schedule_interval = '0 12 * * *')

start_task = EmptyOperator(
    task_id = 'start',
    dag = dag)
def spain_curl_and_push(date,**kwargs):
    import subprocess
    import json
     ti = kwargs['ti']

    # curl 명령어를 사용하여 데이터 가져오기
    curl_command = f"curl 'http://{host_fastapi}:{port_fastapi}/spain/fixtures-ids?date={date}'"
    result = subprocess.run(curl_command, shell=True, capture_output=True, text=True)
    result_str = result.stdout
    # XCom에 데이터 저장
    ti.xcom_push(key='return_value', value=result_str)

    return result_str
spain_curl_and_push_task = PythonOperator(
    task_id="spain.curl.and.push.task",
    python_callable=spain_curl_and_push,
    op_args=[formatted_previous_day],
    provide_context=True,
    dag=dag
)
def spain_pull_fixture_data(date,**kwargs):
    import subprocess
    import re
    ti = kwargs['ti']

    # XCom에서 데이터 불러오기
    pulled_value_str = ti.xcom_pull(task_ids='spain.curl.and.push.task', key='return_value')

    # 문자열을 파싱하여 리스트로 변환
    pulled_value = [int(x) for x in re.findall(r'\d+', pulled_value_str)]

    # 리스트를 문자열로 변환
    fixtures_ids_str = "-".join(map(str, pulled_value))
     command = f'curl "http://{host_fastapi}:{port_fastapi}/spain/fixtures?ids={fixtures_ids_str}&date={date}"'
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    result_str = str(result)
    # 불러온 데이터를 사용하거나 출력
    print(fixtures_ids_str)
    print("Pulled Value:", pulled_value)
    return result_str

# PythonOperator를 사용하여 위에서 정의한 함수 실행
spain_pull_fixture_data_task = PythonOperator(
    task_id='spain.pull.fixture.data.task',
    python_callable=spain_pull_fixture_data,
    op_args=[formatted_previous_day],
    provide_context=True,
    dag=dag
)
def spain_pull_injuries_data(date,**kwargs):
    import subprocess
    import re
    ti = kwargs['ti']

    # XCom에서 데이터 불러오기
    pulled_value_str = ti.xcom_pull(task_ids='spain.curl.and.push.task', key='return_value')
    # 문자열을 파싱하여 리스트로 변환
    pulled_value = [int(x) for x in re.findall(r'\d+', pulled_value_str)]

    # 리스트를 문자열로 변환
    for i in pulled_value:
        command = f'curl "http://{host_fastapi}:{port_fastapi}/spain/injuries?id={i}&date={date}"'
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(result)
spain_pull_injuries_data_task = PythonOperator(
    task_id='spain.pull.injuries.data.task',
    python_callable=spain_pull_injuries_data,
    op_args=[formatted_previous_day],
    provide_context=True,
    dag=dag
)
end_task = EmptyOperator(
    task_id = 'end',
    dag = dag)
start_task >> spain_curl_and_push_task
spain_curl_and_push_task >> [spain_pull_fixture_data_task,spain_pull_injuries_data_task]
[spain_pull_fixture_data_task,spain_pull_injuries_data_task] >> end_task
