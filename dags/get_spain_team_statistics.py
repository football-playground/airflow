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
    'start_date':datetime(2024,2,22)}

dag = DAG(
    'spain_team_statistics',
    default_args=default_args,
    schedule_interval = '0 2 * * 3')

start_task = EmptyOperator(
    task_id = 'start',
    dag = dag)
spain_team_stat= BashOperator(
    task_id='curl.spain.team.stat',
    bash_command= f'curl "http://{host_fastapi}:{port_fastapi}/spain/teamstat?date={formatted_previous_day}"',  # Bash 명령>어
    dag=dag
)
spain_player_stat= BashOperator(
    task_id='curl.spain.player.stat',
    bash_command= f'curl "http://{host_fastapi}:{port_fastapi}/spain/playerstat?date={formatted_previous_day}"',  # Bash 명>령어
    dag=dag
)

end_task = EmptyOperator(task_id = 'end',
    dag = dag)

start_task >> [spain_team_stat,spain_player_stat]
[spain_team_stat,spain_player_stat] >> end_task
