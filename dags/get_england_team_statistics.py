from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

formatted_previous_day = "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

default_args = {
    'owner': 'hanul',
    'depends_on_past': True,
    'start_date':datetime(2024,2,22)}

dag = DAG(
    'england_team_statistics',
    default_args=default_args,
    schedule_interval = '0 2 * * 3')

start_task = EmptyOperator(
    task_id = 'start',
    dag = dag)
england_team_stat= BashOperator(
    task_id='curl.england.team.stat',
    bash_command= f'curl "http://{host_fastapi}:{port_fastapi}/england/teamstat?date={formatted_previous_day}"',  # Bash 명>령어
    dag=dag
)
england_player_stat= BashOperator(
    task_id='curl.england.player.stat',
    bash_command= f'curl "http://{host_fastapi}:{port_fastapi}/england/playerstat?date={formatted_previous_day}"',  # Bash >명령어
    dag=dag
)
end_task = EmptyOperator(
    task_id = 'end',
    dag = dag)

start_task >> [england_team_stat,england_player_stat]
[england_team_stat,england_player_stat] >> end_task
