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
    'sidelines',
    default_args=default_args,
    schedule_interval = '0 2 * * 3')

start_task = EmptyOperator(
    task_id = 'start',
    dag = dag)
coach_sideline= BashOperator(
    task_id='curl.coach.sideline',
    bash_command= f'curl "http://{host_fastapi}:{port_fastapi}/coachsidelined?date={formatted_previous_day}"',  # Bash 명령>어
    dag=dag
)
player_sideline= BashOperator(
    task_id='curl.player.sideline',
    bash_command= f'curl "http://{host_fastapi}:{port_fastapi}/playersidelined?date={formatted_previous_day}"',  # Bash 명령어
    dag=dag
)

end_task = EmptyOperator(
    task_id = 'end',
    dag = dag)

start_task >> [coach_sideline,player_sideline]
[coach_sideline,player_sideline] >> end_task
