from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


dag = DAG(
    "covid_in_germany",
    description="My Airflow DAG",
    schedule_interval="15 9 * * *",
    start_date=datetime(2023, 7, 14),
    catchup=False,
)

# needs to be deleted after implementing ods
task_start = BashOperator(
    task_id='start',
    bash_command='date'
)

raw_districts_age_groups_task = BashOperator(
    task_id='raw_districts_age_groups',
    bash_command="spark-submit ~/CovidInGermanyDWH/raw/districts_age_groups/districts_age_groups.py",
    dag=dag
)

raw_districts_cases = BashOperator(
    task_id='raw_districts_cases',
    bash_command="spark-submit ~/CovidInGermanyDWH/raw/districts_cases/districts_cases.py",
    dag=dag
)

raw_districts_deaths = BashOperator(
    task_id='raw_districts_deaths',
    bash_command="spark-submit ~/CovidInGermanyDWH/raw/districts_deaths/districts_deaths.py",
    dag=dag
)

raw_districts_incidence = BashOperator(
    task_id='raw_districts_incidence',
    bash_command="spark-submit ~/CovidInGermanyDWH/raw/districts_incidence/districts_incidence.py",
    dag=dag
)

raw_districts_info = BashOperator(
    task_id='raw_districts_info',
    bash_command="spark-submit ~/CovidInGermanyDWH/raw/districts_info/districts_info.py",
    dag=dag
)

raw_districts_recovered = BashOperator(
    task_id='raw_districts_recovered',
    bash_command="spark-submit ~/CovidInGermanyDWH/raw/districts_recovered/districts_recovered.py",
    dag=dag
)

raw_states_hospitalization = BashOperator(
    task_id='raw_states_hospitalization',
    bash_command="spark-submit ~/CovidInGermanyDWH/raw/states_hospitalization/states_hospitalization.py",
    dag=dag
)

raw_states_vaccinations = BashOperator(
    task_id='raw_states_vaccinations',
    bash_command="spark-submit ~/CovidInGermanyDWH/raw/states_vaccinations/states_vaccinations.py",
    dag=dag
)

raw_vaccinations = BashOperator(
    task_id='raw_states_vaccinations',
    bash_command="spark-submit ~/CovidInGermanyDWH/raw/vaccinations/vaccinations.py",
    dag=dag
)

ods_districts_recovered = BashOperator(
    task_id='ods_districts_recovered',
    bash_command="spark-submit ~/CovidInGermanyDWH/ods/districts_recovered/districts_recovered.py",
    dag=dag
)

ods_states_vaccinations = BashOperator(
    task_id='ods_states_vaccinations',
    bash_command="spark-submit ~/CovidInGermanyDWH/ods/states_vaccinations/states_vaccinations.py",
    dag=dag
)


ods_states_hospitalization = BashOperator(
    task_id='ods_states_vaccinations',
    bash_command="spark-submit ~/CovidInGermanyDWH/ods/states_vaccinations/states_hospitalization.py",
    dag=dag
)

task_start >> [
    raw_districts_age_groups_task,
    raw_districts_cases,
    raw_districts_deaths,
    raw_districts_incidence,
    raw_districts_info,
    raw_districts_recovered,
    raw_states_hospitalization,
    raw_states_vaccinations,
    raw_vaccinations
]

raw_districts_recovered >> ods_districts_recovered
raw_states_vaccinations >> ods_states_vaccinations
raw_states_hospitalization >> ods_states_hospitalization
