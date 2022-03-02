from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.utils import dates
import datetime
import logging
import argparse
#from past.builtins import unicode


#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG           = 'DAG-cs_to_bq'
project           = 'myung-341706'
owner             = 'NickMyung'
email             = ['nsotoc@uni.pe']
GBQ_CONNECTION_ID = 'bigquery_default'
GCS_PYTHON = "gs://prueba_myung/cs_to_bq.py"
DATA_SAMPLE_GCS_URL = "/cutoff_date=2021-09-05/part-00000-cb0983d6-8d1b-4965-a51d-eb94eb3050ad-c000.snappy.parquet"
#######################################################################################

default_args = {
    'owner': owner,                   # The owner of the task.
    'depends_on_past': False,         # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': datetime.datetime(2022, 2, 10),
    'email': email,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
    'project_id': project,  # Cloud Composer project ID.
}


with DAG(nameDAG,
         default_args = default_args,
         catchup = False,  # Ver caso catchup = True
         max_active_runs = 3,
         schedule_interval = "30 0 * * *") as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    # FUENTE: CRONTRAB: https://crontab.guru/
    #############################################################
    
    t_begin = DummyOperator(task_id="begin")
    
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id=f"create_imp_external_table",
        bucket='products_tktny',
        source_objects=[DATA_SAMPLE_GCS_URL], #pass a list
        destination_project_dataset_table=f"myung-341706.prueba.parquet_table",
        source_format='PARQUET', #use source_format instead of file_format
    )

    t_end = DummyOperator(task_id="end")

    #############################################################
    t_begin >> create_external_table >> t_end

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### SET PROJECT
#       gcloud config set project txd-capacityplanning-tst

### Ejecuta  fechas NO ejecutadas anteriormente (Tiene que tener schedule_interval)
#       gcloud composer environments run capacity-planning-composer-1 --location us-central1 backfill -- -s 20201101 -e 20201105 DAG-poc01-python-funct
#       -s: start date -> INTERVALO CERRADO
#       -e: end date   -> INTERVALO ABIERTO

### RE-ejecuta fechas anteriores
#       gcloud composer environments run capacity-planning-composer-1 --location us-central1 clear -- -c -s 20201106 -e 20201108 DAG-poc01-python-funct02

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
