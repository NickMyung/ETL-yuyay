from airflow import DAG
import datetime
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.operators.dummy_operator import DummyOperator



CLUSTER_NAME = 'cluster-ea7ar'
REGION='us-central1'
PROJECT_ID='myung-341706'
PYSPARK_URI='gs://prueba_myung/cs_to_bq.py'

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    }
}


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

default_args = {
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 2, 24),
    'retries': 0,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
    'project_id': PROJECT_ID,  # Cloud Composer project ID.   
}

with DAG(
    'dataproc-demo',
    default_args=default_args,
    description='A simple DAG to create a Dataproc workflow',
    schedule_interval="30 0 * * *",
    catchup = False
) as dag:

#    create_cluster = DataprocCreateClusterOperator(
#        task_id="create_cluster",
#        project_id=PROJECT_ID,
#        cluster_config=CLUSTER_CONFIG,
#        region=REGION,
#        cluster_name=CLUSTER_NAME,
#    )
    t_begin = DummyOperator(task_id="begin")

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        location=REGION, 
        project_id=PROJECT_ID
    )

#    delete_cluster = DataprocDeleteClusterOperator(
#        task_id="delete_cluster", 
#        project_id=PROJECT_ID, 
#        cluster_name=CLUSTER_NAME, 
#        region=REGION
#    )
    t_end = DummyOperator(task_id="end")

    t_begin >> submit_job >> t_end