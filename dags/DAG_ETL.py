from airflow import DAG
import datetime
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage


CLUSTER_NAME = 'clustertktn'
REGION='us-central1'
PROJECT_ID='myung-341706'
PYSPARK_URI='gs://code_tkth/main.py'
PYSPARK_URI_ZIP='gs://code_tkth/files.zip'
DATE = "2021-09-05"
BUCKET = "products_tktny"

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


default_args = {
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 2, 24),
    'retries': 0,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
    'project_id': PROJECT_ID,  # Cloud Composer project ID.   
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI,
                    "python_file_uris": [PYSPARK_URI_ZIP],
                    "args":[DATE]},
}

def python_func():

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(BUCKET, prefix="cutoff_date=" + DATE + "/part")

    print("Blob:")
    print(blobs)
    for blob in blobs:
        blb = blob.name
        print(blb) 
        break

    DATA_SAMPLE_GCS_URL = "/" + str(blb)
    print("DATA_SAMPLE_GCS_URL: ")
    print(DATA_SAMPLE_GCS_URL)

    return DATA_SAMPLE_GCS_URL


with DAG(
    'DAG_ETL',
    default_args=default_args,
    description='A simple DAG to create a Dataproc workflow',
    schedule_interval="30 0 * * *",
    catchup = False
) as dag:

    t_begin = DummyOperator(task_id="begin")

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    
    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        location=REGION, 
        project_id=PROJECT_ID
    )

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id=f"create_imp_external_table",
        bucket=BUCKET,
        source_objects=[python_func()], #pass a list
        destination_project_dataset_table=f"myung-341706.prueba." + BUCKET,
        source_format='PARQUET', #use source_format instead of file_format
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION
    )
    t_end = DummyOperator(task_id="end")

    t_begin >> create_cluster >> submit_job >> create_external_table >> delete_cluster >> t_end