from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta

REGISTRY = "188897774766.dkr.ecr.eu-west-3.amazonaws.com"
DB_URL = "postgresql://postgres:OlistPassword2026!@olist-db.crc46smg6jwf.eu-west-3.rds.amazonaws.com:5432/postgres"

default_args = {
    'owner': 'shahine',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG(
    'olist_full_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    # 1. Capteur S3 : Attend un CSV dans le dossier 'raw'
    wait_for_csv = S3KeySensor(
        task_id='wait_for_raw_csv',
        bucket_name='olist-data-lake-shahine-belarbi',
        bucket_key='raw/*.csv',
        aws_conn_id='aws_default',
        timeout=600
    )

    # 2. Étape de Nettoyage 
    clean_task = KubernetesPodOperator(
        task_id='clean_olist_data',
        name='cleaner',
        namespace='airflow-v3',
        image=f"{REGISTRY}/olist-project-cleaner:latest",
        env_vars={'S3_BUCKET': 'olist-data-lake-shahine-belarbi'},
        get_logs=True
    )

    # 3. Étape de Transformation 
    transform_task = KubernetesPodOperator(
        task_id='transform_olist_data',
        name='transformer',
        namespace='airflow-v3',
        image=f"{REGISTRY}/olist-project-transformer:latest", 
        env_vars={'S3_BUCKET': 'olist-data-lake-shahine-belarbi'},
        get_logs=True
    )

    # 4. Étape Finale : Fusion et Export RDS 
    merge_to_rds = KubernetesPodOperator(
        task_id='merge_and_load_to_rds',
        name='merger',
        namespace='airflow-v3',
        image=f"{REGISTRY}/olist-project-merger:latest", 
        env_vars={
            'S3_BUCKET': 'olist-data-lake-shahine-belarbi',
            'DATABASE_URL': DB_URL
        },
        get_logs=True
    )

    wait_for_csv >> clean_task >> transform_task >> merge_to_rds