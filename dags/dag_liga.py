from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig


dataflow_script = "include/gcp/dataflow/main_local.py"
default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": datetime(year=2023, month=1, day=1),
    "depends_on_past": False,
}

@dag(
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dataflow','gcp'],
    max_active_runs=1,
    concurrency=1,
    description='DAG to copy data from local to GCS and then to BigQuery',
    default_view='graph',
)
def dag_liga():
    
    copy_folder= LocalFilesystemToGCSOperator(
        task_id='copy_folder',
        src='include/dataset/liga_spain/*.csv',
        dst='liga_spain/',
        bucket='jasm_liga_soccer',
        gcp_conn_id="gcp",
        mime_type="text/csv",
    )
    
    beam_raw = BeamRunPythonPipelineOperator(
        task_id='beam_raw',
        py_file=dataflow_script,
        py_requirements=['apache-beam'],
        py_interpreter='python3',
        py_system_site_packages=False,
        runner = 'DirectRunner',
        gcp_conn_id="gcp",

    )
    copy_raw_file = LocalFilesystemToGCSOperator(
        task_id='copy_raw_file',
        src='include/dataset/beam_output/output.csv',
        dst='raw/liga_spain.csv',
        bucket='jasm_liga_soccer',
        gcp_conn_id="gcp",
        mime_type="text/csv",
    )

    bq_to_raw = GCSToBigQueryOperator(
        task_id='bq_to_raw',
        bucket='jasm_liga_soccer',
        source_objects=['raw/liga_spain.csv'],
        destination_project_dataset_table='liga_spain.raw_liga_spain',
        autodetect=True,
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id="gcp",
    )
    @task.external_python(python='/usr/local/airflow/soda_env/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    
    transform = DbtTaskGroup(
        group_id = 'transform',
        project_config = DBT_PROJECT_CONFIG,
        profile_config = DBT_CONFIG,
        render_config = RenderConfig(
            load_method = LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )
    
    @task.external_python(python='/usr/local/airflow/soda_env/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    report = DbtTaskGroup(
        group_id='report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )

    @task.external_python(python='/usr/local/airflow/soda_env/bin/python')
    def check_report(scan_name='check_transform', checks_subpath='report'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    chain(
        copy_folder,
        beam_raw,
        copy_raw_file,
        bq_to_raw,
        check_load(),
        transform,
        check_transform(),
        report,
        check_report(),
    )
dag_liga()