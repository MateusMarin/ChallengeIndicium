import json, yaml, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from pathlib import Path
from airflow.operators.python import PythonOperator
import psycopg2
from copy import deepcopy

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SOURCE_NORTHWIND_TABLES = [
    "categories",
    "customer_customer_demo",
    "customer_demographics",
    "customers",
    "employees",
    "employee_territories",
    "orders",
    "products",
    "region",
    "shippers",
    "suppliers",
    "territories",
    "us_states"
]

EXTRA_CONFIGS = {
    "categories" : {
        "in": { "column_options": {"picture": {"value_type": "string"}}}
    },
    "employees" : {
        "in": { "column_options": {"photo": {"value_type": "string"}}}
    }
}

SOURCE_CSV_FILES = [
    "order_details"
]

EMBULK_FILE_TEMPLATE = Path("/data/{date}.{table}_")
PWD = Path()

def dict_of_dicts_merge(x, y):
    z = {}
    overlapping_keys = x.keys() & y.keys()
    for key in overlapping_keys:
        if isinstance(x[key],dict):
            z[key] = dict_of_dicts_merge(x[key], y[key])
        elif isinstance(x[key],list):
            z[key] = y[key]
    for key in x.keys() - overlapping_keys:
        z[key] = deepcopy(x[key])
    for key in y.keys() - overlapping_keys:
        z[key] = deepcopy(y[key])
    return z

def get_column_metadata(table_name):
    conn = psycopg2.connect(
        dbname="northwind",
        user="northwind_user",
        password="thewindisblowing",
        host="db",
        port="5432"
    )
    cur = conn.cursor()
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name}';
    """
    cur.execute(query)
    columns = cur.fetchall()
    cur.close()
    conn.close()
    return columns

def log_column_metadata(table_name, **kwargs):
    columns = get_column_metadata(table_name)
    column_options = [{"name": col[0], "type": "string"} for col in columns]
    kwargs['ti'].xcom_push(key=f'{table_name}_column_options', value=column_options)

def prepare_embulk_config(table_name, date, _from, _to, **kwargs):
    ti = kwargs['ti']
    column_options = ti.xcom_pull(key=f'{table_name}_column_options')
    extra_config = {"in": {"parser": {"columns": column_options}}}
    return embulk_config(table_name, date, _from, _to, extra_config=extra_config)

def embulk_config(table, date, _from, _to, extra_config={}):
    file_template = str(EMBULK_FILE_TEMPLATE).format(date=date, table=table)
    resources = dict(
        northwind = {
            "type": "postgresql",
            "host": "db",
            "database": "northwind",
            "port": 5432,
            "user": "northwind_user",
            "password": "thewindisblowing",
            "select": "*",
            "table": table
        },
        local_dest = {
            "type": "file",
            "formatter": {
                "type": "jsonl",
                "encoding": "UTF-8"
            },
            "encoder": {
                "type": "gzip"
            },
            "path_prefix": file_template,
            "file_ext": "json.gz"
        },
        local_source = {
            "type": "file",
            "parser": {
                "type": "jsonl",
                "charset": "UTF-8"
            },
            "decoders": [
                {"type": "gzip"}
            ],
            "path_prefix": file_template,
            "file_ext": "json.gz"
        },
        dw = {
            "type": "postgresql",
            "host": "destination_database",
            "database": "indicium",
            "port": 5432,
            "user": "admin",
            "password": "password",
            "table": table,
            "mode" : "insert_direct"
        }
    )
    valid_resources = set(k.replace(f"_{i}", '') for i in ['dest', 'source'] for k in resources.keys())
    assert _from in valid_resources, f"Source argument _from accepts {valid_resources} only, not {_from}"
    assert _to in valid_resources, f"Destination argument _to accepts {valid_resources} only, not {_to}"
    _from = f"{_from}_source" if _from == "local" else _from
    _to = f"{_to}_dest" if _to == "local" else _to
    config = {
        "in": dict_of_dicts_merge(resources[_from], extra_config.get("in",{})),
        "out": dict_of_dicts_merge(resources[_to], extra_config.get("out",{}))
    }
    return json.dumps(config)

with DAG(
    'indicium_challenge',
    default_args=default_args,
    description='Extract data from tables in SQL file using Embulk',
    schedule_interval="0 0 * * *",
    catchup=False
) as dag:

    for table in SOURCE_NORTHWIND_TABLES:
        extract_metadata = PythonOperator(
            task_id=f'get_metadata_{table}',
            python_callable=log_column_metadata,
            op_args=[table]
        )
        northwind_to_local = DockerOperator(
            task_id=f'northwind_{table}_to_local',
            image="indiciumchallenge/embulk",
            network_mode="indiciumchallenge_default",
            environment={
                "EMBULK_CONFIG": embulk_config(
                    table, "{{ds}}", _from="northwind", _to="local_dest",
                    extra_config=EXTRA_CONFIGS.get(table,{})
                )
            },
            command=[
                "run",
                "config.yml"
            ],
            mounts = [
                Mount(
                    source="embulk",
                    target=str(EMBULK_FILE_TEMPLATE.parent),
                    type="volume"
                )
            ]
        )
        local_to_dw = DockerOperator(
            task_id=f'local_{table}_to_dw',
            image="indiciumchallenge/embulk",
            network_mode="indiciumchallenge_default",
            environment={
                "EMBULK_CONFIG": "{{ task_instance.xcom_pull(task_ids='" f"prepare_embulk_config_{table}')" "}}"
            },
            command=[
                "run",
                "config.yml"
            ],
            mounts=[
                Mount(
                    source="embulk",
                    target=str(EMBULK_FILE_TEMPLATE.parent),
                    type="volume"
                )
            ],
        )

        prepare_embulk_config_task = PythonOperator(
            task_id=f'prepare_embulk_config_{table}',
            python_callable=prepare_embulk_config,
            op_args=[table, "", "local_source", "dw"],
            provide_context=True
        )

        extract_metadata >> northwind_to_local >> prepare_embulk_config_task >> local_to_dw

    # Task to execute Meltano for CSV extraction
    for file in SOURCE_CSV_FILES:

        csv_to_local = DockerOperator(
            
            task_id=f'csv_{file}_to_local',
            image="indiciumchallenge/meltano",
            network_mode="indiciumchallenge_default",
            environment={
                "TARGET_JSONL_DESTINATION_PATH": "/output",
                "TARGET_JSONL_CUSTOM_NAME": "{{ds}}."f"{file}"
            },
            command=[
                "el",
                "tap-csv",
                "target-jsonl"
            ],  
            mounts=[
                Mount(
                    source=f"{os.getenv('HOST_DATA_PATH')}/data",
                    target="/data",
                    type="bind"
                ),
                Mount(
                    source="embulk",
                    target="/output",
                    type="volume"
                )
            ]
        )
        local_to_dw = DockerOperator(
            task_id=f'local_{file}_to_dw',
            image="indiciumchallenge/embulk",
            network_mode="indiciumchallenge_default",
            environment=dict(
                EMBULK_CONFIG=embulk_config(file, "{{ds}}", _from="local_source", _to="dw", extra_config= {"in": {"decoders": [], "file_ext": "jsonl", "parser": {"columns": [{"name": col, "type": "string"} for col in "order_id,product_id,unit_price,quantity,discount".split(",")]}}})
            ),
            command=[
                 "run",
                 "config.yml"
            ],
            mounts=[
                Mount(
                    source="embulk",
                    target=str(EMBULK_FILE_TEMPLATE.parent),
                    type="volume"
                )
            ]
        )
        csv_to_local >> local_to_dw