import sys
sys.path.append("./airflow/dags/")
sys.path.append("/usr/local/airflow/dags/staging/feature-store/")

# airflow operators
import airflow
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator

from datetime import datetime, timedelta

# Import dags dixa libraries
from scripts import preprocessing, feature_store

# Import config file
import feature_store_config as cfg

# define airflow DAG
# It is adviced to use a hardcoded start_date in the DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021,9,15),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id="feature_store_pipeline",
    default_args=default_args,
    description='First test for creating a feature store for conversations',
    schedule_interval=None
)

# set the tasks in the DAG
config = cfg.config

# dummy operator
init = DummyOperator(task_id="start", dag=dag)

## test for virtual operator

def virtualenv_fn():
    import sys
    sys_version = sys.version
    import seaborn as sns
    sns_version  = sns.__version__
    import langdetect
    ld_version = langdetect.__version__
    return(sys_version, sns_version)
    #import torch
    #print("torch version: ",torch.__version__)

virtualenv_task = PythonVirtualenvOperator(
        task_id="virtualenv_task",
        python_callable=virtualenv_fn,
        requirements=["langdetect>=1.0.8", "seaborn>=0.10.1"],
        system_site_packages=False,
        dag=dag,
    )
# preprocess the data
create_table = PythonOperator(
    task_id="create_table",
    dag=dag,
    provide_context=False,
    python_callable=preprocessing.executable,
    op_kwargs=config['preprocessing']
)

# create feature store
create_feature_store = PythonOperator(
    task_id="create_feature_store",
    dag=dag,
    provide_context=False,
    python_callable=feature_store.executable,
    op_kwargs=config['feature_store']
)

init.set_downstream(virtualenv_task)
virtualenv_task.set_downstream(create_table)
create_table.set_downstream(create_feature_store)