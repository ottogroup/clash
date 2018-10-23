## The ClashOperator
This folder contains the code for the [Airflow](https://airflow.apache.org) ClashOperator.
The ClashOperator makes it particularly easy to run long running tasks inside a dedicated GCE environment.
Please visit the [docs](https://airflow.apache.org/plugins.html) for how to install this operator.
The ClashOperator depends on the pyclash package included in this repository and also released on [PyPi](https://pypi.org/project/pyclash).

## Example
An example usage of the Clashoperator looks like this:

```
import copy
import os
from datetime import datetime

from pyclash import clash

from airflow import DAG
from airflow.operators import ClashOperator
from airflow.models import Variable

PROJECT_ID = ‘model-training’

def create_dag(dag_name, schedule_interval, start_date):
    """
    Creates a DAG for model training

    :param dag_name name of the DAG
    :param schedule_interval the schedule of this DAG
    :param start_name the start date of this DAGs execution
    """
    with DAG(
        dag_name,
        description="Trains a model and store it in a bucket",
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False,
    ) as dag:
        task_create_models = ClashOperator(
            cmd="./create_models.sh",
            env_vars={
                "GCP_PROJECT_ID": PROJECT_ID,
            },
            task_id="create_models",
        )

        task_test_model = ClashOperator(
            cmd="./test_model.sh",
            name_prefix="test_model"
            task_id="test_model"
        )

        task_depoy_model = ClashOperator(
            cmd="./depoy_model.sh",
            env_vars={
                "GCP_PROJECT_ID": PROJECT_ID,
            },
            name_prefix="depoy_model"
            task_id="depoy_model"
        )

        task_create_models >> task_test_model 
        task_test_model >> depoy_model

        globals()[dag_name] = dag

```