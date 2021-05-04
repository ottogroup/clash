
# Clash
> **WARNING**: This is a very young project. Although Clash is already used in production environments, the  Python API is still an *alpha release* and might change in the near future.

*Clash* is a simple Python library for running jobs on the [Google Compute Engine](https://cloud.google.com/compute/). Typical use cases are batch jobs which require very specific hardware configurations at runtime (e.g. multiple GPUs for model training). The library offers the following features:

* Automatic management of compute resources (allocation and deallocation)
* Definition of jobs via custom docker images
* Fine-grained cost-control by optionally using [preemptible VMs](https://cloud.google.com/preemptible-vms/)
* An easy-to-use Python API including operators for [Apache Airflow](https://airflow.apache.org/)

## Why Clash?

There are several ways for running jobs on the *Google Cloud Platform* (GCP) where each one comes with its pros and cons. For example, [Google's ML engine](https://cloud.google.com/ml-engine/) is now able to run dockerized jobs as well and should definitely be considered before using Clash, because of its excellent integration in the GCP ecosystem. In fact, the development of Clash started at a point in time when the options for running jobs on GCP were very limited.

On the other hand, Clash might still help to drastically reduce costs by offering the option to use preemptible VMs. In practice, one can usually make jobs robust towards sudden preemptions, for example, by continuously storing checkpoints. Clash automatically attempts to restart preempted machines and, thus, jobs can load their latest checkpoint and just continue where they have been interrupted. This way, we are successfully saving up to 80% of our compute costs.

## Requirements

* Python >= 3.7

Because Clash uses the [Google Cloud SDK](https://github.com/googleapis/google-cloud-python), you first have to set up your local environment to access GCP. Please visit the [gcloud docs](https://cloud.google.com/sdk/gcloud/reference/auth/) for that matter. In addition, Clash requires the following IAM roles to run correctly:

* roles/pubsub.editor # Clash uses [PubSub](https://cloud.google.com/pubsub/docs/) to communicate with its VMs
* roles/compute.instanceAdmin.* # Clash needs to be able to create custom VMs on the project

 If Clash should create VMs that are configured to run as a service account, one must also grant the roles/iam.serviceAccountUser role. 
 
 Note that *Clash VMs also need to be able to delete themselves and delete / publish to PubSub topics* in order to work correctly.

## Usage

```
$ pip install pyclash
```

```Python
from pyclash.clash import JobConfigBuilder, Job

JOB_CONFIG = (
    JobConfigBuilder()
    .project_id("my-gcp-project")
    .image("google/cloud-sdk:latest")
    .machine_type("n1-standard-1")
    .subnetwork("default")
    .preemptible(True)
    .build()
)

result = Job(job_config=JOB_CONFIG, name_prefix="myjob").run(
    ["echo", "hello world"], wait_for_result=True
)

if result["status"] != 0:
    raise ValueError(f"The command failed with status code {result['status']}")
```

By default, Clash runs VMs with the [Compute Engine default service account](https://cloud.google.com/compute/docs/access/service-accounts). One can also use Clash in the [Cloud Composer](https://cloud.google.com/composer/). To deploy the operators, run

```Bash
COMPOSER_ENVIRONMENT="mycomposer-env" \
COMPOSER_LOCATION="europe-west1" ./run.sh deploy-airflow-plugin
```

Note that the pyclash package must be available to the Composer in order to use the Clash operators (the  [official documentation](https://cloud.google.com/composer/docs/how-to/using/installing-python-dependencies) describes how to install custom packages from PyPi). The following example shows how to run jobs using Clash's *ComputeEngineJobOperator*:

```Python
from airflow.operators import ComputeEngineJobOperator
from airflow import DAG

JOB_CONFIG = (
    JobConfigBuilder()
    .project_id("my-gcp-project")
    .image("google/cloud-sdk:latest")
    .machine_type("n1-standard-32")
    .subnetwork("default")
    .build()
)

with DAG(
    "dag_id",
    start_date=datetime(2018, 10, 1),
    catchup=False,
) as dag:
    task_run_script = ComputeEngineJobOperator(
        cmd="echo hello",
        job_config=JOB_CONFIG,
        name_prefix="myjob",
        task_id="run_script_task"
    )
```

## Contributing

The best way to start working on Clash is to first install all the dependencies and run the tests:

```Bash
pipenv install --dev
./run.sh unit-test
GCP_PROJECT_ID='your-project-id' ./run.sh integration-test
```
