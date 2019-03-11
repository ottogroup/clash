:rocket: CLASH is a tool for running Bash scripts on the Google Cloud Platform :rocket:

CLASH is an acronym for CLoud bASH. Its purpose is to provide an easy-to-use Python API / Airflow Operator for running Bash scripts on the *Compute Engine*.

Because CLASH uses the [Google Cloud SDK](https://github.com/googleapis/google-cloud-python), you first have to set up your local environment to access GCP. Please visit the [gcloud docs](https://cloud.google.com/sdk/gcloud/reference/auth/) for that matter.

## Example (Python API)

```
$ pip install pyclash
```

```from pyclash import clash
from pyclash.clash import JobConfigBuilder, Job

DEFAULT_JOB_CONFIG = (
    JobConfigBuilder()
    .project_id("your-project")
    .image("eu.gcr.io/your-project/image:latest")
    .machine_type("n1-standard-1")
    .privileged(True)
    .build()
)

job = Job(job_config=DEFAULT_JOB_CONFIG, name_prefix="test")
job.run("echo hello")

result = job.attach()
if result["status"] != 0:
    raise ValueError(
        "The command failed with status code {}".format(result["status"])
    )
```

## Example (Airflow / Cloud Composer)
```
COMPOSER_ENVIRONMENT="mycomposer-env" ./run.sh deploy-airflow-plugin
```
Note that the pyclash module must be available to Airflow.
```
from airflow.operators import ComputeEngineJobOperator

DEFAULT_JOB_CONFIG = (
    JobConfigBuilder()
    .project_id(PROJECT_ID)
    .image(f"eu.gcr.io/your-project/image:latest")
    .machine_type("n1-standard-32")
    .privileged(True)
    .preemptible(True)
    .build()
)


with DAG(
    "dag_id",
    start_date=datetime(2018, 10, 1),
    catchup=False,
) as dag:
    task_run_script = ComputeEngineJobOperator(
        cmd="echo hello",
        job_config=DEFAULT_JOB_CONFIG,
        name_prefix="prefix",
        task_id="run_script_task"
    )
```