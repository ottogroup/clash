from pyclash.clash import JobConfigBuilder, Job
import os

JOB_CONFIG = (
    JobConfigBuilder()
    .project_id(os.environ['GCP_PROJECT_ID'])
    .image("google/cloud-sdk:latest")
    .machine_type("n1-standard-1")
    .subnetwork("default")
    .preemptible(True)
    .build()
)

result = Job(job_config=JOB_CONFIG, name_prefix="myjob").run(
    "echo 'hello world'", wait_for_result=True
)

if result["status"] != 0:
    raise ValueError(f"The command failed with status code {result['status']}")
