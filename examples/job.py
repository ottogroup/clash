from pyclash import clash
from pyclash.clash import JobConfigBuilder

DEFAULT_JOB_CONFIG = (
    JobConfigBuilder()
    .project_id("yourproject-foobar-dev")
    .image("google/cloud-sdk")
    .machine_type("n1-standard-1")
    .privileged(True)
    .build()
)

job = clash.Job(job_config=DEFAULT_JOB_CONFIG, name_prefix="test")
job.run("echo hello")

result = job.attach()
if result["status"] != 0:
    raise ValueError(
        "The command failed with status code {}".format(result["status"])
    )
