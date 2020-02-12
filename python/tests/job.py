import time
from pyclash import clash
from pyclash.clash import JobConfigBuilder, Job

DEFAULT_JOB_CONFIG = (
    JobConfigBuilder()
    .project_id("oghub-pls-dev")
    .image("bash")
    .machine_type("n1-standard-1")
    .privileged(True)
    .build()
)

job = Job(job_config=DEFAULT_JOB_CONFIG, name_prefix="test")
job.run("echo hello")


def logging_callback(job):
    def log(status):
        print(f"logger{job.name}")

    return log


job.on_finish(logging_callback(job))

while True:
    time.sleep(1)
