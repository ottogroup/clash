from pyclash import clash
from pyclash.clash import JobConfigBuilder

DEFAULT_JOB_CONFIG = (
    JobConfigBuilder()
    .project_id("***REMOVED***")
    .image("eu.gcr.io/***REMOVED***/***REMOVED***:latest")
    .machine_type("n1-standard-1")
    .privileged(True)
    .build()
)

gcs_target = {
    "/tmp/artifacts": "test-bucket-4532"
}

gcs_mounts = {
    "test-bucket-4532": "/home/app/mnt/bucket"
}

job = clash.Job(job_config=DEFAULT_JOB_CONFIG, name_prefix="test")
job.run("echo hello && touch /tmp/artifacts/woo.txt && ls -l /home/app/mnt/bucket", gcs_target=gcs_target, gcs_mounts=gcs_mounts)

result = job.attach()
if result["status"] != 0:
    raise ValueError(
        "The command failed with status code {}".format(result["status"])
    )
