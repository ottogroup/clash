# -*- coding: future_fstrings -*-
from mock import patch, MagicMock
from collections import namedtuple
import pytest
import yaml
import docker

from pyclash import clash

TEST_JOB_CONFIG = {
    "project_id": "***REMOVED***",
    "image": "test-cloudsdk:latest",
    "zone": "europe-west1-b",
    "region": "europe-west1",
    "privileged": False,
    "subnetwork": "default-europe-west1",
    "machine_type": "n1-standard-1",
    "disk_image": {"project": "gce-uefi-images", "family": "cos-stable"},
    "scopes": [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/compute",
    ],
}


class InstanceStub:
    def __init__(self, gcloud, project, zone, body):
        self.gcloud = gcloud
        self.project = project
        self.zone = zone
        self.body = body

    def execute(self):
        manifest = yaml.load(self.body["metadata"]["items"][0]["value"])
        runner = manifest["write_files"][0]["content"]
        script = manifest["write_files"][1]["content"]
        env = manifest["write_files"][2]["content"]
        image = TEST_JOB_CONFIG["image"]
        command = [
            "bash",
            "-c",
            'echo "$SCRIPT" > /var/script.sh && echo "$CLASH_RUNNER" > /tmp/clash-runner.sh && bash /tmp/clash-runner.sh && cat /tmp/gcloud.log',
        ]

        client = docker.from_env()

        environment = {"SCRIPT": script, "CLASH_RUNNER": runner}
        for env_var in env.splitlines():
            var, value = env_var.split("=")
            environment[var] = value

        self.process = client.containers.run(
            image, command, environment=environment, stderr=True, detach=True
        )

        if not self.gcloud.detach:
            self.process.wait()

    def logs(self):
        if not self.process:
            return ""
        return self.process.logs()

    def remove(self):
        if self.process:
            self.process.remove(force=True)


Topic = namedtuple("Topic", "name")


class CloudSdkIntegrationStub:
    def __init__(self):

        self.compute = MagicMock()

        self.topics = []

        self.publisher = MagicMock()
        self.publisher.list_topics.return_value = self.topics
        self.publisher.topic_path.side_effect = lambda project, name: "{}/{}".format(
            project, name
        )
        self.publisher.create_topic.side_effect = lambda topic: self.topics.append(
            Topic(name=topic)
        )

        self.subscriber = MagicMock()
        self.subscriber.pull.return_value.received_messages = []
        self.subscriber.topic_path.side_effect = lambda project, name: "{}/{}".format(
            project, name
        )
        self.subscriber.subscription_path.side_effect = lambda project, name: "{}/{}".format(
            project, name
        )

        self.instances = []
        self.detach = False

        def insert(project, zone, body):
            instance = InstanceStub(self, project, zone, body)
            self.instances.append(instance)
            return instance

        self.compute.instances.return_value.insert.side_effect = insert

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for instance in self.instances:
            instance.remove()

    def get_compute_client(self):
        return self.compute

    def get_publisher(self):
        return self.publisher

    def get_subscriber(self):
        return self.subscriber

    def get_logging(self):
        return MagicMock()


class TestJobIntegration:
    def test_job_actually_runs_script(self):
        with CloudSdkIntegrationStub() as gcloud:
            job = clash.Job(gcloud=gcloud, job_config=TEST_JOB_CONFIG)

            job.run("echo hello")

            assert b"hello\n" in gcloud.instances[0].logs()

    def test_job_shutdowns_machine_eventually(self):
        with CloudSdkIntegrationStub() as gcloud:
            job = clash.Job(gcloud=gcloud, job_config=TEST_JOB_CONFIG)

            job.run("echo hello")

            assert b"gcloud.compute.instances.delete" in gcloud.instances[0].logs()

    @patch("uuid.uuid1")
    def test_job_sends_pubpub_message_on_success(self, mock_uuid_call):
        mock_uuid_call.return_value = 123
        with CloudSdkIntegrationStub() as gcloud:
            job = clash.Job(gcloud=gcloud, job_config=TEST_JOB_CONFIG)

            job.run("exit 0")

            assert (
                b'gcloud.pubsub.topics.publish.clash-job-123.--message={"status": 0}'
                in gcloud.instances[0].logs()
            )

    @patch("uuid.uuid1")
    def test_job_sends_pubpub_message_on_failure(self, mock_uuid_call):
        mock_uuid_call.return_value = 123
        with CloudSdkIntegrationStub() as gcloud:
            job = clash.Job(gcloud=gcloud, job_config=TEST_JOB_CONFIG)

            job.run("exit 1")

            assert (
                b'gcloud.pubsub.topics.publish.clash-job-123.--message={"status": 1}'
                in gcloud.instances[0].logs()
            )

    def test_job_runs_multiline_script(self):
        with CloudSdkIntegrationStub() as gcloud:
            script = """
            echo 'hello'
            the_world_is_flat=true
            if [ "$the_world_is_flat" = true ] ; then
                echo 'world'
            fi
            """
            job = clash.Job(gcloud=gcloud, job_config=TEST_JOB_CONFIG)

            job.run(script)

            assert b"hello\nworld\n" in gcloud.instances[0].logs()

    def test_job_runs_script_from_file(self):
        with CloudSdkIntegrationStub() as gcloud:
            job = clash.Job(gcloud=gcloud, job_config=TEST_JOB_CONFIG)

            job.run_file("tests/script.sh")

            assert b"hello\nworld\n" in gcloud.instances[0].logs()

    def test_job_uses_given_env_vars(self):
        with CloudSdkIntegrationStub() as gcloud:
            script = """
            echo "$MESSAGE"
            """
            job = clash.Job(gcloud=gcloud, job_config=TEST_JOB_CONFIG)

            job.run(script, env_vars={"MESSAGE": "foobar"})

            assert b"foobar\n" in gcloud.instances[0].logs()

    def test_passing_gcs_target_invokes_gsutil(self):
        with CloudSdkIntegrationStub() as gcloud:
            script = """
            touch /tmp/artifacts/foo
            touch /tmp/models/bar
            """
            job = clash.Job(gcloud=gcloud, job_config=TEST_JOB_CONFIG)

            job.run(
                script,
                gcs_target={
                    "/tmp/artifacts": "mybucket",
                    "/tmp/models": "modelsbucket",
                },
            )

            assert (
                b"gsutil.cp.-r./tmp/artifacts/foo.gs://mybucket"
                in gcloud.instances[0].logs()
            )
            assert (
                b"gsutil.cp.-r./tmp/models/bar.gs://modelsbucket"
                in gcloud.instances[0].logs()
            )

    def test_passing_gcs_target_without_artifacts_shows_error(self):
        with CloudSdkIntegrationStub() as gcloud:
            job = clash.Job(gcloud=gcloud, job_config=TEST_JOB_CONFIG)

            job.run("", gcs_target={"/tmp/artifacts": "mybucket"})

            assert b"No artifacts found in /tmp/artifacts" in gcloud.instances[0].logs()

    def test_passing_gcs_mount_invokes_gcsfuse(self):
        with CloudSdkIntegrationStub() as gcloud:
            job = clash.Job(gcloud=gcloud, job_config=TEST_JOB_CONFIG)

            job.run("", gcs_mounts={"mybucket": "/mnt/static"})

            assert (
                b"gcsfuse.--implicit-dirs.mybucket./mnt/static"
                in gcloud.instances[0].logs()
            )
