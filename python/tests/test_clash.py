# -*- coding: future_fstrings -*-
from mock import patch, MagicMock
from collections import namedtuple
import pytest
import yaml
import docker
from io import BytesIO as StringIO
import sys
import contextlib
import os

from pyclash import clash

@contextlib.contextmanager
def redirect_stdout(target):
    original = sys.stdout
    sys.stdout = target
    yield
    sys.stdout = original

TEST_JOB_CONFIG = {
    "project_id": "yourproject-foobar",
    "image": "test-cloudsdk:latest",
    "zone": "europe-west1-b",
    "region": "europe-west1",
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


class CloudSdkStub:
    def __init__(self):
        self.compute = MagicMock()

        self.publisher = MagicMock()
        self.topics = []
        self.publisher.list_topics.return_value = self.topics
        self.publisher.topic_path.side_effect = lambda project, name: "{}/{}".format(
            project, name
        )
        self.publisher.create_topic.side_effect = lambda topic: self.topics.append(
            Topic(name=topic)
        )

        self.subscriber = MagicMock()
        self.subscriber.topic_path.side_effect = lambda project, name: "{}/{}".format(
            project, name
        )
        self.subscriber.subscription_path.side_effect = lambda project, name: "{}/{}".format(
            project, name
        )

    def get_compute_client(self):
        return self.compute

    def get_publisher(self):
        return self.publisher

    def get_subscriber(self):
        return self.subscriber

    def get_logging(self):
        return MagicMock()


class TestStackdriverLogsReader:
    def setup(self):
        self.job = MagicMock()
        self.logging_client = CloudSdkStub().get_logging()

    def test_list_entries_with_correct_filter(self):
        logs_reader = clash.StackdriverLogsReader(self.logging_client)
        self.job.name = "job-123"
        self.job.job_config = TEST_JOB_CONFIG
        logs_reader._now = MagicMock(return_value=100)
        logs_reader._delta = MagicMock(side_effect=lambda x: x)
        logs_reader._to_iso_format = MagicMock(side_effect=lambda x: 2 * x)
        EXPECTED_FILTER = f"""
            resource.type="global"
            logName="projects/{TEST_JOB_CONFIG["project_id"]}/logs/gcplogs-docker-driver"
            jsonPayload.instance.name="job-123"
            timestamp >= "160"
        """

        logs_reader.read_logs(self.job, 20)

        self.logging_client.list_entries.assert_called_with(filter_=EXPECTED_FILTER)

    def test_return_logs(self):
        logs_reader = clash.StackdriverLogsReader(self.logging_client)
        Entry = namedtuple("Entry", "payload")
        self.logging_client.list_entries.return_value = [
            Entry(payload={"data": "foo"}),
            Entry(payload={"data": "bar"}),
        ]

        logs = logs_reader.read_logs(self.job, 20)

        assert ["foo", "bar"] == logs


class TestMachineConfig:
    def setup(self):
        self.gcloud = CloudSdkStub()
        self.cloud_init = clash.CloudInitConfig("_", "", TEST_JOB_CONFIG)

    def test_config_contains_vmname(self):
        manifest = clash.MachineConfig(
            self.gcloud.get_compute_client(), "myvm", self.cloud_init, TEST_JOB_CONFIG
        )

        machine_config = manifest.to_dict()

        assert machine_config["name"] == "myvm"

    def test_config_contains_cloud_init_config(self):
        config = clash.MachineConfig(
            self.gcloud.get_compute_client(),
            "_",
            clash.CloudInitConfig("myname", "_", TEST_JOB_CONFIG),
            TEST_JOB_CONFIG,
        )

        machine_config = config.to_dict()

        assert machine_config["metadata"]["items"][0]["key"] == "user-data"
        cloud_init = yaml.load(machine_config["metadata"]["items"][0]["value"])
        assert cloud_init["users"][0]["name"] == "clash"

    def test_config_contains_machine_type(self):
        manifest = clash.MachineConfig(
            self.gcloud.get_compute_client(), "_", self.cloud_init, TEST_JOB_CONFIG
        )

        machine_config = manifest.to_dict()

        assert machine_config[
            "machineType"
        ] == "https://www.googleapis.com/compute/beta/projects/{}/zones/{}/machineTypes/{}".format(
            TEST_JOB_CONFIG["project_id"],
            TEST_JOB_CONFIG["zone"],
            TEST_JOB_CONFIG["machine_type"],
        )

class DisablePyTestCollectionMixin(object):
  __test__ = False

class TestJobIntegration(DisablePyTestCollectionMixin):
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
            job = clash.Job(gcloud=gcloud, job_config=TEST_JOB_CONFIG)

            job.run("", gcs_target={"/tmp/artifacts": "mybucket", "/tmp/models": "modelsbucket"})

            assert (
                    b'gsutil.cp.-r./tmp/artifacts/*.gs://mybucket'
                in gcloud.instances[0].logs()
            )
            assert (
                    b'gsutil.cp.-r./tmp/models/*.gs://modelsbucket'
                in gcloud.instances[0].logs()
            )

class TestJob:
    def setup(self):
        self.gcloud = CloudSdkStub()

    @patch("uuid.uuid1")
    def test_creates_job(self, mock_uuid_call):
        mock_uuid_call.return_value = 1234

        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)

        assert "clash-job-1234" == job.name

    def test_running_a_job_runs_an_instance(self):
        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)

        job.run("")

        self.gcloud.get_compute_client().instances.return_value.insert.return_value.execute.assert_called()

    def test_running_a_job_creates_a_topic_path(self):
        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)

        job.run("")

        self.gcloud.get_publisher().topic_path.assert_called_with(
            TEST_JOB_CONFIG["project_id"], job.name
        )

    def test_running_a_job_creates_a_pubsub_topic(self):
        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)
        self.gcloud.get_publisher().topic_path.side_effect = lambda x, y: "mytopic"

        job.run("")

        self.gcloud.get_publisher().create_topic.assert_called_with("mytopic")

    def test_attaching_fails_if_there_is_not_a_running_job(self):
        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)
        with pytest.raises(ValueError) as e_info:
            job.attach()

    def test_attaching_succeeds_if_there_is_a_running_job_and_a_message(self):
        message = MagicMock()
        message.message = MagicMock(data='{"status": 0}')
        self.gcloud.get_subscriber().pull.return_value.received_messages = [message]
        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)
        job.run("")

        job.attach()  # throws no exception

    def test_attaching_for_a_job_creates_a_pubsub_subscription(self):
        message = MagicMock()
        message.message = MagicMock(data='{"status": 0}')
        self.gcloud.get_subscriber().pull.return_value.received_messages = [message]
        self.gcloud.get_publisher().topic_path.side_effect = lambda x, y: "mytopic"
        self.gcloud.get_subscriber().topic_path.side_effect = lambda x, y: "mytopic"
        self.gcloud.get_subscriber().subscription_path.side_effect = (
            lambda x, y: "mysubscription"
        )
        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)
        job.run("")

        result = job.attach()

        self.gcloud.get_subscriber().create_subscription.assert_called_with(
            "mysubscription", "mytopic"
        )

    def test_attaching_pulls_message(self):
        message = MagicMock()
        message.message = MagicMock(data='{"status": 0}')
        self.gcloud.get_subscriber().pull.return_value.received_messages = [message]
        self.gcloud.get_subscriber().subscription_path.side_effect = (
            lambda x, y: "mysubscription"
        )
        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)
        job.run("")

        result = job.attach()

        self.gcloud.get_subscriber().pull.assert_called_with(
            "mysubscription", max_messages=1, return_immediately=False, timeout=30
        )

    def test_attaching_acknowledges_messages(self):
        message = MagicMock(ack_id=42)
        message.message = MagicMock(data='{"status": 0}')
        self.gcloud.get_subscriber().pull.return_value.received_messages = [message]
        self.gcloud.get_subscriber().subscription_path.side_effect = (
            lambda x, y: "mysubscription"
        )
        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)
        job.run("")

        result = job.attach()

        self.gcloud.get_subscriber().acknowledge.assert_called_with(
            "mysubscription", [42]
        )

    def test_attaching_deletes_subscription(self):
        message = MagicMock()
        message.message = MagicMock(data='{"status": 0}')
        self.gcloud.get_subscriber().pull.return_value.received_messages = [message]
        self.gcloud.get_subscriber().subscription_path.side_effect = (
            lambda x, y: "mysubscription"
        )
        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)
        job.run("")

        result = job.attach()

        self.gcloud.get_subscriber().delete_subscription.assert_called_with(
            "mysubscription"
        )

    def test_attaching_deletes_subscription_when_pulling_fails(self):
        self.gcloud.get_subscriber().pull.side_effect = ValueError()
        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)
        job.run("")

        with pytest.raises(ValueError) as e_info:
            job.attach()

        self.gcloud.get_subscriber().delete_subscription.assert_called()

    def test_attaching_deletes_subscription_when_ack_fails(self):
        message = MagicMock()
        message.message = MagicMock(data='{"status": 0}')
        self.gcloud.get_subscriber().pull.return_value.received_messages = [message]
        self.gcloud.get_subscriber().acknowledge.side_effect = ValueError()
        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)
        job.run("")

        with pytest.raises(ValueError) as e_info:
            job.attach()

        self.gcloud.get_subscriber().delete_subscription.assert_called()

    def test_attaching_prints_logs(self):
        message = MagicMock()
        message.message = MagicMock(data='{"status": 0}')
        self.gcloud.get_subscriber().pull.return_value.received_messages = [message]
        logs_reader = MagicMock()
        logs_reader.read_logs.return_value = ["foo", "bar"]
        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)
        job.run("")
        string_io = StringIO()

        with redirect_stdout(string_io):
            job.attach(logs_reader)

        out = string_io.getvalue()
        assert "foo\n" in out
        assert "bar\n" in out

    def test_attaching_returns_status_code(self):
        message = MagicMock()
        message.message = MagicMock(data='{"status": 127}')
        self.gcloud.get_subscriber().pull.return_value.received_messages = [message]
        job = clash.Job(TEST_JOB_CONFIG, gcloud=self.gcloud)
        job.run("")

        result = job.attach()

        assert result["status"] == 127

def test_load_config():
    os.environ["MACHINE_TYPE"] = "strongmachine"

    config = clash.load_config("tests/clash.yml")

    assert config["machine_type"] == "strongmachine"
