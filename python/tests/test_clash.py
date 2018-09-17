from unittest.mock import patch, MagicMock
import pytest
import yaml
import docker

from pyclash import clash

TEST_JOB_CONFIG = {
    "project_id": "***REMOVED***",
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
        runner = manifest["spec"]["containers"][0]["env"][0]["value"]
        script = manifest["spec"]["containers"][0]["env"][1]["value"]
        image = manifest["spec"]["containers"][0]["image"]
        command = manifest["spec"]["containers"][0]["args"]

        client = docker.from_env()
        self.process = client.containers.run(
            image,
            command,
            environment={"SCRIPT": script, "CLASH_RUNNER": runner},
            stderr=True,
            detach=True,
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

class CloudSdkIntegrationStub:
    def __init__(self):

        self.compute = MagicMock()

        self.topics = []

        self.publisher = MagicMock()
        self.publisher.list_topics.return_value = self.topics
        self.publisher.topic_path.side_effect = lambda project, name: "{}/{}".format(project, name)
        self.publisher.create_topic.side_effect = lambda topic: self.topics.append(topic)

        self.subscriber = MagicMock()
        self.subscriber.pull.return_value.received_messages = []
        self.subscriber.topic_path.side_effect = lambda project, name: "{}/{}".format(project, name)
        self.subscriber.subscription_path.side_effect = lambda project, name: "{}/{}".format(project, name)

        self.instances = []
        self.detach = False

        def insert(project, zone, body):
            instance = InstanceStub(self, project, zone, body)
            self.instances.append(instance)
            return instance

        self.compute.instances.return_value.insert.side_effect = insert

    def __enter__(self):
        client = docker.from_env()
        with open('tests/Dockerfile', 'rb') as dockerfile:
            client.images.build(path='tests/', tag='test-cloudsdk:latest')
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

class CloudSdkStub:
    def __init__(self):
        self.compute = MagicMock()

        self.publisher = MagicMock()
        self.topics = []
        self.publisher.list_topics.return_value = self.topics
        self.publisher.topic_path.side_effect = lambda project, name: "{}/{}".format(project, name)
        self.publisher.create_topic.side_effect = lambda topic: self.topics.append(topic)

        self.subscriber = MagicMock()
        self.subscriber.topic_path.side_effect = lambda project, name: "{}/{}".format(project, name)
        self.subscriber.subscription_path.side_effect = lambda project, name: "{}/{}".format(project, name)

    def get_compute_client(self):
        return self.compute

    def get_publisher(self):
        return self.publisher

    def get_subscriber(self):
        return self.subscriber

class TestContainerManifest:
    def test_manifest_contains_expected_values(self):
        manifest = clash.ContainerManifest("myvm", "myscript", TEST_JOB_CONFIG)

        rendered = manifest.to_yaml()

        loaded_manifest = yaml.load(rendered)
        assert loaded_manifest["spec"]["containers"][0]["name"] == "myvm"
        assert loaded_manifest["spec"]["containers"][0]["image"] == "test-cloudsdk:latest"
        assert (
            loaded_manifest["spec"]["containers"][0]["env"][1]["value"] == "myscript\n"
        )

    def test_manifest_can_contain_multiline_script(self):
        script = """
        a
        b
        """
        manifest = clash.ContainerManifest("myvm", script, TEST_JOB_CONFIG)

        rendered = manifest.to_yaml()

        loaded_manifest = yaml.load(rendered)
        assert loaded_manifest["spec"]["containers"][0]["env"][1]["value"] == "\na\nb\n"


class TestMachineConfig:
    def setup(self):
        self.gcloud = CloudSdkStub()
        self.container_manifest = clash.ContainerManifest("_", "", TEST_JOB_CONFIG)

    def test_config_contains_vmname(self):
        manifest = clash.MachineConfig(
            self.gcloud.get_compute_client(),
            "myvm",
            self.container_manifest,
            TEST_JOB_CONFIG,
        )

        machine_config = manifest.to_dict()

        assert machine_config["name"] == "myvm"

    def test_config_contains_manifest(self):
        manifest = clash.MachineConfig(
            self.gcloud.get_compute_client(),
            "_",
            clash.ContainerManifest("myname", "_", TEST_JOB_CONFIG),
            TEST_JOB_CONFIG,
        )

        machine_config = manifest.to_dict()

        assert (
            machine_config["metadata"]["items"][0]["key"] == "gce-container-declaration"
        )
        manifest = yaml.load(machine_config["metadata"]["items"][0]["value"])
        assert manifest["spec"]["containers"][0]["name"] == "myname"

    def test_config_contains_machine_type(self):
        manifest = clash.MachineConfig(
            self.gcloud.get_compute_client(),
            "_",
            self.container_manifest,
            TEST_JOB_CONFIG,
        )

        machine_config = manifest.to_dict()

        assert machine_config[
            "machineType"
        ] == "https://www.googleapis.com/compute/beta/projects/{}/zones/{}/machineTypes/{}".format(
            TEST_JOB_CONFIG["project_id"],
            TEST_JOB_CONFIG["zone"],
            TEST_JOB_CONFIG["machine_type"],
        )

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

            assert b"gcloud.pubsub.topics.publish.clash-job-123.--message={'status': 0}" in gcloud.instances[0].logs()

    @patch("uuid.uuid1")
    def test_job_sends_pubpub_message_on_failure(self, mock_uuid_call):
        mock_uuid_call.return_value = 123
        with CloudSdkIntegrationStub() as gcloud:
            job = clash.Job(gcloud=gcloud, job_config=TEST_JOB_CONFIG)

            job.run("exit 1")

            assert b"gcloud.pubsub.topics.publish.clash-job-123.--message={'status': 1}" in gcloud.instances[0].logs()

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

class TestJob:
    def setup(self):
        self.gcloud = CloudSdkStub()

    @patch("uuid.uuid1")
    def test_creates_job(self, mock_uuid_call):
        mock_uuid_call.return_value = 1234

        job = clash.Job(gcloud=self.gcloud)

        assert "clash-job-1234" == job.name

    def test_running_a_job_runs_an_instance(self):
        job = clash.Job(gcloud=self.gcloud)

        job.run("")

        self.gcloud.get_compute_client().instances.return_value.insert.return_value.execute.assert_called()

    def test_running_a_job_creates_a_topic_path(self):
        job = clash.Job(gcloud=self.gcloud)

        job.run("")

        self.gcloud.get_publisher().topic_path.assert_called_with(TEST_JOB_CONFIG["project_id"], job.name)

    def test_running_a_job_creates_a_pubsub_topic(self):
        job = clash.Job(gcloud=self.gcloud)
        self.gcloud.get_publisher().topic_path.side_effect = lambda x,y: "mytopic"

        job.run("")

        self.gcloud.get_publisher().create_topic.assert_called_with("mytopic")

    def test_waiting_fails_if_there_is_not_a_running_job(self):
        job = clash.Job(gcloud=self.gcloud)
        with pytest.raises(ValueError) as e_info:
            job.wait()

    def test_waiting_succeeds_if_there_is_a_running_job_and_a_message(self):
        self.gcloud.get_subscriber().pull.return_value.received_messages = [MagicMock()]
        job = clash.Job(gcloud=self.gcloud)
        job.run("")

        job.wait() # throws no exception

    def test_waiting_for_a_job_creates_a_pubsub_subscription(self):
        self.gcloud.get_subscriber().pull.return_value.received_messages = [MagicMock()]
        self.gcloud.get_publisher().topic_path.side_effect = lambda x,y: "mytopic"
        self.gcloud.get_subscriber().topic_path.side_effect = lambda x,y: "mytopic"
        self.gcloud.get_subscriber().subscription_path.side_effect = lambda x,y: "mysubscription"
        job = clash.Job(gcloud=self.gcloud)
        job.run("")

        result = job.wait()

        self.gcloud.get_subscriber().create_subscription.assert_called_with("mysubscription", "mytopic")

    def test_waiting_pulls_message(self):
        self.gcloud.get_subscriber().pull.return_value.received_messages = [MagicMock(ack_id=42)]
        self.gcloud.get_subscriber().subscription_path.side_effect = lambda x,y: "mysubscription"
        job = clash.Job(gcloud=self.gcloud)
        job.run("")

        result = job.wait()

        self.gcloud.get_subscriber().pull.assert_called_with("mysubscription", max_messages=1, return_immediately=False)

    def test_waiting_acknowledges_messages(self):
        self.gcloud.get_subscriber().pull.return_value.received_messages = [MagicMock(ack_id=42)]
        self.gcloud.get_subscriber().subscription_path.side_effect = lambda x,y: "mysubscription"
        job = clash.Job(gcloud=self.gcloud)
        job.run("")

        result = job.wait()

        self.gcloud.get_subscriber().acknowledge.assert_called_with("mysubscription", 42)

    def test_waiting_deletes_subscription(self):
        self.gcloud.get_subscriber().pull.return_value.received_messages = [MagicMock(ack_id=42)]
        self.gcloud.get_subscriber().subscription_path.side_effect = lambda x,y: "mysubscription"
        job = clash.Job(gcloud=self.gcloud)
        job.run("")

        result = job.wait()

        self.gcloud.get_subscriber().delete_subscription.assert_called_with("mysubscription")

    def test_waiting_deletes_when_pulling_fails(self):
        self.gcloud.get_subscriber().pull.side_effect = ValueError()
        job = clash.Job(gcloud=self.gcloud)
        job.run("")

        with pytest.raises(ValueError) as e_info:
            job.wait()

        self.gcloud.get_subscriber().delete_subscription.assert_called()

    def test_waiting_deletes_when_ack_fails(self):
        self.gcloud.get_subscriber().pull.return_value.received_messages = [MagicMock(ack_id=42)]
        self.gcloud.get_subscriber().acknowledge.side_effect = ValueError()
        job = clash.Job(gcloud=self.gcloud)
        job.run("")

        with pytest.raises(ValueError) as e_info:
            job.wait()

        self.gcloud.get_subscriber().delete_subscription.assert_called()
