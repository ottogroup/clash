from unittest.mock import patch, MagicMock
import yaml
import docker

from pyclash import clash

TEST_JOB_CONFIG = {
    "project_id": "yourproject-foobar",
    "image": "google/cloud-sdk",
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


class CloudSdkStub:
    def __init__(self):
        self.compute = MagicMock()
        self.compute.images.return_value.getFromFamily.return_value.execute.return_value = {
            "selfLink": "a_source_image"
        }

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


class TestContainerManifest:
    def test_manifest_contains_expected_values(self):
        manifest = clash.ContainerManifest("myvm", "myscript", TEST_JOB_CONFIG)

        rendered = manifest.to_yaml()

        loaded_manifest = yaml.load(rendered)
        assert loaded_manifest["spec"]["containers"][0]["name"] == "myvm"
        assert loaded_manifest["spec"]["containers"][0]["image"] == "google/cloud-sdk"
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


class TestJob:
    @patch("uuid.uuid1")
    def test_creates_job(self, mock_uuid_call):
        with CloudSdkStub() as gcloud:
            mock_uuid_call.return_value = 1234

            job = clash.create_job("", gcloud=gcloud)

            assert "clash-job-1234" == job.name

    @patch("uuid.uuid1")
    def test_running_a_job_runs_an_instance(self, mock_uuid_call):
        with CloudSdkStub() as gcloud:
            mock_uuid_call.return_value = 1234
            job = clash.create_job("", gcloud=gcloud)

            job.run()

            assert len(gcloud.instances) == 1
            assert gcloud.instances[0].body["name"] == "clash-job-1234"

    def test_job_actually_runs_script(self):
        with CloudSdkStub() as gcloud:
            job = clash.create_job("echo hello", gcloud=gcloud)

            job.run()

            assert b"hello\n" in gcloud.instances[0].logs()

    def test_job_shutdowns_machine_eventually(self):
        with CloudSdkStub() as gcloud:
            job = clash.create_job("echo hello", gcloud=gcloud)

            job.run()

            assert b"gcloud.compute.instances.delete" in gcloud.instances[0].logs()

    def test_job_runs_multiline_script(self):
        with CloudSdkStub() as gcloud:
            script = """
            echo 'hello'
            the_world_is_flat=true
            if [ "$the_world_is_flat" = true ] ; then
                echo 'world'
            fi
            """
            job = clash.create_job(script, gcloud=gcloud)

            job.run()

            assert b"hello\nworld\n" in gcloud.instances[0].logs()

    def test_wait_for_job(self):
        with CloudSdkStub() as gcloud:
            gcloud.detach = True  # Let us assume that the job runs asynchronously
            job = clash.create_job("sleep 5; echo hello", gcloud=gcloud)
            job.run()

            job.wait()

            assert b"hello\n" in gcloud.instances[0].logs()
