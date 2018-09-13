from unittest.mock import patch, MagicMock
import yaml

from pyclash import clash


class InstanceStub:
    def __init__(self, project, zone, body):
        self.project = project
        self.zone = zone
        self.body = body
        self.running = False

    def execute(self):
        self.running = True


class CloudSdkStub:
    def __init__(self):
        self.compute = MagicMock()
        self.compute.images.return_value.getFromFamily.return_value.execute.return_value = {
            "selfLink": "a_source_image"
        }

        self.instances = []

        def insert(project, zone, body):
            instance = InstanceStub(project, zone, body)
            self.instances.append(instance)
            return instance

        self.compute.instances.return_value.insert.side_effect = insert

    def get_compute_client(self):
        return self.compute


class TestContainerManifest:
    def test_manifest_contains_expected_values(self):
        manifest = clash.ContainerManifest("myvm", "myscript", "myimage")

        rendered = manifest.to_yaml()

        loaded_manifest = yaml.load(rendered)
        assert loaded_manifest["spec"]["containers"][0]["name"] == "myvm"
        assert loaded_manifest["spec"]["containers"][0]["image"] == "myimage"
        assert (
            loaded_manifest["spec"]["containers"][0]["env"][1]["value"] == "myscript\n"
        )


class TestMachineConfig:
    def setup(self):
        self.gcloud = CloudSdkStub()

    def test_config_contains_vmname(self):
        manifest = clash.MachineConfig(
            self.gcloud.get_compute_client(),
            "myvm",
            "_",
            "_",
        )

        machine_config = manifest.to_dict()

        assert machine_config["name"] == "myvm"

    def test_config_contains_manifest(self):
        manifest = clash.MachineConfig(
            self.gcloud.get_compute_client(),
            "_",
            "mycontainermanifest",
            "_",
        )

        machine_config = manifest.to_dict()

        assert (
            machine_config["metadata"]["items"][0]["key"] == "gce-container-declaration"
        )
        assert machine_config["metadata"]["items"][0]["value"] == "mycontainermanifest"

    def test_config_contains_machine_type(self):
        manifest = clash.MachineConfig(
            self.gcloud.get_compute_client(),
            "_",
            "_",
            "n1-standard-32",
        )

        machine_config = manifest.to_dict()

        assert machine_config[
            "machineType"
        ] == "https://www.googleapis.com/compute/beta/projects/{}/{}".format(
            clash.config["project_id"], "n1-standard-32"
        )


class TestJob:
    def setup(self):
        self.gcloud = CloudSdkStub()

    @patch("uuid.uuid1")
    def test_creates_job(self, mock_uuid_call):
        mock_uuid_call.return_value = 1234

        job = clash.create_job("", gcloud=self.gcloud)

        assert "clash-job-1234" == job.name

    @patch("uuid.uuid1")
    def test_running_a_job_creates_a_running_instance(self, mock_uuid_call):
        mock_uuid_call.return_value = 1234
        job = clash.create_job("", gcloud=self.gcloud)

        job.run()

        assert len(self.gcloud.instances) == 1
        assert self.gcloud.instances[0].body["name"] == "clash-job-1234"
        assert self.gcloud.instances[0].running
