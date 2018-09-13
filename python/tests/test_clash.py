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
        self.compute.images.getFromFamily.return_value.execute.return_value = {
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
    def test_rendering_contains_arguments(self):
        manifest = clash.ContainerManifest("myvm", "myscript", "myimage")

        rendered = manifest.render()

        loaded_manifest = yaml.load(rendered)
        assert loaded_manifest['spec']['containers'][0]['name'] == "myvm"
        assert loaded_manifest['spec']['containers'][0]['image'] == "myimage"
        assert loaded_manifest['spec']['containers'][0]['env'][1]['value'] == "myscript\n"


class TestJob:
    def setup(self):
        self.gcloud = CloudSdkStub()

    @patch("uuid.uuid1")
    def test_creates_job(self, mock_uuid_call):
        mock_uuid_call.return_value = 1234

        job = clash.create_job("")

        assert "clash-job-1234" == job.name

    def test_running_a_job_creates_a_running_instance(self):
        job = clash.create_job("")

        job.run(gcloud=self.gcloud)

        assert len(self.gcloud.instances) == 1
        assert self.gcloud.instances[0].running
