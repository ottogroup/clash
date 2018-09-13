import argparse
import uuid

import jinja2
import json
import googleapiclient.discovery

config = {
    "project_id": "yourproject-foobar",
    "default_image": "google/cloud-sdk",
    "zone": "europe-west1-b",
    "region": "europe-west1",
    "default_machine_type": "n1-standard-1",
    "disk_image": {"project": "gce-uefi-images", "family": "cos-stable"},
    "default_scopes": [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/compute",
        "https://www.googleapis.com/auth/devstorage.read_write",
        "https://www.googleapis.com/auth/logging.write",
        "https://www.googleapis.com/auth/monitoring",
        "https://www.googleapis.com/auth/pubsub",
    ],
}


class CloudSdk:
    def __init__(self):
        pass

    def get_compute_client(self):
        return googleapiclient.discovery.build("compute", "v1")


class ContainerManifest:
    def __init__(self, vm_name, script, image):
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(searchpath="../templates")
        )
        self.vm_name = vm_name
        self.script = script
        self.image = image

    def to_yaml(self):
        clash_runner_script = self.template_env.get_template(
            "clash_runner.sh.j2"
        ).render(vm_name=self.vm_name, zone=config["zone"])

        return self.template_env.get_template("container_manifest.yaml.j2").render(
            vm_name=self.vm_name,
            image=self.image,
            clash_runner_script=clash_runner_script,
            script=self.script,
        )


class MachineConfig:
    def __init__(self, compute, vm_name, container_manifest, machine_type):
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(searchpath="../templates")
        )
        self.compute = compute
        self.vm_name = vm_name
        self.container_manifest = container_manifest
        self.machine_type = machine_type

    def to_dict(self):
        image_response = (
            self.compute.images()
            .getFromFamily(
                project=config["disk_image"]["project"],
                family=config["disk_image"]["family"],
            )
            .execute()
        )
        source_disk_image = image_response["selfLink"]

        machine_type = "zones/{}/machineTypes/{}".format(
            config["zone"], self.machine_type
        )

        rendered = json.loads(self.template_env.get_template("machine_config.json.j2").render(
            vm_name=self.vm_name,
            source_image=source_disk_image,
            project_id=config["project_id"],
            machine_type=self.machine_type,
            region=config["region"],
            scopes=config["default_scopes"],
        ))

        rendered["metadata"]["items"][0]["value"] = self.container_manifest

        return rendered

class Job:
    def __init__(self, script, gcloud, machine_type, image):
        self.script = script
        self.name = "clash-job-{}".format(uuid.uuid1())
        self.image = image
        self.compute = gcloud.get_compute_client()
        self.machine_type = machine_type

    def _create_machine_config(self):
        container_manifest = ContainerManifest(
            self.name, self.script, self.image
        ).to_yaml()

        return MachineConfig(self.compute, self.name, container_manifest, self.machine_type).to_dict()

    def run(self, gcloud=CloudSdk()):
        machine_config = self._create_machine_config()

        operation = (
            self.compute.instances()
            .insert(
                project=config["project_id"], zone=config["zone"], body=machine_config
            )
            .execute()
        )


def create_job(script, gcloud=CloudSdk(), machine_type=config["default_machine_type"], image=config["default_image"]):
    return Job(script, gcloud, machine_type, image)


def main():
    job = create_job("echo 'hello world';")
    job.run()


if __name__ == "__main__":
    main()
