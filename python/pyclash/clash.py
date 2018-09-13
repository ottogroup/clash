import argparse
import uuid
import json

import jinja2
import googleapiclient.discovery
import click


class CloudSdk:
    def __init__(self):
        pass

    def get_compute_client(self):
        return googleapiclient.discovery.build("compute", "v1")


class ContainerManifest:
    def __init__(self, vm_name, script, job_config):
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(searchpath="../templates")
        )
        self.vm_name = vm_name
        self.script = script
        self.job_config = job_config

    def to_yaml(self):
        clash_runner_script = self.template_env.get_template(
            "clash_runner.sh.j2"
        ).render(vm_name=self.vm_name, zone=self.job_config["zone"])

        return self.template_env.get_template("container_manifest.yaml.j2").render(
            vm_name=self.vm_name,
            image=self.job_config["image"],
            clash_runner_script=clash_runner_script,
            script=self.script,
        )


class MachineConfig:
    def __init__(self, compute, vm_name, container_manifest, job_config):
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(searchpath="../templates")
        )
        self.compute = compute
        self.vm_name = vm_name
        self.container_manifest = container_manifest
        self.job_config = job_config

    def to_dict(self):
        image_response = (
            self.compute.images()
            .getFromFamily(
                project=self.job_config["disk_image"]["project"],
                family=self.job_config["disk_image"]["family"],
            )
            .execute()
        )
        source_disk_image = image_response["selfLink"]

        machine_type = "zones/{}/machineTypes/{}".format(
            self.job_config["zone"], self.job_config["machine_type"]
        )

        rendered = json.loads(
            self.template_env.get_template("machine_config.json.j2").render(
                vm_name=self.vm_name,
                source_image=source_disk_image,
                project_id=self.job_config["project_id"],
                machine_type=machine_type,
                region=self.job_config["region"],
                scopes=self.job_config["scopes"],
                subnetwork=self.job_config["subnetwork"],
            )
        )

        rendered["metadata"]["items"][0]["value"] = self.container_manifest.to_yaml()

        return rendered


class Job:
    def __init__(self, script, gcloud, job_config):
        self.script = script
        self.name = "clash-job-{}".format(uuid.uuid1())
        self.compute = gcloud.get_compute_client()
        self.job_config = job_config

    def _create_machine_config(self):
        container_manifest = ContainerManifest(self.name, self.script, self.job_config)

        return MachineConfig(
            self.compute, self.name, container_manifest, self.job_config
        ).to_dict()

    def run(self, gcloud=CloudSdk()):
        machine_config = self._create_machine_config()

        operation = (
            self.compute.instances()
            .insert(
                project=self.job_config["project_id"],
                zone=self.job_config["zone"],
                body=machine_config,
            )
            .execute()
        )


def create_job(script, gcloud=CloudSdk()):
    DEFAULT_JOB_CONFIG = {
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
            "https://www.googleapis.com/auth/devstorage.read_write",
            "https://www.googleapis.com/auth/logging.write",
            "https://www.googleapis.com/auth/monitoring",
            "https://www.googleapis.com/auth/pubsub",
        ],
    }
    return Job(script, gcloud, DEFAULT_JOB_CONFIG)

@click.group()
def cli():
    pass

@click.argument('raw_script')
@cli.command()
def run(raw_script):
    job = create_job(raw_script)
    job.run()

if __name__ == "__main__":
    main()
