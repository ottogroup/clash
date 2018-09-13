import argparse
import uuid

import jinja2
import googleapiclient.discovery

config = {
    "project_id": "yourproject-foobar",
    "image": "google/cloud-sdk",
    "zone": "europe-west1-b",
    "region": "europe-west1",
    "machine_type": "n1-standard-1",
    "disk_image": {"project": "gce-uefi-images", "family": "cos-stable"},
    "scopes": [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/compute",
        "https://www.googleapis.com/auth/devstorage.read_write",
        "https://www.googleapis.com/auth/logging.write",
        "https://www.googleapis.com/auth/monitoring",
        "https://www.googleapis.com/auth/pubsub"
    ]
}

class CloudSdk:
    def __init__(self):
        pass

    def get_compute_client(self):
        return googleapiclient.discovery.build("compute", "v1")

class ContainerManifest:
    def __init__(self, vm_name, script, image):
        template_loader = jinja2.FileSystemLoader(searchpath="../templates")
        self.template_env = jinja2.Environment(loader=template_loader)
        self.vm_name = vm_name
        self.script = script
        self.image = image

    def render(self):
        clash_runner_script = self.template_env.get_template("clash_runner.sh.j2").render(
            vm_name=self.vm_name, zone=config["zone"]
        )

        return self.template_env.get_template(
            "container_manifest.yaml.j2"
        ).render(
            vm_name=self.vm_name,
            image=self.image,
            clash_runner_script=clash_runner_script,
            script=self.script,
        )

class MachineConfig:
    def __init__(self):
        pass

class Job:
    def __init__(self, script):
        self.script = script
        self.name = "clash-job-{}".format(uuid.uuid1())

    def _create_machine_config(self, compute):
        image_response = (
            compute.images()
            .getFromFamily(
                project=config["disk_image"]["project"], family=config["disk_image"]["family"]
            )
            .execute()
        )
        source_disk_image = image_response["selfLink"]

        machine_type = "zones/{}/machineTypes/{}".format(
            config["zone"], config["machine_type"]
        )

        template_loader = jinja2.FileSystemLoader(searchpath="../templates")
        template_env = jinja2.Environment(loader=template_loader)

        container_manifest = ContainerManifest(self.name, self.script, config['image']).render()

        return template_env.get_template("machine_config.json.j2").render(
            vm_name=self.name,
            source_image=source_disk_image,
            project_id=config['project_id'],
            machine_type=machine_type,
            container_manifest=container_manifest,
            region=config["region"],
            scopes=config["scopes"],
        )


    def run(self, gcloud=CloudSdk()):
        compute = gcloud.get_compute_client()
        machine_config = self._create_machine_config(compute)

        operation = (
            compute.instances()
            .insert(project=config["project_id"], zone=config["zone"], body=machine_config)
            .execute()
        )


def create_job(script):
    return Job(script)


def main():
    job = create_job("echo 'hello world';")
    job.run()


if __name__ == "__main__":
    main()
