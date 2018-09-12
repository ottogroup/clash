import argparse
import uuid

from jinja2 import Template
import googleapiclient.discovery

config = {
    "image": "google/cloud-sdk",
    "zone": "europe-west1-b",
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


class Job:
    def __init__(self, project_id, script):
        self.project_id = project_id
        self.script = script
        self.vm_name = "clash-job-{}".format(uuid.uuid1())

    def _create_machine_config(self, compute):
        image_response = (
            compute.images()
            .getFromFamily(
                project=config["image"]["project"], family=config["image"]["family"]
            )
            .execute()
        )
        source_disk_image = image_response["selfLink"]

        machine_type = "zones/{}/machineTypes/{}".format(
            config["zone"], config["machine_type"]
        )

        template_loader = jinja2.FileSystemLoader(searchpath="./templates")
        template_env = jinja2.Environment(loader=template_loader)

        clash_runner_script = template_env.get_template("clash_runner.sh.j2").render(
            vm_name=vm_name, zone=config["zone"]
        )

        container_manifest = template_env.get_template(
            "container_manifest.yaml.j2"
        ).render(
            vm_name=vm_name,
            image=config["image"],
            clash_runner_script=clash_runner_script,
            script=self.script,
        )

        return template_env.get_template("machine_config.json.j2").render(
            vm_name=vm_name,
            source_image=source_disk_image,
            project_id=self.project_id,
            machine_type=machine_type,
            container_manifest=container_manifest,
            region=config["region"],
            scopes=config["scopes"],
        )


    def run(self):
        compute = googleapiclient.discovery.build("compute", "v1")
        machine_config = _create_machine_config(compute)

        operation = (
            compute.instances()
            .insert(project=self.project_id, zone=config["zone"], body=machine_config)
            .execute()
        )


def create_job(project_id, zone, script):
    return Job(project_id, zone, script)


def main():
    job = create_job(
        "***REMOVED***", "europe-west1-b", "echo 'hello world';"
    )
    job.run()


if __name__ == "__main__":
    main()
