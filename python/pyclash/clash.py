import argparse
import uuid

from jinja2 import Template
import googleapiclient.discovery

config = {
    'image': 'google/cloud-sdk',
    'zone': 'europe-west1-b',
    'machine_type': 'n1-standard-1',
    'disk_image': {
        'project': 'gce-uefi-images',
        'family': 'cos-stable'
    }
}


class Job:

    def __init__(self, project_id, zone, script):
        self.project_id = project_id
        self.zone = zone
        self.script = script
        self.vm_name = "clash-job-{}".format(uuid.uuid1())

    def run(self):
        compute = googleapiclient.discovery.build('compute', 'v1')

        image_response = compute.images().getFromFamily(
            project=config['image']['project'], family=config['image']['family']).execute()
        source_disk_image = image_response['selfLink']

        machine_type = "zones/{}/machineTypes/{}".format(
            config['zone'], config['machine_type'])

        operation = compute.instances().insert(project=self.project_id,
                                               zone=self.zone, body=config).execute()


def create_job(project_id, zone, script):
    return Job(project_id, zone, script)


def main():
    job = create_job('yourproject-hackingdays2018-dev',
                     'europe-west1-b', 'echo \'hello world\';')
    job.run()


if __name__ == '__main__':
    main()
