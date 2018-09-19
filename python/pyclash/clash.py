import argparse
import logging
import uuid
import json
import datetime
import time
from halo import Halo

import jinja2
import click
import googleapiclient.discovery
from google.cloud import pubsub_v1 as pubsub
from google.cloud import logging as glogging

logger = logging.getLogger(__name__)

DEFAULT_JOB_CONFIG = {
    "project_id": "***REMOVED***",
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


class MemoryCache:
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content


class CloudSdk:
    def __init__(self):
        pass

    def get_compute_client(self):
        return googleapiclient.discovery.build("compute", "v1", cache=MemoryCache())

    def get_publisher(self):
        return pubsub.PublisherClient()

    def get_subscriber(self):
        return pubsub.SubscriberClient()

    def get_logging(self):
        return glogging.Client()


class CloudInitConfig:
    def __init__(self, vm_name, script, job_config):
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(searchpath="../templates")
        )
        self.vm_name = vm_name
        self.script = script
        self.job_config = job_config

    def render(self):
        clash_runner_script = self.template_env.get_template(
            "clash_runner.sh.j2"
        ).render(vm_name=self.vm_name, zone=self.job_config["zone"])

        return self.template_env.get_template("cloud-init.yaml.j2").render(
            vm_name=self.vm_name,
            image=self.job_config["image"],
            clash_runner_script=clash_runner_script,
            script=self.script,
        )


class MachineConfig:
    def __init__(self, compute, vm_name, cloud_init, job_config):
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(searchpath="../templates")
        )
        self.compute = compute
        self.vm_name = vm_name
        self.cloud_init = cloud_init
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

        rendered["metadata"]["items"][0]["value"] = self.cloud_init.render()

        return rendered


class StackdriverLogsReader:

    STACKDRIVER_DELAY_SECONDS = 5

    def __init__(self, logging_client):
        self.logging_client = logging_client

    def wait_for_logs_arrival(self):
        time.sleep(StackdriverLogsReader.STACKDRIVER_DELAY_SECONDS)

    def _now(self):
        return datetime.datetime.now(datetime.timezone.utc)

    def _delta(self, delta_time):
        return datetime.timedelta(seconds=delta_time)

    def _to_iso_format(self, local_time):
        return local_time.astimezone().isoformat()

    def read_logs(self, job, from_seconds_ago):
        project_id = job.job_config["project_id"]
        local_time = self._now() - self._delta(from_seconds_ago)
        iso_time = self._to_iso_format(local_time)
        FILTER = f"""
            resource.type="global"
            logName="projects/{project_id}/logs/gcplogs-docker-driver"
            jsonPayload.instance.name="{job.name}"
            timestamp >= "{iso_time}"
        """
        return [
            entry.payload["data"]
            for entry in self.logging_client.list_entries(filter_=FILTER)
        ]


class Job:

    POLLING_INTERVAL_SECONDS = 30

    def __init__(self, name=None, gcloud=CloudSdk(), job_config=DEFAULT_JOB_CONFIG):
        self.gcloud = gcloud
        self.job_config = job_config

        if not name:
            self.name = "clash-job-{}".format(uuid.uuid1())
        else:
            self.name = name

    def run(self, script):
        machine_config = self._create_machine_config(script)

        client = self.gcloud.get_publisher()
        topic_path = client.topic_path(self.job_config["project_id"], self.name)
        client.create_topic(topic_path)

        self.gcloud.get_compute_client().instances().insert(
            project=self.job_config["project_id"],
            zone=self.job_config["zone"],
            body=machine_config,
        ).execute()

    def _create_machine_config(self, script):
        cloud_init = CloudInitConfig(self.name, script, self.job_config)

        return MachineConfig(
            self.gcloud.get_compute_client(), self.name, cloud_init, self.job_config
        ).to_dict()

    def attach(self, logs_reader=None):
        subscriber = self.gcloud.get_subscriber()
        subscription_path = self._create_subscription(subscriber)

        try:
            while True:
                message = self._pull_message(subscriber, subscription_path)
                if logs_reader:
                    self._print_logs(logs_reader)
                if message:
                    break
        except Exception as ex:
            raise ex
        finally:
            subscriber.delete_subscription(subscription_path)

    def _pull_message(self, subscriber, subscription_path):
        response = subscriber.pull(
            subscription_path,
            max_messages=1,
            return_immediately=False,
            timeout=Job.POLLING_INTERVAL_SECONDS,
        )

        if len(response.received_messages) > 0:
            message = response.received_messages[0]
            ack_id = message.ack_id
            subscriber.acknowledge(subscription_path, [ack_id])
            return message.message

        return None

    def _create_subscription(self, subscriber):
        project_id = self.job_config["project_id"]
        publisher = self.gcloud.get_publisher()
        topic_path = subscriber.topic_path(project_id, self.name)
        topics = [path.name for path in publisher.list_topics(f"projects/{project_id}")]

        if topic_path not in topics:
            raise ValueError("Could not find job {}".format(self.name))

        subscription_path = subscriber.subscription_path(
            self.job_config["project_id"], self.name
        )
        subscriber.create_subscription(subscription_path, topic_path)

        return subscription_path

    def _print_logs(self, logs_reader):
        logs_reader.wait_for_logs_arrival()
        logs = logs_reader.read_logs(self, Job.POLLING_INTERVAL_SECONDS)
        for entry in logs:
            print(entry)


def attach_to(job):
    try:
        logs_reader = StackdriverLogsReader(job.gcloud.get_logging())
        job.attach(logs_reader)
    except Exception as ex:
        logger.error(ex)


@click.group()
def cli():
    pass


@click.argument("raw_script")
@click.option("--detach", is_flag=True)
@cli.command()
def run(raw_script, detach):
    job = Job()
    logging.basicConfig(level=logging.ERROR)
    with Halo(text="Creating job", spinner="dots") as spinner:
        job.run(raw_script)

    if not detach:
        attach_to(job)
    else:
        print(job.name)


@click.argument("job_name")
@cli.command()
def attach(job_name):
    job = Job(job_name)
    attach_to(job)


if __name__ == "__main__":
    main()
