# -*- coding: future_fstrings -*-
from __future__ import print_function
import argparse
import logging
import uuid
import json
import time
import yaml
import os.path
from halo import Halo
import sys
import os
from subprocess import call
from threading import Lock

import jinja2
import click
import googleapiclient.discovery
from google.cloud import pubsub_v1 as pubsub
from google.cloud import logging as glogging

logger = logging.getLogger(__name__)

DEFAULT_JOB_CONFIG = {
    "project_id": "my-gcp-project",
    "image": "google/cloud-sdk",
    "privileged": False,
    "preemptible": False,
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
    """ Having this class avoids dependency issues with the compute engine client"""

    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content


class CloudSdk:
    """ Provides access to the GCP services (e.g. logging, compute engine, etc.) """

    def __init__(self):
        pass

    def get_compute_client(self):
        return googleapiclient.discovery.build("compute", "v1", cache=MemoryCache())

    def get_publisher(self):
        return pubsub.PublisherClient()

    def get_subscriber(self):
        return pubsub.SubscriberClient()

    def get_logging(self, project=None):
        if project:
            return glogging.Client(project=project)
        return glogging.Client()


class CloudInitConfig:
    """
    This class provides means to create a configuration for cloud-init.

    (e.g. one which starts a Docker container on the target machine)
    """

    def __init__(
        self, vm_name, script, job_config, env_vars={}, gcs_target={}, gcs_mounts={}
    ):
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(
                searchpath=os.path.join(os.path.dirname(__file__), "templates")
            )
        )
        self.vm_name = vm_name
        self.script = script
        self.job_config = job_config
        self.env_vars = env_vars
        self.gcs_target = gcs_target
        self.gcs_mounts = gcs_mounts

    def render(self):
        """
        Renders the cloud-init configuration

        Returns:
            string: a cloud-init configuration file
        """
        clash_runner_script = self.template_env.get_template(
            "clash_runner.sh.j2"
        ).render(
            vm_name=self.vm_name,
            zone=self.job_config["zone"],
            gcs_target=self.gcs_target,
            gcs_mounts=self.gcs_mounts,
        )

        env_var_file = "\n".join(
            [f"{var}={value}" for var, value in self.env_vars.items()]
        )

        return self.template_env.get_template("cloud-init.yaml.j2").render(
            vm_name=self.vm_name,
            image=self.job_config["image"],
            clash_runner_script=clash_runner_script,
            script=self.script,
            env_var_file=env_var_file,
            privileged=self.job_config["privileged"],
        )


class MachineConfig:
    """
    This class provides methods for creating a machine configuration
    for the Google Compute Engine.
    """

    def __init__(self, compute, vm_name, cloud_init, job_config):
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(
                searchpath=os.path.join(os.path.dirname(__file__), "templates")
            )
        )
        self.compute = compute
        self.vm_name = vm_name
        self.cloud_init = cloud_init
        self.job_config = job_config

    def to_dict(self):
        """
            Creates the machine configuration

            Returns:
                dict: the configuration
        """
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
                preemptible=self.job_config["preemptible"],
            )
        )

        rendered["metadata"]["items"][0]["value"] = self.cloud_init.render()

        return rendered


class StackdriverLogsReader:
    """
    Reads logs of a job.
    """

    def __init__(self, job, log_func=logger.info):
        """
        Args:
            job: a job
            log_func (string -> ): function which processes a log entry
        """
        self.job = job

        self.logging_client = job.gcloud.get_logging(job.job_config["project_id"])
        self.publisher = job.gcloud.get_publisher()
        self.subscriber = job.gcloud.get_subscriber()
        self.log_func = log_func

    def _create_callback(self):
        logging_mutex = Lock()

        def logging_callback(message):
            logging_mutex.acquire()
            try:
                payload = json.loads(message.data)["jsonPayload"]
                if "data" in payload:
                    self.log_func(payload["data"])
                message.ack()
            finally:
                logging_mutex.release()

        return logging_callback

    def _create_filter(self):
        return f"""
        resource.type="global"
        logName="projects/{self.job.job_config["project_id"]}/logs/gcplogs-docker-driver"
        jsonPayload.instance.name="{self.job.name}"
        """

    def __enter__(self):
        self.logging_topic = self.publisher.topic_path(
            self.job.job_config["project_id"], self.job.name + "-logs"
        )
        self.publisher.create_topic(self.logging_topic)

        self.sink = self.logging_client.sink(
            self.job.name,
            filter_=self._create_filter(),
            destination=f"pubsub.googleapis.com/{self.logging_topic}",
        )
        self.sink.create()

        self.subscription_path = self.subscriber.subscription_path(
            self.job.job_config["project_id"], self.job.name + "-logs"
        )
        self.subscriber.create_subscription(self.subscription_path, self.logging_topic)
        self.subscriber.subscribe(
            self.subscription_path, callback=self._create_callback()
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.sink.delete()
        self.publisher.delete_topic(self.logging_topic)
        self.subscriber.delete_subscription(self.subscription_path)


class Job:
    """
    This class creates Clash-jobs and runs them on the Google Compute Engine (GCE).
    """

    POLLING_INTERVAL_SECONDS = 30

    def __init__(self, job_config, name=None, name_prefix=None, gcloud=CloudSdk()):
        self.gcloud = gcloud
        self.job_config = job_config

        if not name:
            self.name = "clash-job-{}".format(uuid.uuid1())
            if name_prefix:
                self.name = f"{name_prefix}-" + self.name
        else:
            self.name = name

    def run(self, script, env_vars={}, gcs_target={}, gcs_mounts={}):
        """
        Runs a script which is given as a string.

        Args:
            script (string): A Bash script which will be executed on GCE.
            env_vars (dict): Environment variables which can be used by the script.
            gcs_target (dict): Files which will be copied to GCS when the script is done.
            gcs_mounts (dict): Buckets which will be mounted using gcsfuse (if available).
        """
        machine_config = self._create_machine_config(
            script, env_vars, gcs_target, gcs_mounts
        )

        client = self.gcloud.get_publisher()
        job_status_topic = client.topic_path(self.job_config["project_id"], self.name)
        client.create_topic(job_status_topic)

        self.gcloud.get_compute_client().instances().insert(
            project=self.job_config["project_id"],
            zone=self.job_config["zone"],
            body=machine_config,
        ).execute()

    def run_file(self, script_file, env_vars={}, gcs_target={}, gcs_mounts={}):
        """
        Runs a script which is given as a file.

        Args:
            script_file (string): Path to a Bash script which will be executed on GCE.
            env_vars (dict): Environment variables which can be used by the script.
            gcs_target (dict): Files which will be copied to GCS when the script is done.
            gcs_mounts (dict): Buckets which will be mounted using gcsfuse (if available).
        """
        with open(script_file, "r") as f:
            script = f.read()
        self.run(script, env_vars, gcs_target)

    def _create_machine_config(self, script, env_vars, gcs_target, gcs_mounts):
        cloud_init = CloudInitConfig(
            self.name, script, self.job_config, env_vars, gcs_target, gcs_mounts
        )

        return MachineConfig(
            self.gcloud.get_compute_client(), self.name, cloud_init, self.job_config
        ).to_dict()

    def attach(self):
        """
        Blocks until the job terminates.
        """
        subscriber = self.gcloud.get_subscriber()
        subscription_path = self._create_subscription(subscriber)

        try:
            while True:
                message = self._pull_message(subscriber, subscription_path)
                if message:
                    return json.loads(message.data)
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
            logger.info(entry)


def attach_to(job):
    with StackdriverLogsReader(job, log_func=print):
        result = job.attach()
    sys.exit(result["status"])


def ensure_config(config_file):
    if not os.path.isfile(config_file):
        print(f"Creating basic configuration {config_file}...")
        with open(config_file, "w") as f:
            yaml.dump(DEFAULT_JOB_CONFIG, f, default_flow_style=False)
        raw_input("Press enter to review the configuration file.")
        EDITOR = os.environ.get("EDITOR", "vim")
        call([EDITOR, config_file])
    else:
        print(f"Using configuration file {config_file}.")


def from_env(value, key):
    return os.getenv(key, value)


def load_config(config_file):
    if not os.path.isfile(config_file):
        raise ValueError(
            "No configration file found. Please create one (e.g. by using clash init)"
        )

    template_env = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath="."))
    template_env.filters["from_env"] = from_env
    rendered_config = template_env.get_template(config_file).render()
    return yaml.load(rendered_config)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--config", default="clash.yml")
def init(config):
    ensure_config(config)
    print("Clash is now initialized!")


@click.argument("script")
@click.option("--detach", is_flag=True)
@click.option("--from-file", is_flag=True)
@click.option("--config", default="clash.yml")
@click.option("--env", "-e", multiple=True)
@click.option("--gcs-target", multiple=True)
@click.option("--gcs-mount", multiple=True)
@cli.command()
def run(script, detach, from_file, config, env, gcs_target, gcs_mount):
    logging.basicConfig(level=logging.ERROR)

    env_vars = {}
    for e in env:
        var, value = e.split("=")
        env_vars[var] = value

    gcs_targets = {}
    for t in gcs_target:
        directory, gcs_bucket = t.split(":")
        gcs_targets[directory] = gcs_bucket

    gcs_mounts = {}
    for t in gcs_mount:
        gcs_bucket, mount_point = t.split(":")
        gcs_mounts[gcs_bucket] = mount_point

    job_config = load_config(config)
    job = Job(job_config)
    with Halo(text="Creating job", spinner="dots") as spinner:
        if from_file:
            job.run_file(script, env_vars, gcs_targets, gcs_mounts)
        else:
            job.run(script, env_vars, gcs_targets, gcs_mounts)

    if not detach:
        attach_to(job)
    else:
        print(job.name)


@click.argument("job_name")
@click.option("--config", default="clash.yml")
@cli.command()
def attach(job_name, config):
    job_config = load_config(config)
    job = Job(job_config, name=job_name)
    attach_to(job)


if __name__ == "__main__":
    main()
