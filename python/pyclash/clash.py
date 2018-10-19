# -*- coding: future_fstrings -*-
from __future__ import print_function
import argparse
import logging
import uuid
import copy
import json
import time
import yaml
import os.path
from halo import Halo
import sys
import os
from subprocess import call
from threading import Lock
from collections import namedtuple

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


class JobConfigBuilder:
    def __init__(self, base_config=DEFAULT_JOB_CONFIG):
        self.config = copy.deepcopy(base_config)

    def project_id(self, project_id):
        self.config["project_id"] = project_id
        return self

    def image(self, image):
        self.config["image"] = image
        return self

    def privileged(self, privileged):
        self.config["privileged"] = privileged
        return self

    def preemptible(self, preemptible):
        self.config["preemptible"] = preemptible
        return self

    def zone(self, zone):
        self.config["zone"] = zone
        return self

    def region(self, region):
        self.config["region"] = region
        return self

    def subnetwork(self, subnetwork):
        self.config["subnetwork"] = subnetwork
        return self

    def machine_type(self, machine_type):
        self.config["machine_type"] = machine_type
        return self

    def disk_image(self, disk_image):
        self.config["disk_image"] = disk_image
        return self

    def scopes(self, scopes):
        self.config["scopes"] = scopes
        return self

    def build(self):
        return copy.deepcopy(self.config)


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

        rendered = json.loads(
            self.template_env.get_template("machine_config.json.j2").render(
                vm_name=self.vm_name,
                source_image=source_disk_image,
                project_id=self.job_config["project_id"],
                machine_type=self.job_config["machine_type"],
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

    def __init__(self, job_or_group, log_func=logger.info):
        """
        Args:
            job_or_group: a job or a job-group
            log_func (string -> ): function which processes a log entry
        """
        self.job_or_group = job_or_group

        self.logging_client = job_or_group.gcloud.get_logging(
            job_or_group.job_config["project_id"]
        )
        self.publisher = job_or_group.gcloud.get_publisher()
        self.subscriber = job_or_group.gcloud.get_subscriber()
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
        # ':' checks whether a log entry contains a string whereas '=' checks for equality
        log_restriction_char = ":" if self.job_or_group.is_group() else "="
        return f"""
        resource.type="global"
        logName="projects/{self.job_or_group.job_config["project_id"]}/logs/gcplogs-docker-driver"
        jsonPayload.instance.name{log_restriction_char}"{self.job_or_group.name}"
        """

    def __enter__(self):
        self.logging_topic = self.publisher.topic_path(
            self.job_or_group.job_config["project_id"], self.job_or_group.name + "-logs"
        )
        self.publisher.create_topic(self.logging_topic)

        self.sink = self.logging_client.sink(
            self.job_or_group.name,
            filter_=self._create_filter(),
            destination=f"pubsub.googleapis.com/{self.logging_topic}",
        )
        self.sink.create()

        self.subscription_path = self.subscriber.subscription_path(
            self.job_or_group.job_config["project_id"], self.job_or_group.name + "-logs"
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


class JobRuntimeSpec:
    def __init__(self, script, env_vars={}, gcs_mounts={}, gcs_target={}):
        self.script = script
        self.env_vars = env_vars
        self.gcs_mounts = gcs_mounts
        self.gcs_target = gcs_target


class JobFactory:
    def __init__(self, job_config, gcloud=CloudSdk()):
        self.job_config = job_config
        self.gcloud = gcloud

    def create(self, name_prefix):
        return Job(
            name_prefix=name_prefix, job_config=self.job_config, gcloud=self.gcloud
        )


class JobGroup:
    """
    This class allows the creation of multiple jobs.
    """

    def __init__(self, name, job_factory):
        """
        Constructs a new group.

        :param name the name of the group
        :param job_factory a factory that creates individual jobs
        """
        self.name = name
        self.job_factory = job_factory
        self.job_config = job_factory.job_config
        self.gcloud = job_factory.gcloud

        self.job_specs = []
        self.running_jobs = []
        self.jobs_status_codes = []

    def add_job(self, runtime_spec):
        """
        Adds a job to the group.
        :param runtime_spec runtime specification of the job
        """
        self.job_specs.append(runtime_spec)

    def run(self):
        """
        Runs all jobs that are part of the group.
        """
        for spec_id, spec in enumerate(self.job_specs):
            job = self.job_factory.create(name_prefix=f"{self.name}-{spec_id}")
            job.run(
                script=spec.script,
                env_vars=spec.env_vars,
                gcs_mounts=spec.gcs_mounts,
                gcs_target=spec.gcs_target,
            )
            # arrays are thread-safe in Python (due to GIL)
            job.on_finish(
                lambda status_code: self.jobs_status_codes.append(status_code)
            )
            self.running_jobs.append(job)

    def wait(self):
        """
        Blocks until all jobs of the group are complete.

        :returns true if all jobs succeeded else false
        """
        while not len(self.jobs_status_codes) == len(self.running_jobs):
            time.sleep(1)

        return all(map(lambda code: code == 0, self.jobs_status_codes))

    def clean_up(self):
        """
        Manual clean up. This method is a workaround and will disappear soon.
        """
        for job in self.running_jobs:
            job.clean_up()

    def is_group(self):
        return True


class Job:
    """
    This class creates Clash-jobs and runs them on the Google Compute Engine (GCE).
    """

    POLLING_INTERVAL_SECONDS = 30

    def __init__(self, job_config, name=None, name_prefix=None, gcloud=CloudSdk()):
        self.gcloud = gcloud
        self.job_config = job_config
        self.started = False
        self.subscriber = self.gcloud.get_subscriber()
        self.publisher = self.gcloud.get_publisher()

        if not name:
            self.name = "clash-job-{}".format(str(uuid.uuid1())[0:16])
            if name_prefix:
                self.name = f"{name_prefix}-" + self.name
        else:
            self.name = name

    def _wait_for_operation(self, operation, is_global_op):
        compute = self.gcloud.get_compute_client()
        operations_client = (
            compute.globalOperations() if is_global_op else compute.zoneOperations()
        )

        args = {"project": self.job_config["project_id"], "operation": operation}
        if not is_global_op:
            args["zone"] = self.job_config["zone"]

        while True:
            result = operations_client.get(**args).execute()

            if result["status"] == "DONE":
                if "error" in result:
                    raise Exception(result["error"])
                return result

            time.sleep(1)

    def _create_instance_template(self, machine_config):
        template_op = (
            self.gcloud.get_compute_client()
            .instanceTemplates()
            .insert(
                project=self.job_config["project_id"],
                body={"name": self.name, "properties": machine_config},
            )
            .execute()
        )
        self._wait_for_operation(template_op["name"], True)

    def _create_managed_instance_group(self, size):
        self.gcloud.get_compute_client().instanceGroupManagers().insert(
            project=self.job_config["project_id"],
            zone=self.job_config["zone"],
            body={
                "baseInstanceName": self.name,
                "instanceTemplate": f"global/instanceTemplates/{self.name}",
                "name": self.name,
                "targetSize": size,
            },
        ).execute()

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

        self.job_status_topic = None
        self.job_status_subscription = None
        try:
            self.job_status_topic = self._create_status_topic()
            self.job_status_subscription = self._create_status_subscription()
            self._create_instance_template(machine_config)
            self._create_managed_instance_group(1)
        except Exception as ex:
            if self.job_status_topic:
                self.publisher.delete_topic(self.job_status_topic)
            if self.job_status_subscription:
                self.subscriber.delete_subscription(self.job_status_subscription)
            raise ex

        self.started = True

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

    def on_finish(self, callback):
        """
        Sets a callback function which is executed when the job is complete.
        """
        if not self.started:
            raise ValueError("The job is not running")

        def pubsub_callback(message):
            data = json.loads(message.data)
            callback(data["status"])
            message.ack()

        self.subscriber.subscribe(self.job_status_subscription, pubsub_callback)

    def clean_up(self):
        """
        Deletes resources which are left-overs after a job is complete. Currently,
        this method is just a workaround until we find a better solution.
        """
        if self.started:
            self._remove_instance_template()

    def _remove_instance_template(self):
        if not self.started:
            raise Exception("Job is not running")
        template_op = (
            self.gcloud.get_compute_client()
            .instanceTemplates()
            .delete(project=self.job_config["project_id"], instanceTemplate=self.name)
            .execute()
        )
        self._wait_for_operation(template_op["name"], True)

    def attach(self):
        """
        Blocks until the job terminates.
        """
        if not self.started:
            raise ValueError("The job is not running")

        while True:
            message = self._pull_message(self.subscriber, self.job_status_subscription)
            if message:
                return json.loads(message.data)

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

    def _create_status_topic(self):
        job_status_topic = self.publisher.topic_path(
            self.job_config["project_id"], self.name
        )
        self.publisher.create_topic(job_status_topic)
        return job_status_topic

    def _create_status_subscription(self):
        project_id = self.job_config["project_id"]
        topics = [
            path.name for path in self.publisher.list_topics(f"projects/{project_id}")
        ]

        if self.job_status_topic not in topics:
            raise ValueError(f"Could not find status topic for job {self.name}")

        subscription_path = self.subscriber.subscription_path(project_id, self.name)
        self.subscriber.create_subscription(subscription_path, self.job_status_topic)

        return subscription_path

    def _print_logs(self, logs_reader):
        logs_reader.wait_for_logs_arrival()
        logs = logs_reader.read_logs(self, Job.POLLING_INTERVAL_SECONDS)
        for entry in logs:
            logger.info(entry)

    def is_group(self):
        return False


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
