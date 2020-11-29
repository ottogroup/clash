""" CLI for Clash """

import sys
import base64

import click

from pyclash.clash import JobConfigBuilder, Job


@click.group()
def cli():
    pass


@cli.command()
@click.option("--name", type=click.STRING, required=True)
@click.option("--project", type=click.STRING, required=True)
@click.option("--image", type=click.STRING, required=True)
@click.option("--subnetwork", type=click.STRING, required=True)
@click.option("--serviceaccount", type=click.STRING, required=False, default=None)
@click.option("--preemptible", type=click.BOOL, required=False, default=False)
@click.option(
    "--machine-type", type=click.STRING, default="n1-standard-1", required=False
)
@click.option("--arg", type=click.STRING, required=True, multiple=True)
def run(
    name, project, image, subnetwork, serviceaccount, preemptible, machine_type, arg
):
    config = (
        JobConfigBuilder()
        .project_id(project)
        .image(image)
        .machine_type(machine_type)
        .subnetwork(subnetwork)
        .preemptible(preemptible)
    )
    if serviceaccount:
        config.service_account(serviceaccount)

    with Job(job_config=config.build(), name_prefix=name) as job:
        result = job.run(arg, wait_for_result=True)
        sys.stdout.write(base64.b64decode(result["logs"]).decode("utf-8"))
        sys.exit(result["status"])

    sys.exit(-3)
