""" CLI for Clash """

import sys

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
@click.option("--serviceaccount", type=click.STRING, required=True)
@click.option("--preemptible", type=click.BOOL, required=False, default=False)
@click.option(
    "--machine-type", type=click.STRING, default="n1-standard-1", required=False
)
@click.option("--arg", type=click.STRING, required=True, multiple=True)
def run(name, project, image, subnetwork, serviceaccount, preemptible, machine_type, arg):
    config = (
        JobConfigBuilder()
        .project_id(project)
        .image(image)
        .machine_type(machine_type)
        .subnetwork(subnetwork)
        .subnetwork(serviceaccount)
        .preemptible(preemptible)
        .build()
    )
    result = Job(job_config=config, name_prefix=name).run(arg, wait_for_result=True)
    sys.exit(result["status"])
