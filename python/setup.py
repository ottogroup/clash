import os
from setuptools import setup, find_packages

version = "0.1.1"

install_requires = [
    "google-cloud-pubsub>=0.42.1",
    "google-api-python-client>=1.7.9",
    "google-cloud-logging>=1.11.0",
    "requests>=2.20.0",
    "jinja2>=2.10.1",
    "urllib3>=1.24.2",
]

setup(
    name="pyclash",
    version=version,
    description="Running jobs on the Google Compute Engine",
    author="Otto Group",
    license="Apache License, Version 2.0",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
)
