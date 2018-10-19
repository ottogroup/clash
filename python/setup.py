import os
from setuptools import setup, find_packages

version = "0.0.4r6"

install_requires = [
    "jinja2",
    "google-api-python-client>=1.7.4",
    "pyyaml",
    "click",
    "google-cloud-pubsub>=0.38.0",
    "google-cloud-logging>=1.7.0",
    "halo",
    "future-fstrings",
]

tests_require = ["pytest", "docker", "mock"]

setup(
    name="pyclash",
    version=version,
    description="Running bash scripts on the Google Compute Engine",
    author="Otto Group",
    license="Apache License, Version 2.0",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    entry_points={"console_scripts": ["clash = pyclash.clash:cli"]},
)
