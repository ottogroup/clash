import os
from setuptools import setup, find_packages

version = open("../VERSION").read().rstrip()

install_requires = []

tests_require = ["pytest"]

setup(
    name="clash",
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
