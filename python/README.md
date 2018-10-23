## CLASH

CLASH is an acronym for CLoud bASH. It makes running particulalry easiy to run scalable data processing scripts inside the Google Cloud Platform. It has a simple straightforward cli:

```
$ clash
Usage: clash [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  attach
  init
  run
```

Because CLASH uses the [Google Cloud SDK](https://github.com/googleapis/google-cloud-python) you first have to set up your local environment to access GCP. Please visit the [gcloud docs](https://cloud.google.com/sdk/gcloud/reference/auth/) for that matter.


## Example
The obligatory hello-world example. After installing the package you have to first run _clash init_. This will print out the following message:

```
$ pip install pyclash 
$ clash init
Creating basic configuration clash.yml...
Press enter to review the configuration file.
```
The purpose of this step is to create a _clash.yml_ which is a basic configuration file for CLASH and looks like this:

```
disk_image:
  family: cos-stable
  project: gce-uefi-images
image: google/cloud-sdk
machine_type: n1-standard-1
preemptible: false
privileged: false
project_id: my-gcp-project
region: europe-west1
scopes:
- https://www.googleapis.com/auth/bigquery
- https://www.googleapis.com/auth/compute
- https://www.googleapis.com/auth/devstorage.read_write
- https://www.googleapis.com/auth/logging.write
- https://www.googleapis.com/auth/monitoring
- https://www.googleapis.com/auth/pubsub
subnetwork: default-europe-west1
zone: europe-west1-b
```

CLASH supports [Jinja2](http://jinja.pocoo.org/) style templating in _clash.yml_. Which comes very handy for fields like _project_id_ or _machine_type_. 

An actual run will look like this:
```
$ clash run "echo hello-world"
- Creating job
hello-world
```

Alternatively you can also pass a script via:
```
$ clash run --from-file script.sh
```