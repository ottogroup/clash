:rocket: CLASH is a tool for running bash scripts inside the cloud :rocket:

CLASH is an acronym for CLoud bASH. It makes running particulalry easiy to run scalable data processing scripts inside the Google Cloud Platform. It has a simple straightforward cli:

```
# fetch python package
$ pip install pyclash 

# init clash with basic configuration
$ clash init

# run clash
$ clash run "echo hello-world"
Waiting for the job to complete...
hello-world
```
  
Because CLASH uses the [Google Cloud SDK](https://github.com/googleapis/google-cloud-python) you first have to set up your local environment to access GCP. Please visit the [gcloud docs](https://cloud.google.com/sdk/gcloud/reference/auth/) for that matter.

### This repistory is divided into the following two packages:
 - [python: Contains the code of CLASH python package including a cli](python/README.md)
 - [airflow: Contains the code of the ClashOperator](airflow/Readme.md)
