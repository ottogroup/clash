#!/usr/bin/env bash

set -e

function task_usage {
  echo 'Usage: ./run.sh init | lint | build | unit-test | format | integration-test | package | release | deploy-airflow-plugin'
  exit 1
}

ensure_gcloud() {
  if [ -z "$GCP_PROJECT_ID" ]; then
    echo "Please set GCP_PROJECT_ID"
    exit 1
  fi

  if [ -z "$GCP_ZONE" ]; then
    echo "Please set GCP_ZONE"
    exit 1
  fi

  if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    local default_credentials_path=~/.config/gcloud/application_default_credentials.json
    if [ ! -f "$default_credentials_path" ]; then
        gcloud auth application-default login
    else
      echo "Using default credentials $default_credentials_path"
    fi
  else
    gcloud auth activate-service-account --key-file "$GOOGLE_APPLICATION_CREDENTIALS"
  fi

  gcloud config set project "$GCP_PROJECT_ID"
  gcloud config set compute/zone "$GCP_ZONE"
}

function task_lint {
  cd python
  (poetry install && poetry run pylint --rcfile pylintrc pyclash/)
}

function task_format {
  cd python
  (poetry install && poetry run black pyclash/ tests/)
}

function task_unit_test {
  cd python
  (poetry install && poetry run pytest tests/test_clash.py "$@")
}

function task_build_image {
  ensure_gcloud

  version=$(cd python && poetry version -s)
  gcloud builds submit --tag eu.gcr.io/"$GCP_PROJECT_ID"/clash:"$version" .
}

function task_package {
  (cd python && poetry build)
}

function task_release {
  cd python
  (poetry install && poetry run twine upload dist/*)
}

function task_deploy_airflow_plugin {
  if [ -z "$COMPOSER_ENVIRONMENT" ]; then
   echo 'Please set COMPOSER_ENVIRONMENT'
   exit 1
  fi

  if [ -z "$COMPOSER_LOCATION" ]; then
   echo 'Please set COMPOSER_LOCATION'
   exit 1
  fi
  gcloud composer environments storage plugins import --environment "$COMPOSER_ENVIRONMENT" \
      --location "$COMPOSER_LOCATION" \
      --source airflow/clash_plugin.py \
      --destination 'tooling/'
}

function task_integration_test {
  if [ -z "$GCP_PROJECT_ID" ]; then
   echo 'Please set GCP_PROJECT_ID'
   exit 1
  fi

  cd python
  (poetry install && poetry run python ../examples/job.py)
}


cmd=$1
shift || true
case "$cmd" in
  lint) task_lint ;;
  unit-test) task_unit_test "$@" ;;
  integration-test) task_integration_test ;;
  build-image) task_build_image ;;
  format) task_format ;;
  package) task_package ;;
  release) task_release ;;
  deploy-airflow-plugin) task_deploy_airflow_plugin ;;
  *)     task_usage ;;
esac
