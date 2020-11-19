#!/usr/bin/env bash

set -e

function task_usage {
  echo 'Usage: ./run.sh init | lint | build | unit-test | format | integration-test | package | release | deploy-airflow-plugin'
  exit 1
}

function task_lint {
  cd python
  (poetry install && poetry run pylint pyclash/)
}

function task_format {
  cd python
  (poetry install && poetry run black pyclash/)
}

function task_unit_test {
  cd python
  (poetry install && poetry run pytest tests/test_clash.py "$@")
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
  format) task_format ;;
  package) task_package ;;
  release) task_release ;;
  deploy-airflow-plugin) task_deploy_airflow_plugin ;;
  *)     task_usage ;;
esac
