#!/usr/bin/env bash

set -e

function task_usage {
  echo 'Usage: ./run.sh init | lint | build | unit-test | integration-test | clash | format | package | release | deploy-airflow-plugin'
  exit 1
}

function task_init {
  pipenv install
}

function task_lint {
  exit 0
}

function task_format {
  black python/
}

function task_unit_test {
  cd python
  pipenv run python setup.py develop
  pipenv run pytest tests/test_clash.py "$@"
}

function task_integration_test {
  cd python
  docker build -f tests/Dockerfile -t 'test-cloudsdk:latest' tests/
  pipenv run pytest tests/test_integration.py
}

function task_package {
  cd python
  pipenv run python setup.py sdist bdist_wheel
}

function task_release {
  cd python
  pipenv run twine upload dist/*
}

function task_deploy_airflow_plugin {
  if [ -z "$COMPOSER_ENVIRONMENT" ]; then
   echo 'Please set COMPOSER_ENVIRONMENT'
   exit 1
  fi
  gcloud composer environments storage plugins import --environment "$COMPOSER_ENVIRONMENT" \
      --location 'europe-west1' \
      --source airflow/clash_plugin.py \
      --destination 'tooling/'
}

function task_run_example {
  local example=$1

  cd python
  pipenv run python setup.py develop
  pipenv run python ../examples/"$example"
}


cmd=$1
shift || true
case "$cmd" in
  init) task_init ;;
  lint) task_lint ;;
  unit-test) task_unit_test "$@" ;;
  integration-test) task_integration_test ;;
  format) task_format ;;
  package) task_package ;;
  release) task_release ;;
  deploy-airflow-plugin) task_deploy_airflow_plugin ;;
  example) task_run_example "$@" ;;
  *)     task_usage ;;
esac
