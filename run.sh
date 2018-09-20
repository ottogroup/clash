#!/usr/bin/env bash

set -e

function task_usage {
  echo 'Usage: ./run.sh init | lint | build | test | clash | format | package | release'
  exit 1
}

function task_init {
  pipenv install
}

function task_lint {
  exit 0
}

function task_format {
  pipenv run black python
}

function task_test {
  cd python
  pipenv run python setup.py develop
  pipenv run pytest "$@"
}

function task_clash {
  cd python
  pipenv run python setup.py develop &> setup.log
  pipenv run clash "$@"
}

function task_package {
  cd python
  pipenv run python setup.py sdist bdist_wheel
}

function task_release {
  cd python
  pipenv run twine upload dist/*
}


cmd=$1
shift || true
case "$cmd" in
  init) task_init ;;
  lint) task_lint ;;
  test) task_test "$@" ;;
  clash) task_clash "$@" ;;
  format) task_format ;;
  package) task_package ;;
  release) task_release ;;
  *)     task_usage ;;
esac
