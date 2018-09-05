#!/usr/bin/env bash

set -e

function task_usage {
  echo 'Usage: ./run.sh lint | build | test | format'
  exit 1
}

function task_lint {
  shellcheck run.sh
}

function task_format {
  pipenv run autopep8 --in-place --recursive python
}

function task_format {
  shellcheck run.sh
}

function task_test {
  cd python
  pipenv run python setup.py develop
  pipenv run pytest
}


cmd=$1
shift || true
case "$cmd" in
  lint) task_lint ;;
  test) task_test ;;
  format) task_format ;;
  *)     task_usage ;;
esac
