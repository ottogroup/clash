from unittest import mock
from pyclash import clash


def test_create_clash_job():
    script = """
        echo 'hello world'
    """

    job = clash.create_job('project-id', 'zone', script)
    job.run()
