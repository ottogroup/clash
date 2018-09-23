import logging
from pyclash import clash

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class ClashOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        script,
        job_config,
        env_vars={},
        gcs_target={},
        gcs_mounts={},
        *args,
        **kwargs
    ):
        job = clash.Job(job_config=job_config)

        self.script = script
        self.env_vars = env_vars
        self.gcs_target = gcs_target
        self.gcs_mounts = gcs_mounts

        super(ClashOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Running Clash Job...")
        job.run(
            self.script,
            env_vars=self.env_vars,
            gcs_target=self.gcs_target,
            gcs_mounts=self.gcs_mounts,
        )


class ClashPlugin(AirflowPlugin):
    name = "clash_plugin"
    operators = [ClashOperator]
