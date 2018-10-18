import logging
from pyclash import clash

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class ClashOperator(BaseOperator):
    template_fields = ('cmd', 'cmd_file', 'env')

    @apply_defaults
    def __init__(
        self,
        job_config,
        cmd=None,
        cmd_file=None,
        name_prefix=None,
        env_vars={},
        gcs_target={},
        gcs_mounts={},
        *args,
        **kwargs
    ):
        self.job = clash.Job(job_config=job_config, name_prefix=name_prefix)

        self.cmd = cmd
        self.cmd_file = cmd_file
        self.env_vars = env_vars
        self.gcs_target = gcs_target
        self.gcs_mounts = gcs_mounts

        super(ClashOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Running Clash Job...")
        if self.cmd_file:
            self.job.run_file(
                self.cmd_file,
                env_vars=self.env_vars,
                gcs_target=self.gcs_target,
                gcs_mounts=self.gcs_mounts,
            )
        elif self.cmd:
            self.job.run(
                self.cmd,
                env_vars=self.env_vars,
                gcs_target=self.gcs_target,
                gcs_mounts=self.gcs_mounts,
            )
        else:
            raise AirflowException("No command was given")

        with clash.StackdriverLogsReader(self.job, log_func=self.log.info):
            result = self.job.attach()

        if result["status"] != 0:
            raise AirflowException(
                "The command failed with status code {}".format(result["status"])
            )

class ClashGroupOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        name,
        job_factory,
        runtime_specs,
        *args,
        **kwargs
    ):
        self.group = clash.JobGroup(name=name, job_factory=job_factory)
        for spec in runtime_specs:
            self.group.add_job(spec)
        super(ClashGroupOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.group.run()
        with clash.StackdriverLogsReader(self.group, log_func=self.log.info):
            result = self.group.wait()

        # workaround: wait for the instances to clean up resources
        time.sleep(180)
        group.clean_up() # clean up remaining resources

        if not result:
            raise AirflowException(
                "The command failed"
            )


class ClashPlugin(AirflowPlugin):
    name = "clash_plugin"
    operators = [ClashOperator]
