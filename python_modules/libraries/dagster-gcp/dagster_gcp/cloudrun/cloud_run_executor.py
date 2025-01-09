import json
import logging
import os
import uuid
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, cast

from dagster import (
    DagsterInvariantViolationError,
    Field,
    IntSource,
    Noneable,
    Permissive,
    StringSource,
    _check as check,
    executor,
)
from dagster._annotations import experimental
from dagster._core.definitions.executor_definition import multiple_process_executor_requirements
from dagster._core.definitions.metadata import MetadataValue
from dagster._core.events import DagsterEvent, EngineEventData
from dagster._core.execution.retries import RetryMode, get_retries_config
from dagster._core.execution.tags import get_tag_concurrency_limits_config
from dagster._core.executor.base import Executor
from dagster._core.executor.init import InitExecutorContext
from dagster._core.executor.step_delegating import (
    CheckStepHealthResult,
    StepDelegatingExecutor,
    StepHandler,
    StepHandlerContext,
)
from dagster._core.origin import JobPythonOrigin
from dagster._core.storage.tags import DOCKER_IMAGE_TAG
from google.cloud import run_v2
from google.cloud.run_v2.types import Job, Container, ExecutionTemplate, TaskTemplate

DEFAULT_STEP_TASK_RETRIES = "5"

_CLOUD_RUN_EXECUTOR_CONFIG_SCHEMA = {
    "project_id": Field(
        StringSource,
        is_required=True,
        description="The GCP project ID where Cloud Run jobs will be created.",
    ),
    "region": Field(
        StringSource,
        is_required=True,
        description="The GCP region where Cloud Run jobs will be created.",
    ),
    "job_template": Field(
        Permissive({}),
        is_required=False,
        description="Additional configuration for the Cloud Run job.",
    ),
    "container_template": Field(
        Permissive({}),
        is_required=False,
        description="Additional configuration for the container.",
    ),
    "execution_template": Field(
        Permissive({}),
        is_required=False,
        description="Additional configuration for the execution.",
    ),
    "cpu": Field(IntSource, is_required=False),
    "memory": Field(StringSource, is_required=False),
    "retries": get_retries_config(),
    "max_concurrent": Field(
        IntSource,
        is_required=False,
        description=(
            "Limit on the number of jobs that will run concurrently within the scope "
            "of a Dagster run. Note that this limit is per run, not global."
        ),
    ),
    "tag_concurrency_limits": get_tag_concurrency_limits_config(),
}

@executor(
    name="cloud_run",
    config_schema=_CLOUD_RUN_EXECUTOR_CONFIG_SCHEMA,
    requirements=multiple_process_executor_requirements(),
)
@experimental
def cloud_run_executor(init_context: InitExecutorContext) -> Executor:
    """Executor which launches steps as Cloud Run jobs.

    To use the cloud_run_executor, set it as the executor_def when defining a job:

    .. code-block:: python

        @job(
            executor_def=cloud_run_executor.configured({
                "project_id": "my-project",
                "region": "us-central1",
                "cpu": 1,
                "memory": "2Gi",
            })
        )
        def my_job():
            ...

    Args:
        init_context (InitExecutorContext): The context for initializing the executor.

    Returns:
        Executor: The configured Cloud Run executor.
    """
    exc_cfg = init_context.executor_config

    return StepDelegatingExecutor(
        CloudRunStepHandler(
            project_id=check.str_param(exc_cfg["project_id"], "project_id"),
            region=check.str_param(exc_cfg["region"], "region"),
            job_template=exc_cfg.get("job_template"), # type: ignore
            container_template=exc_cfg.get("container_template"), # type: ignore
            execution_template=exc_cfg.get("execution_template"), # type: ignore
            cpu=exc_cfg.get("cpu"), # type: ignore
            memory=exc_cfg.get("memory"), # type: ignore
        ),
        retries=RetryMode.from_config(exc_cfg["retries"]), # type: ignore
        max_concurrent=check.opt_int_elem(exc_cfg, "max_concurrent"),
        tag_concurrency_limits=check.opt_list_elem(exc_cfg, "tag_concurrency_limits"),
        should_verify_step=True,
    )

class CloudRunStepHandler(StepHandler):
    def __init__(
        self,
        project_id: str,
        region: str,
        job_template: Optional[Dict[str, Any]] = None,
        container_template: Optional[Dict[str, Any]] = None,
        execution_template: Optional[Dict[str, Any]] = None,
        cpu: Optional[int] = None,
        memory: Optional[str] = None,
    ):
        self._project_id = check.str_param(project_id, "project_id")
        self._region = check.str_param(region, "region")
        self._job_template = check.opt_dict_param(job_template, "job_template")
        self._container_template = check.opt_dict_param(container_template, "container_template")
        self._execution_template = check.opt_dict_param(execution_template, "execution_template")
        self._cpu = check.opt_int_param(cpu, "cpu")
        self._memory = check.opt_str_param(memory, "memory")
        self._client = run_v2.JobsClient()
        self._launched_jobs = {}

    @property
    def name(self) -> str:
        return "cloud_run_step_handler"

    def _get_image(self, step_handler_context: StepHandlerContext) -> str:
        """Get the Docker image to use for the step."""
        print("step_handler_context.dagster_run.tags")
        print(step_handler_context.dagster_run.tags)
        print("step_handler_context.dagster_run.job_code_origin")
        print(step_handler_context.dagster_run.job_code_origin)
        print("DOCKER_IMAGE_TAG", "USER_PROVIDED_cloud_run_executor_image")
        # First try the job code origin
        image = cast(
            JobPythonOrigin, step_handler_context.dagster_run.job_code_origin
        ).repository_origin.container_image

        print("image 1", image)

        # Then try the run tags
        if not image:
            image = step_handler_context.dagster_run.tags.get("USER_PROVIDED_container_image")

        print("image 2", image)
        if not image:
            raise DagsterInvariantViolationError(
                "No Docker image specified. Set the container_image in the repository definition "
                "or use the DOCKER_IMAGE_TAG in run tags."
            )

        return image

    def _get_job_name(self, step_handler_context: StepHandlerContext, step_key: str) -> str:
        run_id = step_handler_context.dagster_run.run_id
        unique_suffix = str(uuid.uuid4())[:8]
        sanitized_step_key = step_key.lower().replace("_", "-")
        return f"dagster-{sanitized_step_key}-{run_id[:8]}-{unique_suffix}"

    def _get_step_id(self, step_handler_context: StepHandlerContext, step_key: str) -> str:
        if step_handler_context.execute_step_args.known_state:
            retry_count = step_handler_context.execute_step_args.known_state.get_retry_state().get_attempt_count(
                step_key
            )
        else:
            retry_count = 0
        return f"{step_key}-{retry_count}"

    def _get_step_key(self, step_handler_context: StepHandlerContext) -> str:
        step_keys_to_execute = cast(
            List[str], step_handler_context.execute_step_args.step_keys_to_execute
        )
        assert len(step_keys_to_execute) == 1, "Launching multiple steps is not currently supported"
        return step_keys_to_execute[0]

    def launch_step(self, step_handler_context: StepHandlerContext) -> Iterator[DagsterEvent]:
        print("step handler context", step_handler_context.__dict__)
        step_key = self._get_step_key(step_handler_context)
        print("step key", step_key)
        image = self._get_image(step_handler_context)
        container = Container(
            image=image,
            args=step_handler_context.execute_step_args.get_command_args(),
            env=[{"name": k, "value": v} for k, v in step_handler_context.execute_step_args.get_command_env()],
            **self._container_template or {},
        )

        if self._cpu:
            container.resources.limits["cpu"] = str(self._cpu)
        if self._memory:
            container.resources.limits["memory"] = self._memory

        task_template = TaskTemplate(
            containers=[container],
            **self._execution_template or {},
        )

        execution_template = ExecutionTemplate(
            template=task_template,
        )

        job = Job(
            name=self._get_job_name(step_handler_context, step_key),
            template=execution_template,
            **self._job_template or {},
        )

        parent = f"projects/{self._project_id}/locations/{self._region}"
        operation = self._client.create_job(
            parent=parent,
            job=job,
            job_id=job.name,
        )
        
        created_job = operation.result()

        step_id = self._get_step_id(step_handler_context, step_key)
        self._launched_jobs[step_id] = created_job.name

        yield DagsterEvent.step_worker_starting(
            step_handler_context.get_step_context(step_key),
            message=f'Executing step "{step_key}" in Cloud Run job.',
            metadata={
                "Job Name": MetadataValue.text(created_job.name),
            },
        )

    def check_step_health(self, step_handler_context: StepHandlerContext) -> CheckStepHealthResult:
        step_key = self._get_step_key(step_handler_context)
        step_id = self._get_step_id(step_handler_context, step_key)

        try:
            job_name = self._launched_jobs[step_id]
        except KeyError:
            return CheckStepHealthResult.unhealthy(
                f"Job name for step {step_key} could not be found in executor's job map."
            )

        try:
            job = self._client.get_job(name=job_name)
            latest_execution = self._client.get_execution(name=f"{job_name}/executions/-")

            if latest_execution.completed_at:
                if latest_execution.status.state == run_v2.Execution.State.SUCCEEDED:
                    return CheckStepHealthResult.healthy()
                else:
                    return CheckStepHealthResult.unhealthy(
                        f"Cloud Run job failed with status: {latest_execution.status.message}"
                    )
            return CheckStepHealthResult.healthy()

        except Exception as e:
            return CheckStepHealthResult.unhealthy(f"Error checking Cloud Run job status: {str(e)}")

    def terminate_step(self, step_handler_context: StepHandlerContext) -> Iterator[DagsterEvent]:
        step_key = self._get_step_key(step_handler_context)
        step_id = self._get_step_id(step_handler_context, step_key)

        try:
            job_name = self._launched_jobs[step_id]
        except KeyError:
            raise DagsterInvariantViolationError(
                f"Job name for step {step_key} could not be found in executor's job map."
            )

        try:
            self._client.delete_job(name=job_name)
            yield DagsterEvent.engine_event(
                step_handler_context.get_step_context(step_key),
                message=f"Stopping job {job_name} for step",
                event_specific_data=EngineEventData(),
            )
        except Exception as e:
            logging.error(f"Error terminating Cloud Run job {job_name}: {str(e)}") 