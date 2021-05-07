# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

"""
This file defines generic job handler
"""
import enum
import logging
import shutil
from collections import defaultdict
from datetime import datetime
from os import getenv
from pathlib import Path
from typing import Dict, Optional, Set, Type

from celery import signature
from celery.canvas import Signature

from ogr.abstract import GitProject
from packit.api import PackitAPI
from packit.config import JobConfig, JobType, PackageConfig
from packit.constants import DATETIME_FORMAT
from packit.local_project import LocalProject
from packit_service.config import ServiceConfig
from packit_service.models import (
    AbstractTriggerDbType,
)
from packit_service.sentry_integration import push_scope_to_sentry
from packit_service.service.events import Event, EventData
from packit_service.utils import dump_job_config, dump_package_config
from packit_service.worker.result import TaskResults

logger = logging.getLogger(__name__)

MAP_JOB_TYPE_TO_HANDLER: Dict[JobType, Set[Type["JobHandler"]]] = defaultdict(set)
MAP_REQUIRED_JOB_TYPE_TO_HANDLER: Dict[JobType, Set[Type["JobHandler"]]] = defaultdict(
    set
)
SUPPORTED_EVENTS_FOR_HANDLER: Dict[
    Type["JobHandler"], Set[Type["Event"]]
] = defaultdict(set)
MAP_COMMENT_TO_HANDLER: Dict[str, Set[Type["JobHandler"]]] = defaultdict(set)

PROCESSED_FEDMSG_TOPICS = []


def configured_as(job_type: JobType):
    """
    [class decorator]
    Specify a job_type which we want to use this handler for.
    In other words, what job-config in the configuration file
    is compatible with this handler.

    Example:
    ```
    @configured_as(job_type=JobType.propose_downstream)
    class ProposeDownstreamHandler(JobHandler):
    ```

    Multiple handlers can match one job_type.
    (e.g. CoprBuildHandler and CoprBuildEndHandler both uses copr_build)
    The handler needs to match the event type by using @reacts_to decorator.

    Multiple decorators can be applied.
    E.g. CoprBuildHandler uses both copr_build and build:
    ```
    @configured_as(job_type=JobType.copr_build)
    @configured_as(job_type=JobType.build)
    class CoprBuildHandler(JobHandler):
    ```
    """

    def _add_to_mapping(kls: Type["JobHandler"]):
        MAP_JOB_TYPE_TO_HANDLER[job_type].add(kls)
        return kls

    return _add_to_mapping


def required_for(job_type: JobType):
    """
    [class decorator]
    Specify a job_type for which this handler is the prerequisite.
    E.g. for test, we need to run build first.

    If there is a matching job_type defined by @configured_as,
    we don't use the decorated handler with the job-config using this job_type.
    If there is none, we use the job-config with this job_type.

    Example:
        - When there is a build and test defined, we run build only once
          with the build job-config.
        - When there is only test defined,
          we run build with the test job-configuration.

    ```
    @configured_as(job_type=JobType.copr_build)
    @configured_as(job_type=JobType.build)
    @required_for(job_type=JobType.tests)
    class CoprBuildHandler(JobHandler):
    ```
    """

    def _add_to_mapping(kls: Type["JobHandler"]):
        MAP_REQUIRED_JOB_TYPE_TO_HANDLER[job_type].add(kls)
        return kls

    return _add_to_mapping


def reacts_to(event: Type["Event"]):
    """
    [class decorator]
    Specify an event for which we want to use this handler.
    Matching is done via isinstance so you can use some abstract class as well.

    Multiple decorators are allowed.

    Example:
    ```
    @reacts_to(ReleaseEvent)
    @reacts_to(PullRequestGithubEvent)
    @reacts_to(PushGitHubEvent)
    class CoprBuildHandler(JobHandler):
    ```
    """

    def _add_to_mapping(kls: Type["JobHandler"]):
        SUPPORTED_EVENTS_FOR_HANDLER[kls].add(event)
        return kls

    return _add_to_mapping


def run_for_comment(command: str):
    """
    [class decorator]
    Specify a command for which we want to run a handler.
    e.g. for `/packit command` we need to add `command`

    Multiple decorators are allowed.

    Don't forget to specify valid comment events
    using @reacts_to decorator.

    Example:
    ```
    @configured_as(job_type=JobType.propose_downstream)
    @run_for_comment(command="propose-downstream")
    @run_for_comment(command="propose-update")
    @reacts_to(event=ReleaseEvent)
    @reacts_to(event=IssueCommentEvent)
    class ProposeDownstreamHandler(JobHandler):
    ```
    """

    def _add_to_mapping(kls: Type["JobHandler"]):
        MAP_COMMENT_TO_HANDLER[command].add(kls)
        return kls

    return _add_to_mapping


def add_topic(kls: Type["FedmsgHandler"]):
    if issubclass(kls, FedmsgHandler):
        PROCESSED_FEDMSG_TOPICS.append(kls.topic)
    return kls


class TaskName(str, enum.Enum):
    copr_build_start = "task.run_copr_build_start_handler"
    copr_build_end = "task.run_copr_build_end_handler"
    copr_build = "task.run_copr_build_handler"
    installation = "task.run_installation_handler"
    testing_farm = "task.run_testing_farm_handler"
    testing_farm_results = "task.run_testing_farm_results_handler"
    propose_downstream_comment = "task.run_propose_downstream_comment_handler"
    propose_downstream = "task.run_propose_downstream_handler"
    koji_build = "task.run_koji_build_handler"
    distgit_commit = "task.run_distgit_commit_handler"
    bugzilla = "task.run_bugzilla_handler"
    koji_build_report = "task.run_koji_build_report_handler"


class Handler:
    api: Optional[PackitAPI] = None
    local_project: Optional[LocalProject] = None
    _service_config: Optional[ServiceConfig] = None

    @property
    def service_config(self) -> ServiceConfig:
        if not self._service_config:
            self._service_config = ServiceConfig.get_service_config()
        return self._service_config

    @property
    def project(self) -> Optional[GitProject]:
        return None

    def run(self) -> TaskResults:
        raise NotImplementedError("This should have been implemented.")

    def get_tag_info(self) -> dict:
        tags = {"handler": self.__class__.__name__}
        # repository info for easier filtering events that were grouped based on event type
        if self.project:
            tags.update(
                {
                    "repository": self.project.repo,
                    "namespace": self.project.namespace,
                }
            )
        return tags

    def run_n_clean(self) -> TaskResults:
        try:
            with push_scope_to_sentry() as scope:
                for k, v in self.get_tag_info().items():
                    scope.set_tag(k, v)
                return self.run()
        finally:
            self.clean()

    def _clean_workplace(self):
        # clean only when we are in k8s for sure
        if not getenv("KUBERNETES_SERVICE_HOST"):
            logger.debug("This is not a kubernetes pod, won't clean.")
            return
        logger.debug("Removing contents of the PV.")
        p = Path(self.service_config.command_handler_work_dir)
        # Do not clean dir if does not exist
        if not p.is_dir():
            logger.debug(
                f"Directory {self.service_config.command_handler_work_dir!r} does not exist."
            )
            return

        # remove everything in the volume, but not the volume dir
        dir_items = list(p.iterdir())
        if dir_items:
            logger.info("Volume is not empty.")
            logger.debug(f"Content: {[g.name for g in dir_items]}")
        for item in dir_items:
            # symlink pointing to a dir is also a dir and a symlink
            if item.is_symlink() or item.is_file():
                item.unlink()
            else:
                shutil.rmtree(item)

    def pre_check(self) -> bool:
        """
        Implement this method for those handlers, where you want to check if the properties are
        correct. If this method returns False during runtime, execution of service code is skipped.

        :return: False if we can skip the job execution.
        """
        return True

    def clean(self):
        """clean up the mess once we're done"""
        logger.info("Cleaning up the mess.")
        if self.api:
            self.api.clean()
        self._clean_workplace()


class JobHandler(Handler):
    """Generic interface to handle different type of inputs"""

    task_name: TaskName

    def __init__(
        self,
        package_config: PackageConfig,
        job_config: JobConfig,
        event: dict,
    ):
        # build helper needs package_config to resolve dependencies b/w tests and build jobs
        self.package_config = package_config
        # always use job_config to pick up values, use package_config only for package_config.jobs
        self.job_config = job_config
        self.data = EventData.from_event_dict(event)

        self._db_trigger: Optional[AbstractTriggerDbType] = None
        self._project: Optional[GitProject] = None
        self._clean_workplace()

    @property
    def project(self) -> Optional[GitProject]:
        if not self._project and self.data.project_url:
            self._project = self.service_config.get_project(url=self.data.project_url)
        return self._project

    @classmethod
    def get_all_subclasses(cls) -> Set[Type["JobHandler"]]:
        return set(cls.__subclasses__()).union(
            [s for c in cls.__subclasses__() for s in c.get_all_subclasses()]
        )

    def run_job(self):
        """
        If pre-check succeeds, run the job for the specific handler.
        :return: Dict [str, TaskResults]
        """
        job_type = (
            self.job_config.type.value if self.job_config else self.task_name.value
        )
        logger.debug(f"Running handler {str(self)} for {job_type}")
        job_results: Dict[str, TaskResults] = {}
        current_time = datetime.now().strftime(DATETIME_FORMAT)
        result_key = f"{job_type}-{current_time}"
        job_results[result_key] = self.run_n_clean()
        logger.debug("Job finished!")

        for result in job_results.values():
            if not (result and result["success"]):
                logger.error(result["details"]["msg"])

        return job_results

    @classmethod
    def get_signature(cls, event: Event, job: Optional[JobConfig]) -> Signature:
        """
        Get the signature of a Celery task which will run the handler.
        https://docs.celeryproject.org/en/stable/userguide/canvas.html#signatures
        :param event: event which triggered the task
        :param job: job to process
        """
        logger.debug(f"Getting signature of a Celery task {cls.task_name}.")
        return signature(
            cls.task_name.value,
            kwargs={
                "package_config": dump_package_config(event.package_config),
                "job_config": dump_job_config(job),
                "event": event.get_dict(),
            },
        )

    def run(self) -> TaskResults:
        raise NotImplementedError("This should have been implemented.")


class FedmsgHandler(JobHandler):
    """Handlers for events from fedmsg"""

    topic: str

    def __init__(
        self,
        package_config: PackageConfig,
        job_config: JobConfig,
        event: dict,
    ):
        super().__init__(
            package_config=package_config,
            job_config=job_config,
            event=event,
        )
        self._pagure_service = None

    def run(self) -> TaskResults:
        raise NotImplementedError("This should have been implemented.")
