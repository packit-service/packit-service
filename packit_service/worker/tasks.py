# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

import logging
from os import getenv
from typing import List, Optional

from celery import Task
from packit_service.celerizer import celery_app
from packit_service.constants import DEFAULT_RETRY_LIMIT, DEFAULT_RETRY_BACKOFF
from packit_service.utils import load_job_config, load_package_config
from packit_service.worker.build.babysit import check_copr_build
from packit_service.worker.handlers.abstract import TaskName
from packit_service.worker.handlers import (
    BugzillaHandler,
    CoprBuildEndHandler,
    CoprBuildStartHandler,
    KojiBuildReportHandler,
    DistGitCommitHandler,
    CoprBuildHandler,
    GithubAppInstallationHandler,
    KojiBuildHandler,
    ProposeDownstreamHandler,
    TestingFarmHandler,
    TestingFarmResultsHandler,
)
from packit_service.worker.jobs import SteveJobs
from packit_service.worker.result import TaskResults

logger = logging.getLogger(__name__)

# debug logs of these are super-duper verbose
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("github").setLevel(logging.WARNING)
logging.getLogger("kubernetes").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
# info is just enough
logging.getLogger("ogr").setLevel(logging.INFO)
# easier debugging
logging.getLogger("packit").setLevel(logging.DEBUG)
logging.getLogger("sandcastle").setLevel(logging.DEBUG)


class HandlerTaskWithRetry(Task):
    autoretry_for = (Exception,)
    retry_kwargs = {
        "max_retries": int(getenv("CELERY_RETRY_LIMIT", DEFAULT_RETRY_LIMIT))
    }
    retry_backoff = int(getenv("CELERY_RETRY_BACKOFF", DEFAULT_RETRY_BACKOFF))


@celery_app.task(name="task.steve_jobs.process_message", bind=True)
def process_message(
    self, event: dict, topic: str = None, source: str = None
) -> List[TaskResults]:
    """
    Base celery task for processing messages.

    :param event: event data
    :param topic: event topic
    :param source: event source
    :return: dictionary containing task results
    """
    return SteveJobs().process_message(event=event, topic=topic, source=source)


@celery_app.task(
    bind=True,
    name="task.babysit_copr_build",
    retry_backoff=60,  # retry again in 60s, 120s, 240s, 480s...
    retry_backoff_max=60 * 60 * 8,  # is 8 hours okay? gcc/kernel build really long
    max_retries=7,
)
def babysit_copr_build(self, build_id: int):
    """check status of a copr build and update it in DB"""
    if not check_copr_build(build_id=build_id):
        self.retry()


# tasks for running the handlers
@celery_app.task(name=TaskName.copr_build_start, base=HandlerTaskWithRetry)
def run_copr_build_start_handler(event: dict, package_config: dict, job_config: dict):
    handler = CoprBuildStartHandler(
        package_config=load_package_config(package_config),
        job_config=load_job_config(job_config),
        event=event,
    )
    return get_handlers_task_results(handler.run_job(), event)


@celery_app.task(name=TaskName.copr_build_end, base=HandlerTaskWithRetry)
def run_copr_build_end_handler(event: dict, package_config: dict, job_config: dict):
    handler = CoprBuildEndHandler(
        package_config=load_package_config(package_config),
        job_config=load_job_config(job_config),
        event=event,
    )
    return get_handlers_task_results(handler.run_job(), event)


@celery_app.task(
    name=TaskName.copr_build, base=HandlerTaskWithRetry, queue="long-running"
)
def run_copr_build_handler(event: dict, package_config: dict, job_config: dict):
    handler = CoprBuildHandler(
        package_config=load_package_config(package_config),
        job_config=load_job_config(job_config),
        event=event,
    )
    return get_handlers_task_results(handler.run_job(), event)


@celery_app.task(name=TaskName.installation, base=HandlerTaskWithRetry)
def run_installation_handler(event: dict, package_config: dict, job_config: dict):
    handler = GithubAppInstallationHandler(
        package_config=None, job_config=None, event=event
    )
    return get_handlers_task_results(handler.run_job(), event)


@celery_app.task(name=TaskName.testing_farm, base=HandlerTaskWithRetry)
def run_testing_farm_handler(
    event: dict,
    package_config: dict,
    job_config: dict,
    chroot: Optional[str] = None,
    build_id: Optional[int] = None,
):
    handler = TestingFarmHandler(
        package_config=load_package_config(package_config),
        job_config=load_job_config(job_config),
        event=event,
        chroot=chroot,
        build_id=build_id,
    )
    return get_handlers_task_results(handler.run_job(), event)


@celery_app.task(name=TaskName.testing_farm_results, base=HandlerTaskWithRetry)
def run_testing_farm_results_handler(
    event: dict, package_config: dict, job_config: dict
):
    handler = TestingFarmResultsHandler(
        package_config=load_package_config(package_config),
        job_config=load_job_config(job_config),
        event=event,
    )
    return get_handlers_task_results(handler.run_job(), event)


@celery_app.task(bind=True, name=TaskName.propose_downstream, base=HandlerTaskWithRetry)
def run_propose_downstream_handler(
    self, event: dict, package_config: dict, job_config: dict
):
    handler = ProposeDownstreamHandler(
        package_config=load_package_config(package_config),
        job_config=load_job_config(job_config),
        event=event,
        task=self,
    )
    return get_handlers_task_results(handler.run_job(), event)


@celery_app.task(
    name=TaskName.koji_build, base=HandlerTaskWithRetry, queue="long-running"
)
def run_koji_build_handler(event: dict, package_config: dict, job_config: dict):
    handler = KojiBuildHandler(
        package_config=load_package_config(package_config),
        job_config=load_job_config(job_config),
        event=event,
    )
    return get_handlers_task_results(handler.run_job(), event)


@celery_app.task(name=TaskName.distgit_commit, base=HandlerTaskWithRetry)
def run_distgit_commit_handler(event: dict, package_config: dict, job_config: dict):
    handler = DistGitCommitHandler(
        package_config=load_package_config(package_config),
        job_config=load_job_config(job_config),
        event=event,
    )
    return get_handlers_task_results(handler.run_job(), event)


@celery_app.task(name=TaskName.bugzilla, base=HandlerTaskWithRetry)
def run_bugzilla_handler(event: dict, package_config: dict, job_config: dict):
    handler = BugzillaHandler(
        package_config=load_package_config(package_config),
        job_config=load_job_config(job_config),
        event=event,
    )
    return get_handlers_task_results(handler.run_job(), event)


@celery_app.task(name=TaskName.koji_build_report, base=HandlerTaskWithRetry)
def run_koji_build_report_handler(event: dict, package_config: dict, job_config: dict):
    handler = KojiBuildReportHandler(
        package_config=load_package_config(package_config),
        job_config=load_job_config(job_config),
        event=event,
    )
    return get_handlers_task_results(handler.run_job(), event)


def get_handlers_task_results(results: dict, event: dict) -> dict:
    # include original event to provide more info
    return {"job": results, "event": event}
