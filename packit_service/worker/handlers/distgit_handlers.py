# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

"""
This file defines classes for job handlers specific for Fedmsg events
"""

import logging
from os import getenv

from celery import Task
from packit.api import PackitAPI
from packit.config import JobConfig, JobType
from packit.config.aliases import get_branches
from packit.config.package_config import PackageConfig
from packit.local_project import LocalProject

from packit_service import sentry_integration
from packit_service.constants import (
    DEFAULT_RETRY_LIMIT,
    FILE_DOWNLOAD_FAILURE,
    MSG_RETRIGGER,
)
from packit_service.service.events import (
    DistGitCommitEvent,
    ReleaseEvent,
    IssueCommentEvent,
    IssueCommentGitlabEvent,
)
from packit_service.worker.handlers.abstract import (
    JobHandler,
    TaskName,
    configured_as,
    reacts_to,
    add_topic,
    FedmsgHandler,
    run_for_comment,
)
from packit_service.worker.result import TaskResults

logger = logging.getLogger(__name__)


@add_topic
@configured_as(job_type=JobType.sync_from_downstream)
@reacts_to(event=DistGitCommitEvent)
class DistGitCommitHandler(FedmsgHandler):
    """Sync new specfile changes to upstream after a new git push in the dist-git."""

    topic = "org.fedoraproject.prod.git.receive"
    task_name = TaskName.distgit_commit

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
        self.branch = event.get("branch")
        self.dg_branch = event.get("dg_branch")

    def run(self) -> TaskResults:
        self.upstream_local_project = LocalProject(
            git_project=self.project,
            working_dir=self.service_config.command_handler_work_dir,
        )
        self.api = PackitAPI(
            self.service_config,
            self.job_config,
            upstream_local_project=self.local_project,
            stage=self.service_config.use_stage(),
        )
        # rev is a commit
        # we use branch on purpose so we get the latest thing
        # TODO: check if rev is HEAD on {branch}, warn then?
        self.api.sync_from_downstream(
            dist_git_branch=self.dg_branch,
            upstream_branch=self.branch,
            sync_only_specfile=True,
        )
        return TaskResults(success=True, details={})


@configured_as(job_type=JobType.propose_downstream)
@run_for_comment(command="propose-downstream")
@run_for_comment(command="propose-update")  # deprecated
@reacts_to(event=ReleaseEvent)
@reacts_to(event=IssueCommentEvent)
@reacts_to(event=IssueCommentGitlabEvent)
class ProposeDownstreamHandler(JobHandler):
    task_name = TaskName.propose_downstream

    def __init__(
        self,
        package_config: PackageConfig,
        job_config: JobConfig,
        event: dict,
        task: Task = None,
    ):
        super().__init__(
            package_config=package_config,
            job_config=job_config,
            event=event,
        )
        self.task = task

    def run(self) -> TaskResults:
        """
        Sync the upstream release to dist-git as a pull request.
        """

        self.local_project = LocalProject(
            git_project=self.project,
            working_dir=self.service_config.command_handler_work_dir,
        )

        self.api = PackitAPI(
            self.service_config,
            self.job_config,
            self.local_project,
            stage=self.service_config.use_stage(),
        )

        errors = {}
        default_dg_branch = self.api.dg.local_project.git_project.default_branch
        for branch in get_branches(
            *self.job_config.metadata.dist_git_branches, default=default_dg_branch
        ):
            try:
                self.api.sync_release(dist_git_branch=branch, tag=self.data.tag_name)
            except Exception as ex:
                # the archive has not been uploaded to PyPI yet
                if FILE_DOWNLOAD_FAILURE in str(ex):
                    # retry for the archive to become available
                    logger.info(f"We were not able to download the archive: {ex}")
                    # when the task hits max_retries, it raises MaxRetriesExceededError
                    # and the error handling code would be never executed
                    retries = self.task.request.retries
                    if retries < int(getenv("CELERY_RETRY_LIMIT", DEFAULT_RETRY_LIMIT)):
                        # will retry in: 1m and then again in another 2m
                        delay = 60 * 2 ** retries
                        logger.info(
                            f"Will retry for the {retries + 1}. time in {delay}s."
                        )
                        # throw=False so that exception is not raised and task
                        # is not retried also automatically
                        self.task.retry(exc=ex, countdown=delay, throw=False)
                        return TaskResults(
                            success=False,
                            details={
                                "msg": "Not able to download archive. Task will be retried."
                            },
                        )
                sentry_integration.send_to_sentry(ex)
                errors[branch] = str(ex)

        if errors:
            branch_errors = ""
            for branch, err in sorted(
                errors.items(), key=lambda branch_error: branch_error[0]
            ):
                err_without_new_lines = err.replace("\n", " ")
                branch_errors += f"| `{branch}` | `{err_without_new_lines}` |\n"

            msg_retrigger = MSG_RETRIGGER.format(
                job="update", command="propose-downstream", place="issue"
            )
            body_msg = (
                f"Packit failed on creating pull-requests in dist-git:\n\n"
                f"| dist-git branch | error |\n"
                f"| --------------- | ----- |\n"
                f"{branch_errors}\n\n"
                f"{msg_retrigger}\n"
            )

            self.project.create_issue(
                title=f"[packit] Propose downstream failed for release {self.data.tag_name}",
                body=body_msg,
            )

            return TaskResults(
                success=False,
                details={"msg": "Propose downstream failed.", "errors": errors},
            )

        return TaskResults(success=True, details={})
