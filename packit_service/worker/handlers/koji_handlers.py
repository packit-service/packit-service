# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

"""
This file defines classes for job handlers specific for Fedmsg events
"""

import logging
from datetime import datetime
from typing import Optional

from ogr.abstract import CommitStatus, GitProject
from packit.config import (
    JobConfig,
    JobType,
)
from packit.config.package_config import PackageConfig

from packit_service.constants import (
    KOJI_PRODUCTION_BUILDS_ISSUE,
    PERMISSIONS_ERROR_WRITE_OR_ADMIN,
)
from packit_service.constants import (
    KojiBuildState,
)
from packit_service.models import AbstractTriggerDbType, KojiBuildModel
from packit_service.service.events import (
    KojiBuildEvent,
    MergeRequestCommentGitlabEvent,
    MergeRequestGitlabEvent,
    PullRequestCommentGithubEvent,
    PullRequestCommentPagureEvent,
    PullRequestGithubEvent,
    PushGitHubEvent,
    PushGitlabEvent,
    PushPagureEvent,
    ReleaseEvent,
)
from packit_service.service.urls import (
    get_koji_build_info_url,
)
from packit_service.worker.build.koji_build import KojiBuildJobHelper
from packit_service.worker.handlers.abstract import (
    JobHandler,
    TaskName,
    configured_as,
    reacts_to,
    run_for_comment,
    add_topic,
    FedmsgHandler,
)
from packit_service.worker.result import TaskResults

logger = logging.getLogger(__name__)


@configured_as(job_type=JobType.production_build)
@run_for_comment(command="production-build")
@reacts_to(ReleaseEvent)
@reacts_to(PullRequestGithubEvent)
@reacts_to(PushGitHubEvent)
@reacts_to(PushGitlabEvent)
@reacts_to(MergeRequestGitlabEvent)
@reacts_to(PullRequestCommentGithubEvent)
@reacts_to(MergeRequestCommentGitlabEvent)
@reacts_to(PullRequestCommentPagureEvent)
class KojiBuildHandler(JobHandler):
    task_name = TaskName.koji_build

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

        # lazy property
        self._koji_build_helper: Optional[KojiBuildJobHelper] = None
        self._project: Optional[GitProject] = None

    @property
    def koji_build_helper(self) -> KojiBuildJobHelper:
        if not self._koji_build_helper:
            self._koji_build_helper = KojiBuildJobHelper(
                service_config=self.service_config,
                package_config=self.package_config,
                project=self.project,
                metadata=self.data,
                db_trigger=self.data.db_trigger,
                job_config=self.job_config,
            )
        return self._koji_build_helper

    def run(self) -> TaskResults:
        return self.koji_build_helper.run_koji_build()

    def pre_check(self) -> bool:
        if self.data.event_type in (
            PushGitHubEvent.__name__,
            PushGitlabEvent.__name__,
            PushPagureEvent.__name__,
        ):
            configured_branch = self.koji_build_helper.job_build_branch
            if self.data.git_ref != configured_branch:
                logger.info(
                    f"Skipping build on '{self.data.git_ref}'. "
                    f"Push configured only for '{configured_branch}'."
                )
                return False

        if self.data.event_type == PullRequestGithubEvent.__name__:
            user_can_merge_pr = self.project.can_merge_pr(self.data.user_login)
            if not (
                user_can_merge_pr or self.data.user_login in self.service_config.admins
            ):
                self.koji_build_helper.report_status_to_all(
                    description=PERMISSIONS_ERROR_WRITE_OR_ADMIN,
                    state=CommitStatus.failure,
                )
                return False

        if not self.koji_build_helper.is_scratch:
            msg = "Non-scratch builds not possible from upstream."
            self.koji_build_helper.report_status_to_all(
                description=msg,
                state=CommitStatus.error,
                url=KOJI_PRODUCTION_BUILDS_ISSUE,
            )
            return False

        return True


@add_topic
@configured_as(job_type=JobType.production_build)
@reacts_to(event=KojiBuildEvent)
class KojiBuildReportHandler(FedmsgHandler):
    topic = "org.fedoraproject.prod.buildsys.task.state.change"
    task_name = TaskName.koji_build_report

    def __init__(
        self, package_config: PackageConfig, job_config: JobConfig, event: dict
    ):
        super().__init__(
            package_config=package_config,
            job_config=job_config,
            event=event,
        )
        self.koji_event: KojiBuildEvent = KojiBuildEvent.from_event_dict(event)
        self._db_trigger: Optional[AbstractTriggerDbType] = None
        self._build: Optional[KojiBuildModel] = None

    @property
    def build(self) -> Optional[KojiBuildModel]:
        if not self._build:
            self._build = KojiBuildModel.get_by_build_id(
                build_id=str(self.koji_event.build_id)
            )
        return self._build

    @property
    def db_trigger(self) -> Optional[AbstractTriggerDbType]:
        if not self._db_trigger and self.build:
            self._db_trigger = self.build.get_trigger_object()
        return self._db_trigger

    def run(self):
        build = KojiBuildModel.get_by_build_id(build_id=str(self.koji_event.build_id))

        if not build:
            msg = f"Koji build {self.koji_event.build_id} not found in the database."
            logger.warning(msg)
            return TaskResults(success=False, details={"msg": msg})

        logger.debug(
            f"Build on {build.target} in koji changed state "
            f"from {self.koji_event.old_state} to {self.koji_event.state}."
        )

        build.set_build_start_time(
            datetime.utcfromtimestamp(self.koji_event.start_time)
            if self.koji_event.start_time
            else None
        )

        build.set_build_finished_time(
            datetime.utcfromtimestamp(self.koji_event.completion_time)
            if self.koji_event.completion_time
            else None
        )

        url = get_koji_build_info_url(build.id)
        build_job_helper = KojiBuildJobHelper(
            service_config=self.service_config,
            package_config=self.package_config,
            project=self.project,
            metadata=self.data,
            db_trigger=self.db_trigger,
            job_config=self.job_config,
        )

        if self.koji_event.state == KojiBuildState.open:
            build.set_status("pending")
            build_job_helper.report_status_to_all_for_chroot(
                description="RPM build is in progress...",
                state=CommitStatus.pending,
                url=url,
                chroot=build.target,
            )
        elif self.koji_event.state == KojiBuildState.closed:
            build.set_status("success")
            build_job_helper.report_status_to_all_for_chroot(
                description="RPMs were built successfully.",
                state=CommitStatus.success,
                url=url,
                chroot=build.target,
            )
        elif self.koji_event.state == KojiBuildState.failed:
            build.set_status("failed")
            build_job_helper.report_status_to_all_for_chroot(
                description="RPMs failed to be built.",
                state=CommitStatus.failure,
                url=url,
                chroot=build.target,
            )
        elif self.koji_event.state == KojiBuildState.canceled:
            build.set_status("error")
            build_job_helper.report_status_to_all_for_chroot(
                description="RPMs build was canceled.",
                state=CommitStatus.error,
                url=url,
                chroot=build.target,
            )
        else:
            logger.debug(
                f"We don't react to this koji build state change: {self.koji_event.state}"
            )

        koji_build_logs = self.koji_event.get_koji_build_logs_url(
            koji_logs_url=self.service_config.koji_logs_url
        )
        build.set_build_logs_url(koji_build_logs)
        koji_rpm_task_web_url = self.koji_event.get_koji_rpm_build_web_url(
            koji_web_url=self.service_config.koji_web_url
        )
        build.set_web_url(koji_rpm_task_web_url)

        msg = (
            f"Build on {build.target} in koji changed state "
            f"from {self.koji_event.old_state} to {self.koji_event.state}."
        )
        return TaskResults(success=True, details={"msg": msg})
