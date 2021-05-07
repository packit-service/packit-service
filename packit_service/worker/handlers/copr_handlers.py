# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

import logging
from datetime import datetime
from typing import Optional

from celery import signature
from ogr.abstract import CommitStatus
from ogr.services.github import GithubProject
from ogr.services.gitlab import GitlabProject
from packit.config import (
    JobConfig,
    JobType,
)
from packit.config import JobConfigTriggerType
from packit.config.package_config import PackageConfig

from packit_service.constants import (
    COPR_API_SUCC_STATE,
    PG_COPR_BUILD_STATUS_FAILURE,
    PG_COPR_BUILD_STATUS_SUCCESS,
)
from packit_service.models import AbstractTriggerDbType, CoprBuildModel
from packit_service.service.events import (
    CoprBuildEndEvent,
    AbstractCoprBuildEvent,
    CoprBuildStartEvent,
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
    get_copr_build_info_url,
)
from packit_service.utils import dump_job_config, dump_package_config
from packit_service.worker.build import CoprBuildJobHelper
from packit_service.worker.handlers.abstract import (
    JobHandler,
    TaskName,
    configured_as,
    reacts_to,
    required_for,
    run_for_comment,
    add_topic,
    FedmsgHandler,
)
from packit_service.worker.result import TaskResults

logger = logging.getLogger(__name__)


@configured_as(job_type=JobType.copr_build)
@configured_as(job_type=JobType.build)
@required_for(job_type=JobType.tests)
@run_for_comment(command="build")
@run_for_comment(command="copr-build")
@reacts_to(ReleaseEvent)
@reacts_to(PullRequestGithubEvent)
@reacts_to(PushGitHubEvent)
@reacts_to(PushGitlabEvent)
@reacts_to(MergeRequestGitlabEvent)
@reacts_to(PullRequestCommentGithubEvent)
@reacts_to(MergeRequestCommentGitlabEvent)
@reacts_to(PullRequestCommentPagureEvent)
class CoprBuildHandler(JobHandler):
    task_name = TaskName.copr_build

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

        self._copr_build_helper: Optional[CoprBuildJobHelper] = None

    @property
    def copr_build_helper(self) -> CoprBuildJobHelper:
        if not self._copr_build_helper:
            self._copr_build_helper = CoprBuildJobHelper(
                service_config=self.service_config,
                package_config=self.package_config,
                project=self.project,
                metadata=self.data,
                db_trigger=self.data.db_trigger,
                job_config=self.job_config,
            )
        return self._copr_build_helper

    def run(self) -> TaskResults:
        return self.copr_build_helper.run_copr_build()

    def pre_check(self) -> bool:
        if self.data.event_type in (
            PushGitHubEvent.__name__,
            PushGitlabEvent.__name__,
            PushPagureEvent.__name__,
        ):
            configured_branch = self.copr_build_helper.job_build_branch
            if self.data.git_ref != configured_branch:
                logger.info(
                    f"Skipping build on '{self.data.git_ref}'. "
                    f"Push configured only for '{configured_branch}'."
                )
                return False

        if not (self.copr_build_helper.job_build or self.copr_build_helper.job_tests):
            logger.info("No copr_build or tests job defined.")
            # we can't report it to end-user at this stage
            return False

        return True


class AbstractCoprBuildReportHandler(FedmsgHandler):
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
        self.copr_event = AbstractCoprBuildEvent.from_event_dict(event)
        self._build = None
        self._db_trigger = None

    @property
    def build(self):
        if not self._build:
            self._build = CoprBuildModel.get_by_build_id(
                str(self.copr_event.build_id), self.copr_event.chroot
            )
        return self._build

    @property
    def db_trigger(self) -> Optional[AbstractTriggerDbType]:
        if not self._db_trigger:
            self._db_trigger = self.build.get_trigger_object()
        return self._db_trigger


@add_topic
@configured_as(job_type=JobType.copr_build)
@configured_as(job_type=JobType.build)
@required_for(job_type=JobType.tests)
@reacts_to(event=CoprBuildStartEvent)
class CoprBuildStartHandler(AbstractCoprBuildReportHandler):
    topic = "org.fedoraproject.prod.copr.build.start"
    task_name = TaskName.copr_build_start

    def run(self):
        build_job_helper = CoprBuildJobHelper(
            service_config=self.service_config,
            package_config=self.package_config,
            project=self.project,
            metadata=self.data,
            db_trigger=self.db_trigger,
            job_config=self.job_config,
        )

        if self.copr_event.chroot == "srpm-builds":
            # we don't want to set the check status for this
            msg = "SRPM build in copr has started."
            logger.debug(msg)
            return TaskResults(success=True, details={"msg": msg})

        if not self.build:
            msg = f"Copr build {self.copr_event.build_id} not in CoprBuildDB."
            logger.warning(msg)
            return TaskResults(success=False, details={"msg": msg})

        start_time = (
            datetime.utcfromtimestamp(self.copr_event.timestamp)
            if self.copr_event.timestamp
            else None
        )
        self.build.set_start_time(start_time)
        url = get_copr_build_info_url(self.build.id)
        self.build.set_status("pending")
        copr_build_logs = self.copr_event.get_copr_build_logs_url()
        self.build.set_build_logs_url(copr_build_logs)

        build_job_helper.report_status_to_all_for_chroot(
            description="RPM build is in progress...",
            state=CommitStatus.pending,
            url=url,
            chroot=self.copr_event.chroot,
        )
        msg = f"Build on {self.copr_event.chroot} in copr has started..."
        return TaskResults(success=True, details={"msg": msg})


@add_topic
@configured_as(job_type=JobType.copr_build)
@configured_as(job_type=JobType.build)
@required_for(job_type=JobType.tests)
@reacts_to(event=CoprBuildEndEvent)
class CoprBuildEndHandler(AbstractCoprBuildReportHandler):
    topic = "org.fedoraproject.prod.copr.build.end"
    task_name = TaskName.copr_build_end

    def was_last_packit_comment_with_congratulation(self):
        """
        Check if the last comment by the packit app
        was about successful build to not duplicate it.

        :return: bool
        """
        comments = self.project.get_pr_comments(
            pr_id=self.copr_event.pr_id, reverse=True
        )
        for comment in comments:
            if comment.author.startswith("packit-as-a-service"):
                return "Congratulations!" in comment.comment
        # if there is no comment from p-s
        return False

    def set_srpm_url(self, build_job_helper: CoprBuildJobHelper) -> None:
        srpm_build = self.build.get_srpm_build()

        if srpm_build.url is not None:
            # URL has been already set
            return

        srpm_url = build_job_helper.get_build(
            self.copr_event.build_id
        ).source_package.get("url")
        srpm_build.set_url(srpm_url)

    def run(self):
        build_job_helper = CoprBuildJobHelper(
            service_config=self.service_config,
            package_config=self.package_config,
            project=self.project,
            metadata=self.data,
            db_trigger=self.db_trigger,
            job_config=self.job_config,
        )

        if self.copr_event.chroot == "srpm-builds":
            # we don't want to set check for this
            msg = "SRPM build in copr has finished."
            logger.debug(msg)
            return TaskResults(success=True, details={"msg": msg})

        if not self.build:
            # TODO: how could this happen?
            msg = f"Copr build {self.copr_event.build_id} not in CoprBuildDB."
            logger.warning(msg)
            return TaskResults(success=False, details={"msg": msg})
        if self.build.status in [
            PG_COPR_BUILD_STATUS_FAILURE,
            PG_COPR_BUILD_STATUS_SUCCESS,
        ]:
            msg = (
                f"Copr build {self.copr_event.build_id} is already"
                f" processed (status={self.copr_event.build.status})."
            )
            logger.info(msg)
            return TaskResults(success=True, details={"msg": msg})

        end_time = (
            datetime.utcfromtimestamp(self.copr_event.timestamp)
            if self.copr_event.timestamp
            else None
        )
        self.build.set_end_time(end_time)
        self.set_srpm_url(build_job_helper)

        url = get_copr_build_info_url(self.build.id)

        # https://pagure.io/copr/copr/blob/master/f/common/copr_common/enums.py#_42
        if self.copr_event.status != COPR_API_SUCC_STATE:
            failed_msg = "RPMs failed to be built."
            build_job_helper.report_status_to_all_for_chroot(
                state=CommitStatus.failure,
                description=failed_msg,
                url=url,
                chroot=self.copr_event.chroot,
            )
            self.build.set_status(PG_COPR_BUILD_STATUS_FAILURE)
            return TaskResults(success=False, details={"msg": failed_msg})

        if (
            build_job_helper.job_build
            and build_job_helper.job_build.trigger == JobConfigTriggerType.pull_request
            and self.copr_event.pr_id
            and isinstance(self.project, (GithubProject, GitlabProject))
            and not self.was_last_packit_comment_with_congratulation()
            and self.job_config.notifications.pull_request.successful_build
        ):
            msg = (
                f"Congratulations! One of the builds has completed. :champagne:\n\n"
                ":warning: Please note that our current plans include removal of these "
                "comments in the near future (at least 2 weeks after including this "
                "disclaimer), if you have serious concerns regarding their removal "
                "or would like to continue receiving them please reach out to us. "
                ":warning:\n\n"
                "You can install the built RPMs by following these steps:\n\n"
                "* `sudo yum install -y dnf-plugins-core` on RHEL 8\n"
                "* `sudo dnf install -y dnf-plugins-core` on Fedora\n"
                f"* `dnf copr enable {self.copr_event.owner}/{self.copr_event.project_name}`\n"
                "* And now you can install the packages.\n"
                "\nPlease note that the RPMs should be used only in a testing environment."
            )
            self.project.pr_comment(pr_id=self.copr_event.pr_id, body=msg)

        build_job_helper.report_status_to_build_for_chroot(
            state=CommitStatus.success,
            description="RPMs were built successfully.",
            url=url,
            chroot=self.copr_event.chroot,
        )
        build_job_helper.report_status_to_test_for_chroot(
            state=CommitStatus.pending,
            description="RPMs were built successfully.",
            url=url,
            chroot=self.copr_event.chroot,
        )
        self.build.set_status(PG_COPR_BUILD_STATUS_SUCCESS)

        if (
            build_job_helper.job_tests
            and self.copr_event.chroot in build_job_helper.tests_targets
        ):
            signature(
                TaskName.testing_farm.value,
                kwargs={
                    "package_config": dump_package_config(self.package_config),
                    "job_config": dump_job_config(build_job_helper.job_tests),
                    "event": self.data.get_dict(),
                    "chroot": self.copr_event.chroot,
                    "build_id": self.build.id,
                },
            ).apply_async()
        else:
            logger.debug("Testing farm not in the job config.")

        return TaskResults(success=True, details={})
