# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

"""
This file defines classes for job handlers specific for Testing farm
"""
import logging
from typing import Optional

from ogr import GitlabService
from ogr.abstract import CommitStatus
from packit.config import JobConfig, JobType
from packit.config.package_config import PackageConfig

from packit_service.models import AbstractTriggerDbType, TFTTestRunModel, CoprBuildModel
from packit_service.service.events import (
    TestResult,
    TestingFarmResult,
    TestingFarmResultsEvent,
    PullRequestCommentGithubEvent,
    MergeRequestCommentGitlabEvent,
    PullRequestCommentPagureEvent,
)
from packit_service.service.urls import get_testing_farm_info_url
from packit_service.worker.handlers import JobHandler
from packit_service.worker.handlers.abstract import (
    TaskName,
    configured_as,
    reacts_to,
    run_for_comment,
)
from packit_service.worker.reporting import StatusReporter
from packit_service.worker.result import TaskResults
from packit_service.worker.testing_farm import TestingFarmJobHelper

logger = logging.getLogger(__name__)


@run_for_comment(command="test")
@reacts_to(PullRequestCommentGithubEvent)
@reacts_to(MergeRequestCommentGitlabEvent)
@reacts_to(PullRequestCommentPagureEvent)
@configured_as(job_type=JobType.tests)
class TestingFarmHandler(JobHandler):
    """
    The automatic matching is now used only for /packit test
    TODO: We can react directly to the finished Copr build.
    """

    task_name = TaskName.testing_farm

    def __init__(
        self,
        package_config: PackageConfig,
        job_config: JobConfig,
        event: dict,
        chroot: Optional[str] = None,
        build_id: Optional[int] = None,
    ):
        super().__init__(
            package_config=package_config,
            job_config=job_config,
            event=event,
        )
        self.chroot = chroot
        self.build_id = build_id
        self._db_trigger: Optional[AbstractTriggerDbType] = None

    @property
    def db_trigger(self) -> Optional[AbstractTriggerDbType]:
        if not self._db_trigger:
            # copr build end
            if self.build_id:
                build = CoprBuildModel.get_by_id(self.build_id)
                self._db_trigger = build.get_trigger_object()
            # '/packit test' comment
            else:
                self._db_trigger = self.data.db_trigger
        return self._db_trigger

    def run(self) -> TaskResults:
        # TODO: once we turn handlers into respective celery tasks, we should iterate
        #       here over *all* matching jobs and do them all, not just the first one
        testing_farm_helper = TestingFarmJobHelper(
            service_config=self.service_config,
            package_config=self.package_config,
            project=self.project,
            metadata=self.data,
            db_trigger=self.db_trigger,
            job_config=self.job_config,
        )

        if self.data.event_type in (
            PullRequestCommentGithubEvent.__name__,
            MergeRequestCommentGitlabEvent.__name__,
            PullRequestCommentPagureEvent.__name__,
        ):
            logger.debug(f"Test job config: {testing_farm_helper.job_tests}")
            return testing_farm_helper.run_testing_farm_on_all()

        if self.build_id:
            copr_build = CoprBuildModel.get_by_id(self.build_id)
        else:
            copr_build = testing_farm_helper.get_latest_copr_build(target=self.chroot)
        logger.info(f"Running testing farm for {copr_build}:{self.chroot}.")
        return testing_farm_helper.run_testing_farm(
            build=copr_build, chroot=self.chroot
        )


@configured_as(job_type=JobType.tests)
@reacts_to(event=TestingFarmResultsEvent)
class TestingFarmResultsHandler(JobHandler):
    task_name = TaskName.testing_farm_results

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

        self.tests = [TestResult(**test) for test in event.get("tests", [])]
        self.result = (
            TestingFarmResult(event.get("result")) if event.get("result") else None
        )
        self.pipeline_id = event.get("pipeline_id")
        self.log_url = event.get("log_url")
        self.copr_chroot = event.get("copr_chroot")
        self.summary = event.get("summary")
        self._db_trigger: Optional[AbstractTriggerDbType] = None

    @property
    def db_trigger(self) -> Optional[AbstractTriggerDbType]:
        if not self._db_trigger:
            run_model = TFTTestRunModel.get_by_pipeline_id(pipeline_id=self.pipeline_id)
            if run_model:
                self._db_trigger = run_model.get_trigger_object()
        return self._db_trigger

    def run(self) -> TaskResults:
        logger.debug(f"Testing farm {self.pipeline_id} result:\n{self.result}")
        logger.debug(f"Testing farm test results:\n{self.tests}")

        test_run_model = TFTTestRunModel.get_by_pipeline_id(
            pipeline_id=self.pipeline_id
        )
        if not test_run_model:
            logger.warning(
                f"Unknown pipeline_id received from the testing-farm: "
                f"{self.pipeline_id}"
            )

        if test_run_model:
            test_run_model.set_status(self.result)

        if self.result == TestingFarmResult.running:
            status = CommitStatus.pending
            if isinstance(self.project.service, GitlabService):
                # only Gitlab has 'running' state
                status = CommitStatus.running
            summary = self.summary or "Tests are running ..."
        elif self.result == TestingFarmResult.passed:
            status = CommitStatus.success
            summary = self.summary or "Tests passed ..."
        elif self.result == TestingFarmResult.error:
            status = CommitStatus.error
            if isinstance(self.project.service, GitlabService):
                # Gitlab has no 'error' state
                status = CommitStatus.failure
            summary = self.summary or "Error ..."
        else:
            status = CommitStatus.failure
            summary = self.summary or "Tests failed ..."

        if len(self.tests) > 0 and self.tests[0].name == "/packit/install-and-verify":
            logger.debug("No-fmf scenario discovered.")
            summary = (
                "Installation passed"
                if status == CommitStatus.success
                else "Installation failed"
            )

        if test_run_model:
            test_run_model.set_web_url(self.log_url)
        status_reporter = StatusReporter(
            project=self.project, commit_sha=self.data.commit_sha, pr_id=self.data.pr_id
        )
        status_reporter.report(
            state=status,
            description=summary,
            url=get_testing_farm_info_url(test_run_model.id)
            if test_run_model
            else self.log_url,
            check_names=TestingFarmJobHelper.get_test_check(self.copr_chroot),
        )

        return TaskResults(success=True, details={})
