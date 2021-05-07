# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

import logging
import re
from typing import Optional

from ogr.abstract import CommitStatus, PullRequest
from packit.config import (
    JobConfig,
)
from packit.config.package_config import PackageConfig

from packit_service.models import (
    BugzillaModel,
)
from packit_service.service.events import (
    MergeRequestGitlabEvent,
    GitlabEventAction,
)
from packit_service.worker.handlers.abstract import (
    JobHandler,
    TaskName,
    reacts_to,
)
from packit_service.worker.psbugzilla import Bugzilla
from packit_service.worker.reporting import StatusReporter
from packit_service.worker.result import TaskResults

logger = logging.getLogger(__name__)


@reacts_to(MergeRequestGitlabEvent)
class BugzillaHandler(JobHandler):
    task_name = TaskName.bugzilla

    def __init__(
        self, package_config: PackageConfig, job_config: JobConfig, event: dict
    ):
        super().__init__(
            package_config=package_config,
            job_config=job_config,
            event=event,
        )
        self.action: GitlabEventAction = GitlabEventAction(event["action"])
        self.target_repo_name = event.get("target_repo_name")
        self.target_repo_namespace = event.get("target_repo_namespace")
        self.target_repo_branch = event.get("target_repo_branch")

        self.pr: PullRequest = self.project.get_pr(self.data.pr_id)
        # lazy properties
        self._bz_model: Optional[BugzillaModel] = None
        self._bugzilla: Optional[Bugzilla] = None
        self._status_reporter: Optional[StatusReporter] = None

    @property
    def bz_model(self) -> Optional[BugzillaModel]:
        if self._bz_model is None:
            self._bz_model = BugzillaModel.get_by_pr(
                pr_id=self.data.pr_id,
                namespace=self.target_repo_namespace,
                repo_name=self.target_repo_name,
                project_url=self.data.project_url,
            )
        return self._bz_model

    @property
    def bugzilla(self) -> Bugzilla:
        if self._bugzilla is None:
            self._bugzilla = Bugzilla(
                url=self.service_config.bugzilla_url,
                api_key=self.service_config.bugzilla_api_key,
            )
        return self._bugzilla

    @property
    def status_reporter(self) -> StatusReporter:
        if not self._status_reporter:
            self._status_reporter = StatusReporter(
                self.project, self.data.commit_sha, self.data.pr_id
            )
        return self._status_reporter

    def _create_bug(self):
        """Fill a Bugzilla bug and store in db."""
        description = f"""This bug has been opened as a response to a Merge Request (MR)
{self.pr.url}
in the source-git repository.

For more info
https://wiki.centos.org/Contribute/CentOSStream
https://docs.centos.org/en-US/stream-contrib

How to get a patch from the MR:
curl {self.pr.url}.patch"""
        bug_id, bug_url = self.bugzilla.create_bug(
            product="Red Hat Enterprise Linux 8",
            version="CentOS Stream",
            component=self.target_repo_name,
            summary=self.pr.title,
            description=description,
        )
        self._bz_model = BugzillaModel.get_or_create(
            pr_id=self.data.pr_id,
            namespace=self.target_repo_namespace,
            repo_name=self.target_repo_name,
            project_url=self.data.project_url,
            bug_id=bug_id,
            bug_url=bug_url,
        )

    def _set_status(self):
        """
        Set commit status & pull-request flag with bug id as a name and a link to the created bug.
        """
        if not (self.bz_model and self.bz_model.bug_id and self.bz_model.bug_url):
            raise RuntimeError(
                "MergeRequestLabelHandler._set_status(): bug_id or bug_url not set"
            )

        self.status_reporter.set_status(
            state=CommitStatus.success,
            description="Bugzilla bug created.",
            check_name=f"RHBZ#{self.bz_model.bug_id}",
            url=self.bz_model.bug_url,
        )

    def run(self) -> TaskResults:
        if self.action != GitlabEventAction.opened:
            logger.debug("Won't run BugzillaHandler for already opened MR.")
            return TaskResults(success=True)
        logger.debug(
            f"About to create a bugzilla based on MR "
            f"{self.target_repo_namespace}/{self.target_repo_name}/{self.data.identifier} "
            f"branch {self.target_repo_branch}"
        )
        if not any(
            re.match(n, self.target_repo_namespace)
            for n in self.service_config.bugz_namespaces
        ):
            logger.debug(
                f"We accept only {self.service_config.bugz_namespaces} namespaces"
            )
            return TaskResults(success=True)
        if not any(
            re.match(b, self.target_repo_branch)
            for b in self.service_config.bugz_branches
        ):
            logger.debug(f"We accept only {self.service_config.bugz_branches} branches")
            return TaskResults(success=True)

        if not self.bz_model:
            self._create_bug()
        self._set_status()
        return TaskResults(success=True)
