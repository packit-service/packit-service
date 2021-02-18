# MIT License
#
# Copyright (c) 2018-2019 Red Hat, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import logging
from io import StringIO
from pathlib import Path
from typing import List, Optional, Set, Tuple, Union

from kubernetes.client.rest import ApiException

from ogr.abstract import CommitStatus, GitProject
from ogr.exceptions import GitlabAPIException
from ogr.services.gitlab import GitlabProject
from packit.api import PackitAPI
from packit.config import JobConfig, JobType
from packit.config.package_config import PackageConfig
from packit.local_project import LocalProject
from packit.utils import PackitFormatter
from packit_service import sentry_integration
from packit_service.config import Deployment, ServiceConfig
from packit_service.models import RunModel, SRPMBuildModel
from packit_service.service.events import EventData
from packit_service.trigger_mapping import are_job_types_same
from packit_service.worker.reporting import StatusReporter
from sandcastle import SandcastleTimeoutReached

logger = logging.getLogger(__name__)


class BaseBuildJobHelper:
    job_type_build: Optional[JobType] = None
    job_type_test: Optional[JobType] = None
    status_name_build: str = "base-build-status"
    status_name_test: str = "base-test-status"

    def __init__(
        self,
        service_config: ServiceConfig,
        package_config: PackageConfig,
        project: GitProject,
        metadata: EventData,
        db_trigger,
        job_config: JobConfig,
    ):
        self.service_config: ServiceConfig = service_config
        self.job_config = job_config
        self.package_config = package_config
        self.project: GitProject = project
        self.db_trigger = db_trigger
        self.msg_retrigger: Optional[str] = ""
        self.metadata: EventData = metadata
        self.run_model: Optional[RunModel] = None

        # lazy properties
        self._api = None
        self._local_project = None
        self._status_reporter: Optional[StatusReporter] = None
        self._test_check_names: Optional[List[str]] = None
        self._build_check_names: Optional[List[str]] = None
        self._srpm_model: Optional[SRPMBuildModel] = None
        self._srpm_path: Optional[Path] = None
        self._job_tests: Optional[JobConfig] = None
        self._job_build: Optional[JobConfig] = None
        self._base_project: Optional[GitProject] = None
        self._pr_id: Optional[int] = None
        self._is_reporting_allowed: Optional[bool] = None
        self._is_gitlab_instance: Optional[bool] = None

    @property
    def local_project(self) -> LocalProject:
        if self._local_project is None:
            self._local_project = LocalProject(
                git_project=self.project,
                working_dir=self.service_config.command_handler_work_dir,
                ref=self.metadata.git_ref,
                pr_id=self.metadata.pr_id,
            )
        return self._local_project

    @property
    def is_gitlab_instance(self) -> bool:
        if self._is_gitlab_instance is None:
            self._is_gitlab_instance = isinstance(self.project, GitlabProject)

        return self._is_gitlab_instance

    @property
    def pr_id(self) -> Optional[int]:
        if self._pr_id is None:
            self._pr_id = self.metadata.pr_id
        return self._pr_id

    @property
    def is_reporting_allowed(self) -> bool:
        username = self.project.service.user.get_username()
        if self._is_reporting_allowed is None:
            self._is_reporting_allowed = self.base_project.can_merge_pr(username)
        return self._is_reporting_allowed

    @property
    def base_project(self) -> GitProject:
        """
        Getting the source project info from PR,
        In case of build events we loose the source info.
        """
        if self._base_project is None:
            if self.pr_id:
                self._base_project = self.project.get_pr(
                    pr_id=self.pr_id
                ).source_project
            else:
                self._base_project = self.project
        return self._base_project

    def request_project_access(self) -> None:
        try:
            self.base_project.request_access()
        except GitlabAPIException:
            logger.info("Access already requested")

    @property
    def api(self) -> PackitAPI:
        if not self._api:
            self._api = PackitAPI(
                self.service_config,
                self.job_config,
                self.local_project,
                stage=self.service_config.use_stage(),
            )
        return self._api

    @property
    def api_url(self) -> str:
        return (
            "https://prod.packit.dev/api"
            if self.service_config.deployment == Deployment.prod
            else "https://stg.packit.dev/api"
        )

    @property
    def configured_build_targets(self) -> Set[str]:
        """
        Return the targets to build.

        1. Use targets defined for build job and targets defined for test job.
        2. Use "fedora-stable" alias if neither defined.
        """
        targets = set()
        if self.job_build:
            targets.update(self.job_build.metadata.targets)

        if self.job_tests:
            targets.update(self.job_tests.metadata.targets)

        return targets or {"fedora-stable"}

    @property
    def configured_tests_targets(self) -> Set[str]:
        """
        Return the targets used in the testing farm.
        Has to be a sub-set of the `configured_build_targets`.

        Return an empty set if there is no test job configured.

        If not defined:
        1. use the `configured_build_targets` if the build job is configured
        2. use "fedora-stable" alias otherwise
        """
        if not self.job_tests:
            return set()

        if not self.job_tests.metadata.targets and self.job_build:
            return self.configured_build_targets

        return self.job_tests.metadata.targets or {"fedora-stable"}

    @property
    def job_build(self) -> Optional[JobConfig]:
        """
        Check if there is JobConfig for builds defined
        :return: JobConfig or None
        """
        if not self.job_type_build:
            return None
        if not self._job_build:
            for job in [self.job_config] + self.package_config.jobs:
                if are_job_types_same(job.type, self.job_type_build) and (
                    self.db_trigger
                    and self.db_trigger.job_config_trigger_type == job.trigger
                ):
                    self._job_build = job
                    break
        return self._job_build

    @property
    def job_build_branch(self) -> Optional[str]:
        """
        Branch used for the build job or project's default branch.
        """
        if self.job_build and self.job_build.metadata.branch:
            return self.job_build.metadata.branch

        return self.project.default_branch

    @property
    def job_tests(self) -> Optional[JobConfig]:
        """
        Check if there is JobConfig for tests defined
        :return: JobConfig or None
        """
        if not self.job_type_test:
            return None

        if not self._job_tests:
            for job in [self.job_config] + self.package_config.jobs:
                if are_job_types_same(job.type, self.job_type_test) and (
                    self.db_trigger
                    and self.db_trigger.job_config_trigger_type == job.trigger
                ):
                    self._job_tests = job
                    break
        return self._job_tests

    @property
    def status_reporter(self) -> StatusReporter:
        if not self._status_reporter:
            self._status_reporter = StatusReporter(
                project=self.project,
                commit_sha=self.metadata.commit_sha,
                pr_id=self.metadata.pr_id,
            )
        return self._status_reporter

    @property
    def build_targets(self) -> Set[str]:
        """
        Return the targets/chroots to build.

        (Used when submitting the koji/copr build and as a part of the commit status name.)

        1. If the job is not defined, use the test chroots.
        2. If the job is defined without targets, use "fedora-stable".
        """
        raise NotImplementedError("Use subclass instead.")

    @property
    def tests_targets(self) -> Set[str]:
        """
        Return the list of targets/chroots used in testing farm.
        Has to be a sub-set of the `build_targets`.

        (Used when submitting the koji/copr build and as a part of the commit status name.)

        Return an empty list if there is no job configured.

        If not defined:
        1. use the build_targets if the job si configured
        2. use "fedora-stable" alias otherwise
        """
        raise NotImplementedError("Use subclass instead.")

    @property
    def test_check_names(self) -> List[str]:
        """
        List of full names of the commit statuses.

        e.g. ["packit/copr-build-fedora-rawhide-x86_64"]
        or ["packit-stg/production-build-f31", "packit-stg/production-build-f32"]
        """
        if not self._test_check_names:
            self._test_check_names = [
                self.get_test_check(target) for target in self.tests_targets
            ]
        return self._test_check_names

    @property
    def build_check_names(self) -> List[str]:
        """
        List of full names of the commit statuses.

        e.g. ["packit/copr-build-fedora-rawhide-x86_64"]
        or ["packit-stg/production-build-f31", "packit-stg/production-build-f32"]
        """
        if not self._build_check_names:
            self._build_check_names = [
                self.get_build_check(target) for target in self.build_targets
            ]
        return self._build_check_names

    @property
    def srpm_model(self) -> SRPMBuildModel:
        if not self._srpm_model:
            self._create_srpm()
        return self._srpm_model

    @property
    def srpm_path(self) -> Optional[Path]:
        self.create_srpm_if_needed()
        return self._srpm_path

    @classmethod
    def get_build_check(cls, chroot: str = None) -> str:
        config = ServiceConfig.get_service_config()
        deployment_str = (
            "packit" if config.deployment == Deployment.prod else "packit-stg"
        )
        chroot_str = f"-{chroot}" if chroot else ""
        return f"{deployment_str}/{cls.status_name_build}{chroot_str}"

    @classmethod
    def get_test_check(cls, chroot: str = None) -> str:
        config = ServiceConfig.get_service_config()
        deployment_str = (
            "packit" if config.deployment == Deployment.prod else "packit-stg"
        )
        chroot_str = f"-{chroot}" if chroot else ""
        return f"{deployment_str}/{cls.status_name_test}{chroot_str}"

    def create_srpm_if_needed(self) -> None:
        """If you want to be sure we already created the SRPM."""
        if not (self._srpm_path or self._srpm_model):
            self._create_srpm()

    def _create_srpm(self):
        # we want to get packit logs from the SRPM creation process
        # so we stuff them into a StringIO buffer
        stream = StringIO()
        handler = logging.StreamHandler(stream)
        packit_logger = logging.getLogger("packit")
        packit_logger.setLevel(logging.DEBUG)
        packit_logger.addHandler(handler)
        formatter = PackitFormatter()
        handler.setFormatter(formatter)

        srpm_success = True
        exception: Optional[Exception] = None
        extra_logs: str = ""

        try:
            self._srpm_path = Path(
                self.api.create_srpm(srpm_dir=self.api.up.local_project.working_dir)
            )
        except SandcastleTimeoutReached as ex:
            exception = ex
            extra_logs = "\nYou have reached 10-minute timeout while creating SRPM.\n"
        except ApiException as ex:
            exception = ex
            # this is an internal error: let's not expose anything to public
            extra_logs = (
                "\nThere was a problem in the environment the packit-service is running in.\n"
                "Please hang tight, the help is coming."
            )
        except Exception as ex:
            exception = ex

        # collect the logs now
        packit_logger.removeHandler(handler)
        stream.seek(0)
        srpm_logs = stream.read()

        if exception:
            logger.info(f"exception while running SRPM build: {exception}")
            logger.debug(f"{exception!r}")

            srpm_success = False

            # when do we NOT want to send stuff to sentry?
            sentry_integration.send_to_sentry(exception)

            # this needs to be done AFTER we gather logs
            # so that extra logs are after actual logs
            srpm_logs += extra_logs
            if hasattr(exception, "output"):
                output = getattr(exception, "output", "")  # mypy
                srpm_logs += f"\nOutput of the command in the sandbox:\n{output}\n"

            srpm_logs += (
                f"\nMessage: {exception}\nException: {exception!r}\n{self.msg_retrigger}"
                "\nPlease join the freenode IRC channel #packit for the latest info.\n"
            )

        self._srpm_model, self.run_model = SRPMBuildModel.create_with_new_run(
            logs=srpm_logs,
            success=srpm_success,
            trigger_model=self.db_trigger,
        )

    def _report(
        self,
        state: CommitStatus,
        description: str,
        url: str = "",
        check_names: Union[str, list, None] = None,
    ) -> None:
        """
        The status reporting should be done through this method
        so we can extend it in subclasses easily.
        """
        if self.is_gitlab_instance and not self.is_reporting_allowed:
            self.request_project_access()
            description = (
                f"{description}\n\n---\n*Packit-User does not have access to the "
                "source project. We have requested access to be able to set "
                "commit statuses / pipelines instead of the comments.*"
            )

            final_commit_states = (
                CommitStatus.success,
                CommitStatus.failure,
                CommitStatus.error,
            )
            # We are only commenting final states to avoid multiple comments for a build
            # Ignoring all other states eg. pending, running
            if state not in final_commit_states:
                return

        self.status_reporter.report(
            description=description,
            state=state,
            url=url,
            check_names=check_names,
        )

    def report_status_to_all(
        self, description: str, state: CommitStatus, url: str = ""
    ) -> None:
        self.report_status_to_build(description, state, url)
        self.report_status_to_tests(description, state, url)

    def report_status_to_build(self, description, state, url: str = "") -> None:
        if self.job_build:
            self._report(
                description=description,
                state=state,
                url=url,
                check_names=self.build_check_names,
            )

    def report_status_to_tests(self, description, state, url: str = "") -> None:
        if self.job_tests:
            self._report(
                description=description,
                state=state,
                url=url,
                check_names=self.test_check_names,
            )

    def report_status_to_build_for_chroot(
        self, description, state, url: str = "", chroot: str = ""
    ) -> None:
        if self.job_build and chroot in self.build_targets:
            cs = self.get_build_check(chroot)
            self._report(
                description=description,
                state=state,
                url=url,
                check_names=cs,
            )

    def report_status_to_test_for_chroot(
        self, description, state, url: str = "", chroot: str = ""
    ) -> None:
        if self.job_tests and chroot in self.tests_targets:
            self._report(
                description=description,
                state=state,
                url=url,
                check_names=self.get_test_check(chroot),
            )

    def report_status_to_all_for_chroot(
        self, description, state, url: str = "", chroot: str = ""
    ):
        self.report_status_to_build_for_chroot(description, state, url, chroot)
        self.report_status_to_test_for_chroot(description, state, url, chroot)

    def run_build(
        self, target: Optional[str] = None
    ) -> Tuple[Optional[int], Optional[str]]:
        """
        Trigger the build and return id and web_url
        :param target: str, run for all if not set
        :return: task_id, task_url
        """
        raise NotImplementedError()
