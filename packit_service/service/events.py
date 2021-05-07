# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

"""
This file defines classes for events which are sent by GitHub or FedMsg.
"""
import copy
import enum
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union

from ogr.abstract import GitProject
from ogr.services.pagure import PagureProject
from packit.config import PackageConfig, get_package_config_from_repo

from packit_service.config import PackageConfigGetter, ServiceConfig
from packit_service.constants import KojiBuildState
from packit_service.models import (
    AbstractTriggerDbType,
    CoprBuildModel,
    GitBranchModel,
    IssueModel,
    JobTriggerModelType,
    KojiBuildModel,
    ProjectReleaseModel,
    PullRequestModel,
    TFTTestRunModel,
    TestingFarmResult,
    AllowlistStatus,
)
from packit_service.service.db_triggers import (
    AddBranchPushDbTrigger,
    AddIssueDbTrigger,
    AddPullRequestDbTrigger,
    AddReleaseDbTrigger,
)

logger = logging.getLogger(__name__)


class PullRequestAction(enum.Enum):
    opened = "opened"
    reopened = "reopened"
    synchronize = "synchronize"


class GitlabEventAction(enum.Enum):
    opened = "opened"
    reopen = "reopen"
    update = "update"


class PullRequestCommentAction(enum.Enum):
    created = "created"
    edited = "edited"


class IssueCommentAction(enum.Enum):
    created = "created"
    edited = "edited"


class PullRequestLabelAction(enum.Enum):
    added = "added"
    removed = "removed"


class FedmsgTopic(enum.Enum):
    dist_git_push = "org.fedoraproject.prod.git.receive"
    copr_build_finished = "org.fedoraproject.prod.copr.build.end"
    copr_build_started = "org.fedoraproject.prod.copr.build.start"
    pr_flag_added = "org.fedoraproject.prod.pagure.pull-request.flag.added"


class TestResult(dict):
    def __init__(self, name: str, result: TestingFarmResult, log_url: str):
        dict.__init__(self, name=name, result=result, log_url=log_url)
        self.name = name
        self.result = result
        self.log_url = log_url

    def __str__(self) -> str:
        return f"TestResult(name='{self.name}', result={self.result}, log_url='{self.log_url}')"

    def __repr__(self):
        return self.__str__()

    def __hash__(self) -> int:  # type: ignore
        return hash(self.__str__())

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, TestResult):
            return False

        return (
            self.name == o.name
            and self.result == o.result
            and self.log_url == o.log_url
        )


class EventData:
    """
    Class to represent the data which are common for handlers and comes from the original event
    """

    def __init__(
        self,
        event_type: str,
        user_login: str,
        trigger_id: int,
        project_url: str,
        tag_name: Optional[str],
        git_ref: Optional[str],
        pr_id: Optional[int],
        commit_sha: Optional[str],
        identifier: Optional[str],
        event_dict: Optional[dict],
        issue_id: Optional[int],
    ):
        self.event_type = event_type
        self.user_login = user_login
        self.trigger_id = trigger_id
        self.project_url = project_url
        self.tag_name = tag_name
        self.git_ref = git_ref
        self.pr_id = pr_id
        self.commit_sha = commit_sha
        self.identifier = identifier
        self.event_dict = event_dict
        self.issue_id = issue_id

        # lazy attributes
        self._project = None
        self._db_trigger: Optional[AbstractTriggerDbType] = None

    @classmethod
    def from_event_dict(cls, event: dict):
        event_type = event.get("event_type")
        user_login = event.get("user_login")
        trigger_id = event.get("trigger_id")
        project_url = event.get("project_url")
        tag_name = event.get("tag_name")
        git_ref = event.get("git_ref")
        # event has _pr_id as the attribute while pr_id is a getter property
        pr_id = event.get("_pr_id") or event.get("pr_id")
        commit_sha = event.get("commit_sha")
        identifier = event.get("identifier")
        issue_id = event.get("issue_id")

        return EventData(
            event_type=event_type,
            user_login=user_login,
            trigger_id=trigger_id,
            project_url=project_url,
            tag_name=tag_name,
            git_ref=git_ref,
            pr_id=pr_id,
            commit_sha=commit_sha,
            identifier=identifier,
            event_dict=event,
            issue_id=issue_id,
        )

    @property
    def project(self):
        if not self._project:
            self._project = self.get_project()
        return self._project

    @property
    def db_trigger(self) -> Optional[AbstractTriggerDbType]:
        if not self._db_trigger:

            # TODO, do a better job
            # Probably, try to recreate original classes.
            if self.event_type in {
                PullRequestGithubEvent.__name__,
                PullRequestPagureEvent.__name__,
                MergeRequestGitlabEvent.__name__,
                PullRequestCommentGithubEvent.__name__,
                MergeRequestCommentGitlabEvent.__name__,
                PullRequestCommentPagureEvent.__name__,
            }:
                self._db_trigger = PullRequestModel.get_or_create(
                    pr_id=self.pr_id,
                    namespace=self.project.namespace,
                    repo_name=self.project.repo,
                    project_url=self.project_url,
                )
            elif self.event_type in {
                PushGitHubEvent.__name__,
                PushGitlabEvent.__name__,
                PushPagureEvent.__name__,
            }:
                self._db_trigger = GitBranchModel.get_or_create(
                    branch_name=self.git_ref,
                    namespace=self.project.namespace,
                    repo_name=self.project.repo,
                    project_url=self.project_url,
                )

            elif self.event_type in {
                ReleaseEvent.__name__,
            }:
                self._db_trigger = ProjectReleaseModel.get_or_create(
                    tag_name=self.tag_name,
                    namespace=self.project.namespace,
                    repo_name=self.project.repo,
                    project_url=self.project_url,
                    commit_hash=self.commit_sha,
                )
            elif self.event_type in {
                IssueCommentEvent.__name__,
                IssueCommentGitlabEvent.__name__,
            }:
                self._db_trigger = IssueModel.get_or_create(
                    issue_id=self.issue_id,
                    namespace=self.project.namespace,
                    repo_name=self.project.repo,
                    project_url=self.project_url,
                )
            else:
                logger.warning(
                    "We don't know, what to search in the database for this event data."
                )

        return self._db_trigger

    def get_dict(self) -> dict:
        d = self.__dict__
        d = copy.deepcopy(d)
        return d

    def get_project(self) -> Optional[GitProject]:
        if not self.project_url:
            return None
        return ServiceConfig.get_service_config().get_project(
            url=self.project_url or self.db_trigger.project.project_url
        )


class Event:
    def __init__(self, created_at: Union[int, float, str] = None):
        self.created_at: datetime
        if created_at:
            if isinstance(created_at, (int, float)):
                self.created_at = datetime.fromtimestamp(created_at, timezone.utc)
            elif isinstance(created_at, str):
                # https://stackoverflow.com/questions/127803/how-do-i-parse-an-iso-8601-formatted-date/49784038
                created_at = created_at.replace("Z", "+00:00")
                self.created_at = datetime.fromisoformat(created_at)
        else:
            self.created_at = datetime.now()

    @staticmethod
    def ts2str(event: dict):
        """
        Convert 'created_at' key from timestamp to iso 8601 time format.
        This would normally be in a from_dict(), but we don't have such method.
        In api/* we read events from db and directly serve them to clients.
        Deserialize (from_dict) and serialize (to_dict) every entry
        just to do this ts2str would be waste of resources.
        """
        created_at = event.get("created_at")
        if isinstance(created_at, int):
            event["created_at"] = datetime.fromtimestamp(created_at).isoformat()
        return event

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        d = default_dict or self.__dict__
        d = copy.deepcopy(d)
        # whole dict has to be JSON serializable because of redis
        d["event_type"] = self.__class__.__name__
        d["trigger_id"] = self.db_trigger.id if self.db_trigger else None
        d["created_at"] = int(d["created_at"].timestamp())
        d["project_url"] = d.get("project_url") or (
            self.db_trigger.project.project_url if self.db_trigger else None
        )
        return d

    @property
    def db_trigger(self) -> Optional[AbstractTriggerDbType]:
        return None

    @property
    def project(self):
        raise NotImplementedError("Please implement me!")

    @property
    def base_project(self):
        raise NotImplementedError("Please implement me!")

    @property
    def package_config(self):
        raise NotImplementedError("Please implement me!")

    def get_package_config(self):
        raise NotImplementedError("Please implement me!")

    def get_project(self) -> GitProject:
        raise NotImplementedError("Please implement me!")

    def pre_check(self) -> bool:
        """
        Implement this method for those events, where you want to check if event properties are
        correct. If this method returns False during runtime, execution of service code is skipped.

        :return: False if we can ignore the event
        """
        return True

    def __str__(self):
        return str(self.get_dict())

    def __repr__(self):
        return f"{self.__class__.__name__}({self.get_dict()})"


class AbstractForgeIndependentEvent(Event):
    commit_sha: Optional[str]
    project_url: str

    def __init__(
        self,
        created_at: Union[int, float, str] = None,
        project_url=None,
        pr_id: Optional[int] = None,
    ):
        super().__init__(created_at)
        self.project_url = project_url
        self._pr_id = pr_id

        # Lazy properties
        self._project: Optional[GitProject] = None
        self._base_project: Optional[GitProject] = None
        self._package_config: Optional[PackageConfig] = None

    @property
    def project(self):
        if not self._project:
            self._project = self.get_project()
        return self._project

    @property
    def base_project(self):
        if not self._base_project:
            self._base_project = self.get_base_project()
        return self._base_project

    @property
    def package_config(self):
        if not self._package_config:
            self._package_config = self.get_package_config()
        return self._package_config

    @property
    def db_trigger(self) -> Optional[AbstractTriggerDbType]:
        raise NotImplementedError()

    @property
    def pr_id(self) -> Optional[int]:
        return self._pr_id

    def get_project(self) -> Optional[GitProject]:
        if not (self.project_url or self.db_trigger):
            return None

        return ServiceConfig.get_service_config().get_project(
            url=self.project_url or self.db_trigger.project.project_url
        )

    def get_base_project(self) -> Optional[GitProject]:
        """Reimplement in the PR events."""
        return None

    def get_package_config(self) -> Optional[PackageConfig]:
        logger.debug(
            f"Getting package_config:\n"
            f"\tproject: {self.project}\n"
            f"\tbase_project: {self.base_project}\n"
            f"\treference: {self.commit_sha}\n"
            f"\tpr_id: {self.pr_id}"
        )

        spec_path = None
        if isinstance(self.base_project, PagureProject):
            spec_path = f"SPECS/{self.project.repo}.spec"
            logger.debug(
                f"Getting package_config from Pagure. "
                f"(Spec-file is expected to be in {spec_path})"
            )
        package_config = PackageConfigGetter.get_package_config_from_repo(
            base_project=self.base_project,
            project=self.project,
            reference=self.commit_sha,
            pr_id=self.pr_id,
            fail_when_missing=False,
            spec_file_path=spec_path,
        )

        # job config change note:
        #   this is used in sync-from-downstream which is buggy - we don't need to change this
        if package_config:
            package_config.upstream_project_url = self.project_url
        return package_config

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        # so that it is JSON serializable (because of Celery tasks)
        result.pop("_project")
        result.pop("_base_project")
        result.pop("_package_config")
        return result


class AbstractGithubEvent(AbstractForgeIndependentEvent):
    def __init__(self, project_url: str, pr_id: Optional[int] = None):
        super().__init__(pr_id=pr_id)
        self.project_url: str = project_url
        self.git_ref: Optional[str] = None  # git ref that can be 'git checkout'-ed
        self.identifier: Optional[
            str
        ] = None  # will be shown to users -- e.g. in logs or in the copr-project name


class AbstractGitlabEvent(AbstractForgeIndependentEvent):
    def __init__(
        self,
        project_url: str,
        pr_id: Optional[int] = None,
    ):
        super().__init__(pr_id=pr_id)
        self.project_url: str = project_url
        self.git_ref: Optional[str] = None
        self.identifier: Optional[
            str
        ] = None  # will be shown to users -- e.g. in logs or in the copr-project name


class ReleaseEvent(AddReleaseDbTrigger, AbstractGithubEvent):
    def __init__(
        self, repo_namespace: str, repo_name: str, tag_name: str, project_url: str
    ):
        super().__init__(project_url=project_url)
        self.repo_namespace = repo_namespace
        self.repo_name = repo_name
        self.tag_name = tag_name
        self.git_ref = tag_name
        self.identifier = tag_name
        self._commit_sha: Optional[str] = None

    @property
    def commit_sha(self) -> Optional[str]:  # type:ignore
        # mypy does not like properties
        if not self._commit_sha:
            self._commit_sha = self.project.get_sha_from_tag(tag_name=self.tag_name)
        return self._commit_sha

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["commit_sha"] = self.commit_sha
        return result


class PushGitHubEvent(AddBranchPushDbTrigger, AbstractGithubEvent):
    def __init__(
        self,
        repo_namespace: str,
        repo_name: str,
        git_ref: str,
        project_url: str,
        commit_sha: str,
    ):
        super().__init__(project_url=project_url)
        self.repo_namespace = repo_namespace
        self.repo_name = repo_name
        self.git_ref = git_ref
        self.commit_sha = commit_sha
        self.identifier = git_ref


class PushGitlabEvent(AddBranchPushDbTrigger, AbstractGitlabEvent):
    def __init__(
        self,
        repo_namespace: str,
        repo_name: str,
        git_ref: str,
        project_url: str,
        commit_sha: str,
    ):
        super().__init__(project_url=project_url)
        self.repo_namespace = repo_namespace
        self.repo_name = repo_name
        self.git_ref = git_ref
        self.commit_sha = commit_sha
        self.identifier = git_ref


class MergeRequestGitlabEvent(AddPullRequestDbTrigger, AbstractGitlabEvent):
    def __init__(
        self,
        action: GitlabEventAction,
        username: str,
        object_id: int,
        object_iid: int,
        source_repo_namespace: str,
        source_repo_name: str,
        source_repo_branch: str,
        target_repo_namespace: str,
        target_repo_name: str,
        target_repo_branch: str,
        project_url: str,
        commit_sha: str,
    ):
        super().__init__(
            project_url=project_url,
            pr_id=object_iid,
        )
        self.action = action
        self.user_login = username
        self.object_id = object_id
        self.identifier = str(object_iid)
        self.source_repo_namespace = source_repo_namespace
        self.source_repo_name = source_repo_name
        self.source_repo_branch = source_repo_branch
        self.target_repo_namespace = target_repo_namespace
        self.target_repo_name = target_repo_name
        self.target_repo_branch = target_repo_branch
        self.project_url = project_url
        self.commit_sha = commit_sha

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["action"] = result["action"].value
        return result

    def get_base_project(self) -> GitProject:
        fork = self.project.service.get_project(
            namespace=self.source_repo_namespace,
            repo=self.source_repo_name,
        )
        return fork


class PullRequestGithubEvent(AddPullRequestDbTrigger, AbstractGithubEvent):
    def __init__(
        self,
        action: PullRequestAction,
        pr_id: int,
        base_repo_namespace: str,
        base_repo_name: str,
        base_ref: str,
        target_repo_namespace: str,
        target_repo_name: str,
        project_url: str,
        commit_sha: str,
        user_login: str,
    ):
        super().__init__(project_url=project_url, pr_id=pr_id)
        self.action = action
        self.base_repo_namespace = base_repo_namespace
        self.base_repo_name = base_repo_name
        self.base_ref = base_ref
        self.target_repo_namespace = target_repo_namespace
        self.target_repo_name = target_repo_name
        self.commit_sha = commit_sha
        self.user_login = user_login
        self.identifier = str(pr_id)
        self.git_ref = None  # pr_id will be used for checkout

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["action"] = result["action"].value
        return result

    def get_base_project(self) -> Optional[GitProject]:
        return None  # With Github app, we cannot work with fork repo


class MergeRequestCommentGitlabEvent(AddPullRequestDbTrigger, AbstractGitlabEvent):
    def __init__(
        self,
        action: GitlabEventAction,
        object_id: int,
        object_iid: int,
        source_repo_namespace: str,
        source_repo_name: Optional[str],
        target_repo_namespace: str,
        target_repo_name: str,
        project_url: str,
        username: str,
        comment: str,
        commit_sha: str,
    ):
        super().__init__(
            project_url=project_url,
            pr_id=object_iid,
        )
        self.action = action
        self.object_id = object_id
        self.object_iid = object_iid
        self.source_repo_namespace = source_repo_namespace
        self.source_repo_name = source_repo_name
        self.target_repo_namespace = target_repo_namespace
        self.target_repo_name = target_repo_name
        self.project_url = project_url
        self.user_login = username
        self.comment = comment
        self.commit_sha = commit_sha
        self.identifier = str(object_iid)

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["action"] = result["action"].value
        return result

    def get_base_project(self) -> GitProject:
        fork = self.project.service.get_project(
            namespace=self.source_repo_namespace,
            repo=self.source_repo_name,
        )
        return fork


class PullRequestCommentGithubEvent(AddPullRequestDbTrigger, AbstractGithubEvent):
    def __init__(
        self,
        action: PullRequestCommentAction,
        pr_id: int,
        base_repo_namespace: str,
        base_repo_name: Optional[str],
        base_ref: Optional[str],
        target_repo_namespace: str,
        target_repo_name: str,
        project_url: str,
        user_login: str,
        comment: str,
        commit_sha: Optional[str] = None,
    ):
        super().__init__(project_url=project_url, pr_id=pr_id)
        self.action = action
        self.base_repo_namespace = base_repo_namespace
        self.base_repo_name = base_repo_name
        self.base_ref = base_ref
        self.target_repo_namespace = target_repo_namespace
        self.target_repo_name = target_repo_name
        self.user_login = user_login
        self.comment = comment
        self.identifier = str(pr_id)
        self.git_ref = None  # pr_id will be used for checkout

        # Lazy properties
        self._commit_sha = commit_sha

    @property
    def commit_sha(self) -> Optional[str]:  # type:ignore
        # mypy does not like properties
        if not self._commit_sha:
            self._commit_sha = self.project.get_pr(pr_id=self.pr_id).head_commit
        return self._commit_sha

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["action"] = result["action"].value
        result["commit_sha"] = self.commit_sha
        return result

    def get_base_project(self) -> Optional[GitProject]:
        return None  # With Github app, we cannot work with fork repo


class IssueCommentGitlabEvent(AddIssueDbTrigger, AbstractGitlabEvent):
    def __init__(
        self,
        action: GitlabEventAction,
        issue_id: int,
        repo_namespace: str,
        repo_name: str,
        project_url: str,
        username: str,
        comment: str,
    ):
        super().__init__(project_url=project_url)
        self.action = action
        self.issue_id = issue_id
        self.repo_namespace = repo_namespace
        self.repo_name = repo_name
        self.project_url = project_url
        self.user_login = username
        self.comment = comment
        self._tag_name = None

    @property
    def tag_name(self):
        if not self._tag_name:
            self._tag_name = ""
            if latest_release := self.project.get_latest_release():
                self._tag_name = latest_release.tag_name
        return self._tag_name

    @property
    def commit_sha(self):
        return self.tag_name

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["action"] = result["action"].value
        result["tag_name"] = self.tag_name
        result["issue_id"] = self.issue_id
        return result


class IssueCommentEvent(AddIssueDbTrigger, AbstractGithubEvent):
    def __init__(
        self,
        action: IssueCommentAction,
        issue_id: int,
        repo_namespace: str,
        repo_name: str,
        target_repo: str,
        project_url: str,
        user_login: str,
        comment: str,
        tag_name: str = "",
        base_ref: Optional[
            str
        ] = "master",  # default is master when working with issues
    ):
        super().__init__(project_url=project_url)
        self.action = action
        self.issue_id = issue_id
        self.repo_namespace = repo_namespace
        self.repo_name = repo_name
        self.base_ref = base_ref
        self._tag_name = tag_name
        self.target_repo = target_repo
        self.user_login = user_login
        self.comment = comment
        self.identifier = str(issue_id)

    @property
    def tag_name(self):
        if not self._tag_name:
            self._tag_name = ""
            if latest_release := self.project.get_latest_release():
                self._tag_name = latest_release.tag_name
        return self._tag_name

    @property
    def commit_sha(self):
        return self.tag_name

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["action"] = result["action"].value
        result["tag_name"] = self.tag_name
        result["issue_id"] = self.issue_id
        return result


class InstallationEvent(Event):
    def __init__(
        self,
        installation_id: int,
        account_login: str,
        account_id: int,
        account_url: str,
        account_type: str,
        created_at: Union[int, float, str],
        repositories: List[str],
        sender_id: int,
        sender_login: str,
        status: AllowlistStatus = AllowlistStatus.waiting,
    ):
        super().__init__(created_at)
        self.installation_id = installation_id
        # account == namespace (user/organization) into which the app has been installed
        self.account_login = account_login
        self.account_id = account_id
        self.account_url = account_url
        self.account_type = account_type
        # repos within the account/namespace
        self.repositories = repositories
        # sender == user who installed the app into 'account'
        self.sender_id = sender_id
        self.sender_login = sender_login
        self.status = status

    @classmethod
    def from_event_dict(cls, event: dict):
        return InstallationEvent(
            installation_id=event.get("installation_id"),
            account_login=event.get("account_login"),
            account_id=event.get("account_id"),
            account_url=event.get("account_url"),
            account_type=event.get("account_type"),
            created_at=event.get("created_at"),
            repositories=event.get("repositories"),
            sender_id=event.get("sender_id"),
            sender_login=event.get("sender_login"),
        )

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["status"] = result["status"].value
        return result

    @property
    def package_config(self):
        return None

    @property
    def project(self):
        return self.get_project()

    def get_project(self):
        return None


class DistGitCommitEvent(AbstractForgeIndependentEvent):
    def __init__(
        self,
        topic: str,
        repo_namespace: str,
        repo_name: str,
        branch: str,
        project_url: str,
        dg_repo_namespace: str,
        dg_repo_name: str,
        dg_branch: str,
        dg_rev: str,
        dg_project_url: str,
    ):
        super().__init__()
        self.topic = FedmsgTopic(topic)
        self.repo_namespace = repo_namespace
        self.repo_name = repo_name
        self.branch = branch
        self.project_url = project_url
        self.identifier = branch
        self.dg_repo_namespace = dg_repo_namespace
        self.dg_repo_name = dg_repo_name
        self.dg_branch = dg_branch
        self.dg_rev = dg_rev
        self.dg_project_url = dg_project_url

        self._package_config = None
        self._db_trigger: Optional[AbstractTriggerDbType] = None

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["topic"] = result["topic"].value
        result.pop("_db_trigger")
        return result

    @property
    def package_config(self):
        if not self._package_config:
            self._package_config = get_package_config_from_repo(
                self.project, self.branch
            )
        return self._package_config

    @property
    def db_trigger(self) -> Optional[AbstractTriggerDbType]:
        if not self._db_trigger:
            self._db_trigger = GitBranchModel.get_or_create(
                branch_name=self.dg_rev,
                namespace=self.dg_repo_namespace,
                repo_name=self.dg_repo_name,
                project_url=self.dg_project_url,
            )
        return self._db_trigger


class TestingFarmResultsEvent(AbstractForgeIndependentEvent):
    def __init__(
        self,
        pipeline_id: str,
        result: TestingFarmResult,
        compose: str,
        summary: str,
        log_url: str,
        copr_build_id: str,
        copr_chroot: str,
        tests: List[TestResult],
        commit_sha: str,
        project_url: str,
    ):
        super().__init__(project_url=project_url)
        self.pipeline_id = pipeline_id
        self.result = result
        self.compose = compose
        self.summary = summary
        self.log_url = log_url
        self.copr_build_id = copr_build_id
        self.copr_chroot = copr_chroot
        self.tests = tests
        self.commit_sha: str = commit_sha

        # Lazy properties
        self._pr_id: Optional[int] = None
        self._db_trigger: Optional[AbstractTriggerDbType] = None

    @property
    def pr_id(self) -> Optional[int]:
        if not self._pr_id and isinstance(self.db_trigger, PullRequestModel):
            self._pr_id = self.db_trigger.pr_id
        return self._pr_id

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["result"] = result["result"].value
        result["pr_id"] = self.pr_id
        result.pop("_db_trigger")
        return result

    @property
    def db_trigger(self) -> Optional[AbstractTriggerDbType]:
        if not self._db_trigger:
            run_model = TFTTestRunModel.get_by_pipeline_id(pipeline_id=self.pipeline_id)
            if run_model:
                self._db_trigger = run_model.get_trigger_object()
        return self._db_trigger

    def get_base_project(self) -> Optional[GitProject]:
        if self.pr_id is not None:
            if isinstance(self.project, PagureProject):
                pull_request = self.project.get_pr(pr_id=self.pr_id)
                return self.project.service.get_project(
                    namespace=self.project.namespace,
                    username=pull_request.author,
                    repo=self.project.repo,
                    is_fork=True,
                )
            else:
                return None  # With Github app, we cannot work with fork repo
        return self.project


class KojiBuildEvent(AbstractForgeIndependentEvent):
    def __init__(
        self,
        build_id: int,
        state: KojiBuildState,
        old_state: Optional[KojiBuildState] = None,
        rpm_build_task_id: Optional[int] = None,
        start_time: Optional[Union[int, float, str]] = None,
        completion_time: Optional[Union[int, float, str]] = None,
    ):
        super().__init__()
        self.build_id = build_id
        self.state = state
        self.old_state = old_state
        self.start_time: Optional[Union[int, float, str]] = start_time
        self.completion_time: Optional[Union[int, float, str]] = completion_time
        self.rpm_build_task_id = rpm_build_task_id

        # Lazy properties
        self._pr_id: Optional[int] = None
        self._commit_sha: Optional[str] = None
        self._db_trigger: Optional[AbstractTriggerDbType] = None
        self._identifier: Optional[str] = None
        self._build_model: Optional[KojiBuildModel] = None
        self._git_ref: Optional[str] = None

    @property
    def pr_id(self) -> Optional[int]:
        if not self._pr_id and isinstance(self.db_trigger, PullRequestModel):
            self._pr_id = self.db_trigger.pr_id
        return self._pr_id

    @property
    def commit_sha(self) -> Optional[str]:  # type:ignore
        if not self.build_model:
            return None

        # mypy does not like properties
        if not self._commit_sha:
            self._commit_sha = self.build_model.commit_sha
        return self._commit_sha

    @property
    def build_model(self) -> Optional[KojiBuildModel]:
        if not self._build_model:
            self._build_model = KojiBuildModel.get_by_build_id(build_id=self.build_id)
        return self._build_model

    @property
    def db_trigger(self) -> Optional[AbstractTriggerDbType]:
        if not self._db_trigger and self.build_model:
            self._db_trigger = self.build_model.get_trigger_object()
        return self._db_trigger

    @property
    def git_ref(self) -> str:
        if not self._git_ref:
            if isinstance(self.db_trigger, PullRequestModel):
                self._git_ref = self.commit_sha
            elif isinstance(self.db_trigger, ProjectReleaseModel):
                self._git_ref = self.db_trigger.tag_name
            elif isinstance(self.db_trigger, GitBranchModel):
                self._git_ref = self.db_trigger.name
            else:
                self._git_ref = self.commit_sha
        return self._git_ref

    @property
    def identifier(self) -> str:
        if not self._identifier:
            if isinstance(self.db_trigger, PullRequestModel):
                self._identifier = str(self.db_trigger.pr_id)
            elif isinstance(self.db_trigger, ProjectReleaseModel):
                self._identifier = self.db_trigger.tag_name
            elif isinstance(self.db_trigger, GitBranchModel):
                self._identifier = self.db_trigger.name
            else:
                self._identifier = self.commit_sha
        return self._identifier

    @classmethod
    def from_event_dict(cls, event: dict):
        return KojiBuildEvent(
            build_id=event.get("build_id"),
            state=KojiBuildState(event.get("state")) if event.get("state") else None,
            old_state=(
                KojiBuildState(event.get("old_state"))
                if event.get("old_state")
                else None
            ),
            rpm_build_task_id=event.get("rpm_build_task_id"),
            start_time=event.get("start_time"),
            completion_time=event.get("completion_time"),
        )

    def get_base_project(self) -> Optional[GitProject]:
        if self.pr_id is not None:
            if isinstance(self.project, PagureProject):
                pull_request = self.project.get_pr(pr_id=self.pr_id)
                return self.project.service.get_project(
                    namespace=self.project.namespace,
                    username=pull_request.author,
                    repo=self.project.repo,
                    is_fork=True,
                )
            else:
                return None  # With Github app, we cannot work with fork repo
        return self.project

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["state"] = result["state"].value
        result["old_state"] = result["old_state"].value
        result["commit_sha"] = self.commit_sha
        result["pr_id"] = self.pr_id
        result["git_ref"] = self.git_ref
        result["identifier"] = self.identifier
        result.pop("_build_model")
        result.pop("_db_trigger")
        return result

    def get_koji_build_logs_url(
        self, koji_logs_url: str = "https://kojipkgs.fedoraproject.org"
    ) -> Optional[str]:
        if not self.rpm_build_task_id:
            return None

        return (
            f"{koji_logs_url}//work/tasks/"
            f"{self.rpm_build_task_id % 10000}/{self.rpm_build_task_id}/build.log"
        )

    def get_koji_rpm_build_web_url(
        self, koji_web_url: str = "https://koji.fedoraproject.org"
    ) -> Optional[str]:
        if not self.rpm_build_task_id:
            return None

        return f"{koji_web_url}/koji/taskinfo?taskID={self.rpm_build_task_id}"


class AbstractCoprBuildEvent(AbstractForgeIndependentEvent):
    build: Optional[CoprBuildModel]

    def __init__(
        self,
        topic: str,
        build_id: int,
        build: CoprBuildModel,
        chroot: str,
        status: int,
        owner: str,
        project_name: str,
        pkg: str,
        timestamp,
    ):
        trigger_db = build.get_trigger_object()
        self.commit_sha = build.commit_sha
        self.base_repo_name = trigger_db.project.repo_name
        self.base_repo_namespace = trigger_db.project.namespace
        git_ref = self.commit_sha  # ref should be name of the branch, not a hash
        self.topic = FedmsgTopic(topic)

        trigger_db = build.get_trigger_object()
        pr_id = None
        if trigger_db.job_trigger_model_type == JobTriggerModelType.pull_request:
            pr_id = trigger_db.pr_id  # type: ignore
            self.identifier = str(trigger_db.pr_id)  # type: ignore
        elif trigger_db.job_trigger_model_type == JobTriggerModelType.release:
            pr_id = None
            self.identifier = trigger_db.tag_name  # type: ignore
        elif trigger_db.job_trigger_model_type == JobTriggerModelType.branch_push:
            pr_id = None
            self.identifier = trigger_db.name  # type: ignore

        super().__init__(project_url=trigger_db.project.project_url, pr_id=pr_id)

        self.git_ref = git_ref
        self.build_id = build_id
        self.build = build
        self.chroot = chroot
        self.status = status
        self.owner = owner
        self.project_name = project_name
        self.pkg = pkg
        self.timestamp = timestamp

    @property
    def db_trigger(self) -> Optional[AbstractTriggerDbType]:
        return self.build.get_trigger_object()

    def get_base_project(self) -> Optional[GitProject]:
        if self.pr_id is not None:
            if isinstance(self.project, PagureProject):
                pull_request = self.project.get_pr(pr_id=self.pr_id)
                return self.project.service.get_project(
                    namespace=self.project.namespace,
                    username=pull_request.author,
                    repo=self.project.repo,
                    is_fork=True,
                )
            else:
                return None  # With Github app, we cannot work with fork repo
        return self.project

    @classmethod
    def from_build_id(
        cls,
        topic: str,
        build_id: int,
        chroot: str,
        status: int,
        owner: str,
        project_name: str,
        pkg: str,
        timestamp,
    ) -> Optional["AbstractCoprBuildEvent"]:
        """Return cls instance or None if build_id not in CoprBuildDB"""
        build = CoprBuildModel.get_by_build_id(str(build_id), chroot)
        if not build:
            logger.warning(f"Build id {build_id} not in CoprBuildDB.")
            return None

        return cls(
            topic, build_id, build, chroot, status, owner, project_name, pkg, timestamp
        )

    @classmethod
    def from_event_dict(cls, event: dict):
        return AbstractCoprBuildEvent.from_build_id(
            topic=event.get("topic"),
            build_id=event.get("build_id"),
            chroot=event.get("chroot"),
            status=event.get("status"),
            owner=event.get("owner"),
            project_name=event.get("project_name"),
            pkg=event.get("pkg"),
            timestamp=event.get("timestamp"),
        )

    def pre_check(self):
        if not self.build:
            logger.warning("Copr build is not handled by this deployment.")
            return False

        return True

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["topic"] = result["topic"].value
        result.pop("build")
        return result

    def get_copr_build_url(self) -> str:
        return (
            "https://copr.fedorainfracloud.org/coprs/"
            f"{self.owner}/{self.project_name}/build/{self.build_id}/"
        )

    def get_copr_build_logs_url(self) -> str:
        return (
            f"https://copr-be.cloud.fedoraproject.org/results/{self.owner}/"
            f"{self.project_name}/{self.chroot}/"
            f"{self.build_id:08d}-{self.pkg}/builder-live.log.gz"
        )


class CoprBuildStartEvent(AbstractCoprBuildEvent):
    pass


class CoprBuildEndEvent(AbstractCoprBuildEvent):
    pass


class AbstractPagureEvent(AbstractForgeIndependentEvent):
    def __init__(self, project_url: str, pr_id: Optional[int] = None):
        super().__init__(pr_id=pr_id)
        self.project_url: str = project_url
        self.git_ref: Optional[str] = None  # git ref that can be 'git checkout'-ed
        self.identifier: Optional[
            str
        ] = None  # will be shown to users -- e.g. in logs or in the copr-project name


class PushPagureEvent(AbstractPagureEvent):
    def __init__(
        self,
        repo_namespace: str,
        repo_name: str,
        git_ref: str,
        project_url: str,
        commit_sha: str,
    ):
        super().__init__(project_url=project_url)
        self.repo_namespace = repo_namespace
        self.repo_name = repo_name
        self.git_ref = git_ref
        self.commit_sha = commit_sha
        self.identifier = git_ref


class PullRequestCommentPagureEvent(AddPullRequestDbTrigger, AbstractPagureEvent):
    def __init__(
        self,
        action: PullRequestCommentAction,
        pr_id: int,
        base_repo_namespace: str,
        base_repo_name: str,
        base_repo_owner: str,
        base_ref: Optional[str],
        target_repo: str,
        project_url: str,
        user_login: str,
        comment: str,
        commit_sha: str = "",
    ):
        super().__init__(project_url=project_url, pr_id=pr_id)
        self.action = action
        self.base_repo_namespace = base_repo_namespace
        self.base_repo_name = base_repo_name
        self.base_repo_owner = base_repo_owner
        self.base_ref = base_ref
        self.commit_sha = commit_sha
        self.target_repo = target_repo
        self.user_login = user_login
        self.comment = comment
        self.identifier = str(pr_id)
        self.git_ref = None  # pr_id will be used for checkout

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["action"] = result["action"].value
        return result

    def get_base_project(self) -> GitProject:
        fork = self.project.service.get_project(
            namespace=self.base_repo_namespace,
            repo=self.base_repo_name,
            username=self.base_repo_owner,
            is_fork=True,
        )
        logger.debug(f"Base project: {fork} owned by {self.base_repo_owner}")
        return fork


class PullRequestPagureEvent(AddPullRequestDbTrigger, AbstractPagureEvent):
    def __init__(
        self,
        action: PullRequestAction,
        pr_id: int,
        base_repo_namespace: str,
        base_repo_name: str,
        base_repo_owner: str,
        base_ref: str,
        target_repo: str,
        project_url: str,
        commit_sha: str,
        user_login: str,
    ):
        super().__init__(project_url=project_url, pr_id=pr_id)
        self.action = action
        self.base_repo_namespace = base_repo_namespace
        self.base_repo_name = base_repo_name
        self.base_repo_owner = base_repo_owner
        self.base_ref = base_ref
        self.target_repo = target_repo
        self.commit_sha = commit_sha
        self.user_login = user_login
        self.identifier = str(pr_id)
        self.git_ref = None  # pr_id will be used for checkout
        self.project_url = project_url

    def get_dict(self, default_dict: Optional[Dict] = None) -> dict:
        result = super().get_dict()
        result["action"] = result["action"].value
        return result

    def get_base_project(self) -> GitProject:
        fork = self.project.service.get_project(
            namespace=self.base_repo_namespace,
            repo=self.base_repo_name,
            username=self.base_repo_owner,
            is_fork=True,
        )
        logger.debug(f"Base project: {fork} owned by {self.base_repo_owner}")
        return fork
