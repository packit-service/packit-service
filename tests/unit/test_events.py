# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

"""
Tests for events parsing
"""

import json
from datetime import datetime, timezone

import pytest
from flexmock import flexmock
from ogr import PagureService
from ogr.services.github import GithubProject, GithubService
from ogr.services.gitlab import GitlabProject, GitlabService
from ogr.services.pagure import PagureProject

from packit_service.config import ServiceConfig, PackageConfigGetter, ProjectToSync
from packit_service.constants import KojiBuildState
from packit_service.models import (
    CoprBuildModel,
    KojiBuildModel,
    TestingFarmResult,
    TFTTestRunModel,
)
from packit_service.service.db_triggers import AddPullRequestDbTrigger
from packit_service.service.events import (
    AllowlistStatus,
    InstallationEvent,
    ReleaseEvent,
    PullRequestGithubEvent,
    PullRequestAction,
    TestingFarmResultsEvent,
    PullRequestCommentGithubEvent,
    PullRequestCommentAction,
    IssueCommentEvent,
    IssueCommentAction,
    AbstractCoprBuildEvent,
    FedmsgTopic,
    DistGitCommitEvent,
    PushGitHubEvent,
    PullRequestPagureEvent,
    PullRequestCommentPagureEvent,
    MergeRequestGitlabEvent,
    GitlabEventAction,
    KojiBuildEvent,
    IssueCommentGitlabEvent,
    MergeRequestCommentGitlabEvent,
    PushGitlabEvent,
    EventData,
)
from packit_service.worker.parser import Parser, CentosEventParser
from packit_service.worker.testing_farm import TestingFarmJobHelper
from tests.conftest import copr_build_model
from tests.spellbook import DATA_DIR


@pytest.fixture(scope="module")
def copr_build_results_start():
    with open(DATA_DIR / "fedmsg" / "copr_build_start.json") as outfile:
        return json.load(outfile)


@pytest.fixture(scope="module")
def copr_build_results_end():
    with open(DATA_DIR / "fedmsg" / "copr_build_end.json") as outfile:
        return json.load(outfile)


class TestEvents:
    @pytest.fixture()
    def github_installation(self, request):
        file = f"installation_{request.param}.json"
        with open(DATA_DIR / "webhooks" / "github" / file) as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def github_issue_comment_propose_downstream(self):
        with open(
            DATA_DIR / "webhooks" / "github" / "issue_propose_downstream.json"
        ) as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def testing_farm_notification(self):
        with open(
            DATA_DIR / "webhooks" / "testing_farm" / "notification.json"
        ) as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def testing_farm_results(self):
        with open(DATA_DIR / "webhooks" / "testing_farm" / "results.json") as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def testing_farm_results_error(self):
        with open(
            DATA_DIR / "webhooks" / "testing_farm" / "results_error.json"
        ) as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def github_pr_comment_created(self):
        with open(
            DATA_DIR / "webhooks" / "github" / "pr_comment_copr_build.json"
        ) as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def merge_request(self):
        with open(DATA_DIR / "webhooks" / "gitlab" / "mr_event.json") as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def merge_request_update(self):
        with open(DATA_DIR / "webhooks" / "gitlab" / "mr_update_event.json") as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def github_pr_comment_empty(self):
        with open(
            DATA_DIR / "webhooks" / "github" / "pr_comment_empty.json"
        ) as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def github_push(self):
        with open(DATA_DIR / "webhooks" / "github" / "push.json") as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def github_push_branch(self):
        with open(DATA_DIR / "webhooks" / "github" / "push_branch.json") as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def gitlab_push(self):
        with open(
            DATA_DIR / "webhooks" / "gitlab" / "push_with_one_commit.json"
        ) as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def gitlab_push_many_commits(self):
        with open(
            DATA_DIR / "webhooks" / "gitlab" / "push_with_many_commits.json"
        ) as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def gitlab_issue_comment(self):
        with open(DATA_DIR / "webhooks" / "gitlab" / "issue_comment.json") as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def gitlab_mr_comment(self):
        with open(DATA_DIR / "webhooks" / "gitlab" / "mr_comment.json") as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def distgit_commit(self):
        with open(DATA_DIR / "fedmsg" / "distgit_commit.json") as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def koji_build_scratch_start(self):
        with open(
            DATA_DIR / "fedmsg" / "koji_build_scratch_start.json", "r"
        ) as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def koji_build_scratch_end(self):
        with open(DATA_DIR / "fedmsg" / "koji_build_scratch_end.json", "r") as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def mock_config(self):
        service_config = ServiceConfig()
        service_config.services = {
            GithubService(token="token"),
            GitlabService(token="token"),
        }
        service_config.github_requests_log_path = "/path"
        ServiceConfig.service_config = service_config

    # https://stackoverflow.com/questions/35413134/what-does-indirect-true-false-in-pytest-mark-parametrize-do-mean
    @pytest.mark.parametrize("github_installation", ["added", "created"], indirect=True)
    def test_parse_installation(self, github_installation):
        event_object = Parser.parse_event(github_installation)

        assert isinstance(event_object, InstallationEvent)
        assert event_object.installation_id == 1708454
        assert event_object.account_login == "packit-service"
        assert event_object.account_id == 46870917
        assert event_object.account_url == "https://api.github.com/users/packit-service"
        assert event_object.account_type == "Organization"
        assert event_object.created_at == datetime.fromtimestamp(
            1567090283, timezone.utc
        )
        assert event_object.sender_login == "jpopelka"
        assert event_object.sender_id == 288686
        assert event_object.status == AllowlistStatus.waiting
        assert event_object.repositories == ["jpopelka/brewutils"]

    def test_parse_release(self, github_release_webhook):
        event_object = Parser.parse_event(github_release_webhook)

        assert isinstance(event_object, ReleaseEvent)
        assert event_object.repo_namespace == "packit-service"
        assert event_object.repo_name == "hello-world"
        assert event_object.tag_name == "0.3.0"
        assert (
            event_object.project_url == "https://github.com/packit-service/hello-world"
        )

    def test_parse_mr(self, merge_request):
        event_object = Parser.parse_event(merge_request)

        assert isinstance(event_object, MergeRequestGitlabEvent)
        assert event_object.action == GitlabEventAction.opened
        assert event_object.object_id == 58759529
        assert event_object.identifier == "1"
        assert event_object.source_repo_namespace == "testing/packit"
        assert event_object.source_repo_name == "hello-there"
        assert event_object.source_repo_branch == "test"
        assert event_object.commit_sha == "1f6a716aa7a618a9ffe56970d77177d99d100022"
        assert event_object.target_repo_namespace == "testing/packit"
        assert event_object.target_repo_name == "hello-there"
        assert event_object.target_repo_branch == "master"
        assert (
            event_object.project_url == "https://gitlab.com/testing/packit/hello-there"
        )

        assert isinstance(event_object.project, GitlabProject)
        assert event_object.project.full_repo_name == "testing/packit/hello-there"
        assert event_object.base_project.full_repo_name == "testing/packit/hello-there"

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=1,
            reference="1f6a716aa7a618a9ffe56970d77177d99d100022",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()
        assert event_object.package_config

    def test_parse_mr_action(self, merge_request_update):
        event_object = Parser.parse_event(merge_request_update)
        assert isinstance(event_object, MergeRequestGitlabEvent)
        assert event_object.action == GitlabEventAction.update
        assert event_object.commit_sha == "45e272a57335e4e308f3176df6e9226a9e7805a9"
        assert event_object.identifier == "2"

        assert isinstance(event_object.project, GitlabProject)
        assert event_object.project.full_repo_name == "testing/packit/hello-there"
        assert event_object.base_project.full_repo_name == "testing/packit/hello-there"

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=2,
            reference="45e272a57335e4e308f3176df6e9226a9e7805a9",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()
        assert event_object.package_config

    def test_parse_pr(self, github_pr_webhook):
        event_object = Parser.parse_event(github_pr_webhook)

        assert isinstance(event_object, PullRequestGithubEvent)
        assert event_object.action == PullRequestAction.opened
        assert event_object.pr_id == 342
        assert event_object.base_repo_namespace == "lbarcziova"
        assert event_object.base_repo_name == "packit"
        assert event_object.base_ref == "528b803be6f93e19ca4130bf4976f2800a3004c4"
        assert event_object.target_repo_namespace == "packit-service"
        assert event_object.target_repo_name == "packit"
        assert event_object.project_url == "https://github.com/packit-service/packit"
        assert event_object.commit_sha == "528b803be6f93e19ca4130bf4976f2800a3004c4"

        assert isinstance(event_object.project, GithubProject)
        assert event_object.project.full_repo_name == "packit-service/packit"
        assert (
            not event_object.base_project  # With Github app, we cannot work with fork repo
        )

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=None,
            project=event_object.project,
            pr_id=342,
            reference="528b803be6f93e19ca4130bf4976f2800a3004c4",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()
        assert event_object.package_config

    def test_parse_pr_comment_created(self, github_pr_comment_created):
        event_object = Parser.parse_event(github_pr_comment_created)

        assert isinstance(event_object, PullRequestCommentGithubEvent)
        assert event_object.action == PullRequestCommentAction.created
        assert event_object.pr_id == 9
        assert event_object.base_repo_namespace == "phracek"
        assert event_object.base_repo_name is None  # It's not present in the payload
        assert event_object.target_repo_namespace == "packit-service"
        assert event_object.target_repo_name == "hello-world"
        assert (
            event_object.project_url == "https://github.com/packit-service/hello-world"
        )
        assert event_object.user_login == "phracek"
        assert event_object.comment == "/packit copr-build"

        assert isinstance(event_object.project, GithubProject)
        assert event_object.project.full_repo_name == "packit-service/hello-world"
        assert (
            not event_object.base_project  # With Github app, we cannot work with fork repo
        )

        flexmock(GithubProject).should_receive("get_pr").with_args(pr_id=9).and_return(
            flexmock(head_commit="12345")
        )

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=None,
            project=event_object.project,
            pr_id=9,
            reference="12345",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()
        assert event_object.package_config

    def test_parse_mr_comment(self, gitlab_mr_comment):
        event_object = Parser.parse_event(gitlab_mr_comment)

        assert isinstance(event_object, MergeRequestCommentGitlabEvent)
        assert event_object.action == GitlabEventAction.opened
        assert event_object.pr_id == 2
        assert event_object.source_repo_namespace == "testing/packit"
        assert event_object.source_repo_name == "hello-there"
        assert event_object.target_repo_namespace == "testing/packit"
        assert event_object.target_repo_name == "hello-there"
        assert (
            event_object.project_url == "https://gitlab.com/testing/packit/hello-there"
        )
        assert event_object.user_login == "shreyaspapi"
        assert event_object.comment == "must be reopened"
        assert event_object.commit_sha == "45e272a57335e4e308f3176df6e9226a9e7805a9"

        assert isinstance(event_object.project, GitlabProject)
        assert event_object.project.full_repo_name == "testing/packit/hello-there"

        assert isinstance(event_object.project, GitlabProject)
        assert event_object.base_project.full_repo_name == "testing/packit/hello-there"

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=2,
            reference="45e272a57335e4e308f3176df6e9226a9e7805a9",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()
        assert event_object.package_config

    def test_parse_pr_comment_empty(self, github_pr_comment_empty):
        event_object = Parser.parse_event(github_pr_comment_empty)

        assert isinstance(event_object, PullRequestCommentGithubEvent)
        assert event_object.action == PullRequestCommentAction.created
        assert event_object.pr_id == 9
        assert event_object.base_repo_namespace == "phracek"
        assert event_object.base_repo_name is None  # It's not present in the payload
        assert event_object.target_repo_namespace == "packit-service"
        assert event_object.target_repo_name == "hello-world"
        assert (
            event_object.project_url == "https://github.com/packit-service/hello-world"
        )
        assert event_object.user_login == "phracek"
        assert event_object.comment == ""

        assert isinstance(event_object.project, GithubProject)
        assert event_object.project.full_repo_name == "packit-service/hello-world"
        assert (
            not event_object.base_project  # With Github app, we cannot work with fork repo
        )

        flexmock(GithubProject).should_receive("get_pr").with_args(pr_id=9).and_return(
            flexmock(head_commit="12345")
        )

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=None,
            project=event_object.project,
            pr_id=9,
            reference="12345",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()

        assert event_object.package_config

    def test_parse_issue_comment(self, github_issue_comment_propose_downstream):
        event_object = Parser.parse_event(github_issue_comment_propose_downstream)

        assert isinstance(event_object, IssueCommentEvent)
        assert event_object.action == IssueCommentAction.created
        assert event_object.issue_id == 512
        assert event_object.repo_namespace == "packit-service"
        assert event_object.repo_name == "packit"
        assert (
            event_object.target_repo
            == f"{event_object.repo_namespace}/{event_object.repo_name}"
        )
        assert event_object.base_ref == "master"
        assert event_object.project_url == "https://github.com/packit-service/packit"
        assert event_object.user_login == "phracek"
        assert event_object.comment == "/packit propose-downstream"

        assert isinstance(event_object.project, GithubProject)
        assert event_object.project.full_repo_name == "packit-service/packit"
        assert not event_object.base_project

        flexmock(event_object.project).should_receive("get_latest_release").and_return(
            flexmock(tag_name="0.5.0")
        )
        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=None,
            reference="0.5.0",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()
        assert event_object.package_config

    def test_parse_gitlab_issue_comment(self, gitlab_issue_comment):
        event_object = Parser.parse_event(gitlab_issue_comment)

        assert isinstance(event_object, IssueCommentGitlabEvent)
        assert event_object.action == GitlabEventAction.opened
        assert event_object.issue_id == 1
        assert event_object.repo_namespace == "testing/packit"
        assert event_object.repo_name == "hello-there"

        assert (
            event_object.project_url == "https://gitlab.com/testing/packit/hello-there"
        )
        assert event_object.user_login == "shreyaspapi"
        assert event_object.comment == "testing comment"

        assert isinstance(event_object.project, GitlabProject)
        assert event_object.project.full_repo_name == "testing/packit/hello-there"

        flexmock(event_object.project).should_receive("get_latest_release").and_return(
            flexmock(tag_name="0.5.0")
        )
        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=None,
            reference="0.5.0",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()
        assert event_object.package_config

    def test_parse_github_push(self, github_push_branch):
        event_object = Parser.parse_event(github_push_branch)

        assert isinstance(event_object, PushGitHubEvent)
        assert event_object.repo_namespace == "packit-service"
        assert event_object.repo_name == "hello-world"
        assert event_object.commit_sha == "04885ff850b0fa0e206cd09db73565703d48f99b"
        assert (
            event_object.project_url == "https://github.com/packit-service/hello-world"
        )
        assert event_object.git_ref == "build-branch"

        assert isinstance(event_object.project, GithubProject)
        assert event_object.project.full_repo_name == "packit-service/hello-world"
        assert not event_object.base_project

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=None,
            reference="04885ff850b0fa0e206cd09db73565703d48f99b",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()
        assert event_object.package_config

    def test_parse_gitlab_push(self, gitlab_push):
        event_object = Parser.parse_event(gitlab_push)

        assert isinstance(event_object, PushGitlabEvent)
        assert event_object.repo_namespace == "testing/packit"
        assert event_object.repo_name == "hello-there"
        assert event_object.commit_sha == "cb2859505e101785097e082529dced35bbee0c8f"
        assert (
            event_object.project_url == "https://gitlab.com/testing/packit/hello-there"
        )
        assert event_object.git_ref == "test2"

        assert isinstance(event_object.project, GitlabProject)
        assert event_object.project.full_repo_name == "testing/packit/hello-there"
        assert not event_object.base_project

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=None,
            reference="cb2859505e101785097e082529dced35bbee0c8f",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()
        assert event_object.package_config

    def test_parse_gitlab_push_many_commits(self, gitlab_push_many_commits):
        event_object = Parser.parse_event(gitlab_push_many_commits)

        assert isinstance(event_object, PushGitlabEvent)
        assert event_object.repo_namespace == "mike"
        assert event_object.repo_name == "diaspora"
        assert event_object.commit_sha == "da1560886d4f094c3e6c9ef40349f7d38b5d27d7"
        assert event_object.project_url == "http://gitlab.com/mike/diaspora"
        assert event_object.git_ref == "master"

        assert isinstance(event_object.project, GitlabProject)
        assert event_object.project.full_repo_name == "mike/diaspora"
        assert not event_object.base_project

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=None,
            reference="da1560886d4f094c3e6c9ef40349f7d38b5d27d7",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()
        assert event_object.package_config

    def test_parse_github_push_branch(self, github_push_branch):
        event_object = Parser.parse_event(github_push_branch)

        assert isinstance(event_object, PushGitHubEvent)
        assert event_object.repo_namespace == "packit-service"
        assert event_object.repo_name == "hello-world"
        assert event_object.commit_sha == "04885ff850b0fa0e206cd09db73565703d48f99b"
        assert (
            event_object.project_url == "https://github.com/packit-service/hello-world"
        )
        assert event_object.git_ref == "build-branch"

        assert isinstance(event_object.project, GithubProject)
        assert event_object.project.full_repo_name == "packit-service/hello-world"
        assert not event_object.base_project

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=None,
            reference="04885ff850b0fa0e206cd09db73565703d48f99b",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()

        assert event_object.package_config

    def test_parse_testing_farm_notification(
        self, testing_farm_notification, testing_farm_results
    ):
        request_id = "129bd474-e4d3-49e0-9dec-d994a99feebc"
        flexmock(TestingFarmJobHelper).should_receive("get_request_details").with_args(
            request_id
        ).and_return(testing_farm_results)
        flexmock(TFTTestRunModel).should_receive("get_by_pipeline_id").and_return(
            flexmock(
                job_trigger=flexmock(),
                data={"base_project_url": "https://github.com/packit/packit"},
                commit_sha="12345",
            )
            .should_receive("get_trigger_object")
            .and_return(flexmock(pr_id=10))
            .once()
            .mock()
        )
        event_object = Parser.parse_event(testing_farm_notification)

        assert isinstance(event_object, TestingFarmResultsEvent)
        assert event_object.pipeline_id == request_id
        assert event_object.result == TestingFarmResult.passed
        assert event_object.project_url == "https://github.com/packit/packit"
        assert event_object.commit_sha == "12345"
        assert not event_object.summary
        assert event_object.compose == "Fedora-32"
        assert event_object.copr_build_id == "1810530"
        assert event_object.copr_chroot == "fedora-32-x86_64"
        assert event_object.tests
        assert event_object.db_trigger
        assert isinstance(event_object.project, GithubProject)
        assert event_object.project.full_repo_name == "packit/packit"

    def test_parse_testing_farm_notification_error(
        self, testing_farm_notification, testing_farm_results_error
    ):
        request_id = "129bd474-e4d3-49e0-9dec-d994a99feebc"
        flexmock(TestingFarmJobHelper).should_receive("get_request_details").with_args(
            request_id
        ).and_return(testing_farm_results_error)
        flexmock(TFTTestRunModel).should_receive("get_by_pipeline_id").and_return(
            flexmock(
                job_trigger=flexmock(),
                data={"base_project_url": "https://github.com/packit/packit"},
                commit_sha="12345",
            )
            .should_receive("get_trigger_object")
            .and_return(flexmock(pr_id=10))
            .once()
            .mock()
        )
        event_object = Parser.parse_event(testing_farm_notification)

        assert isinstance(event_object, TestingFarmResultsEvent)
        assert event_object.pipeline_id == request_id
        assert event_object.result == TestingFarmResult.error
        assert event_object.project_url == "https://github.com/packit/packit"
        assert event_object.commit_sha == "12345"
        assert event_object.summary == "something went wrong"
        assert event_object.compose == "Fedora-32"
        assert event_object.copr_build_id == "1810530"
        assert event_object.copr_chroot == "fedora-32-x86_64"
        assert event_object.tests
        assert event_object.db_trigger
        assert isinstance(event_object.project, GithubProject)
        assert event_object.project.full_repo_name == "packit/packit"

    def test_parse_copr_build_event_start(
        self, copr_build_results_start, copr_build_pr
    ):
        flexmock(CoprBuildModel).should_receive("get_by_build_id").and_return(
            copr_build_pr
        )

        event_object = Parser.parse_event(copr_build_results_start)

        assert isinstance(event_object, AbstractCoprBuildEvent)
        assert event_object.topic == FedmsgTopic.copr_build_started
        assert event_object.build_id == 1044215
        assert event_object.chroot == "fedora-rawhide-x86_64"
        assert event_object.status == 3
        assert event_object.owner == "packit"
        assert event_object.project_name == "packit-service-hello-world-24-stg"
        assert event_object.project_url == "https://github.com/foo/bar"
        assert event_object.base_repo_name == "bar"
        assert event_object.base_repo_namespace == "foo"
        assert event_object.pkg == "hello"

        assert isinstance(event_object.project, GithubProject)
        assert event_object.project.full_repo_name == "foo/bar"

        assert (
            not event_object.base_project  # With Github app, we cannot work with fork repo
        )

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=None,
            project=event_object.project,
            pr_id=123,
            reference="0011223344",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()

        assert event_object.package_config

    def test_parse_copr_build_event_end(self, copr_build_results_end, copr_build_pr):
        flexmock(CoprBuildModel).should_receive("get_by_build_id").and_return(
            copr_build_pr
        )

        event_object = Parser.parse_event(copr_build_results_end)

        assert isinstance(event_object, AbstractCoprBuildEvent)
        assert event_object.topic == FedmsgTopic.copr_build_finished
        assert event_object.build_id == 1044215
        assert event_object.chroot == "fedora-rawhide-x86_64"
        assert event_object.status == 1
        assert event_object.owner == "packit"
        assert event_object.project_name == "packit-service-hello-world-24-stg"
        assert event_object.base_repo_name == "bar"
        assert event_object.base_repo_namespace == "foo"
        assert event_object.pkg == "hello"
        assert event_object.git_ref == "0011223344"

        flexmock(GithubProject).should_receive("get_pr").with_args(
            pr_id=123
        ).and_return(flexmock(author="the-fork"))
        assert isinstance(event_object.project, GithubProject)
        assert event_object.project.full_repo_name == "foo/bar"

        assert (
            not event_object.base_project  # With Github app, we cannot work with fork repo
        )

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=None,
            project=event_object.project,
            pr_id=123,
            reference="0011223344",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()

        assert event_object.package_config

    def test_parse_koji_build_scratch_event_start(
        self, koji_build_scratch_start, koji_build_pr
    ):
        flexmock(KojiBuildModel).should_receive("get_by_build_id").and_return(
            koji_build_pr
        )

        event_object = Parser.parse_event(koji_build_scratch_start)

        assert isinstance(event_object, KojiBuildEvent)
        assert event_object.build_id == 45270170
        assert event_object.state == KojiBuildState.open
        assert not event_object.rpm_build_task_id

        assert isinstance(event_object.project, GithubProject)
        assert event_object.project.full_repo_name == "foo/bar"

        """
        assert (
            not event_object.base_project  # With Github app, we cannot work with fork repo
        )

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=None,
            project=event_object.project,
            pr_id=123,
            reference="0011223344",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()

        assert event_object.package_config
        """

    def test_parse_koji_build_scratch_event_end(
        self, koji_build_scratch_end, koji_build_pr
    ):
        flexmock(KojiBuildModel).should_receive("get_by_build_id").and_return(
            koji_build_pr
        )

        event_object = Parser.parse_event(koji_build_scratch_end)

        assert isinstance(event_object, KojiBuildEvent)
        assert event_object.build_id == 45270170
        assert event_object.state == KojiBuildState.closed
        assert event_object.rpm_build_task_id == 45270227

        flexmock(GithubProject).should_receive("get_pr").with_args(
            pr_id=123
        ).and_return(flexmock(author="the-fork"))
        assert isinstance(event_object.project, GithubProject)
        assert event_object.project.full_repo_name == "foo/bar"

        """
        assert (
            not event_object.base_project  # With Github app, we cannot work with fork repo
        )

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=None,
            project=event_object.project,
            pr_id=123,
            reference="0011223344",
            fail_when_missing=False,
            spec_file_path=None,
        ).and_return(
            flexmock()
        ).once()

        assert event_object.package_config
        """

    def test_get_project_pr(self, github_pr_webhook, mock_config):
        event_object = Parser.parse_event(github_pr_webhook)

        assert isinstance(event_object, PullRequestGithubEvent)

        assert isinstance(event_object.project, GithubProject)
        assert isinstance(event_object.project.service, GithubService)
        assert event_object.project.namespace == "packit-service"
        assert event_object.project.repo == "packit"

    def test_get_project_release(self, github_release_webhook, mock_config):
        event_object = Parser.parse_event(github_release_webhook)

        assert isinstance(event_object, ReleaseEvent)

        assert isinstance(event_object.project, GithubProject)
        assert isinstance(event_object.project.service, GithubService)
        assert event_object.project.namespace == "packit-service"
        assert event_object.project.repo == "hello-world"

    def test_get_project_testing_farm_notification(
        self, testing_farm_notification, testing_farm_results, mock_config
    ):
        request_id = "129bd474-e4d3-49e0-9dec-d994a99feebc"
        flexmock(TestingFarmJobHelper).should_receive("get_request_details").with_args(
            request_id
        ).and_return(testing_farm_results)
        flexmock(TFTTestRunModel).should_receive("get_by_pipeline_id").with_args(
            request_id
        ).and_return(flexmock(data={"base_project_url": "abc"}, commit_sha="12345"))
        event_object = Parser.parse_event(testing_farm_notification)

        assert isinstance(event_object, TestingFarmResultsEvent)
        assert isinstance(event_object.pipeline_id, str)
        assert event_object.pipeline_id == request_id
        assert event_object.project_url == "abc"
        assert event_object.commit_sha == "12345"

    def test_distgit_commit(self, distgit_commit):

        flexmock(ServiceConfig).should_receive("get_service_config").and_return(
            ServiceConfig(
                projects_to_sync=[
                    ProjectToSync(
                        forge="https://github.com",
                        repo_namespace="example",
                        repo_name="upstream_buildah",
                        branch="aaa",
                        dg_branch="master",
                        dg_repo_name="buildah",
                    )
                ]
            )
        )
        event_object = Parser.parse_event(distgit_commit)

        assert isinstance(event_object, DistGitCommitEvent)
        assert event_object.topic == FedmsgTopic.dist_git_push
        assert event_object.dg_repo_namespace == "rpms"
        assert event_object.dg_repo_name == "buildah"
        assert event_object.dg_rev == "abcd"
        assert event_object.dg_branch == "master"
        assert (
            event_object.dg_project_url == "https://src.fedoraproject.org/rpms/buildah"
        )
        assert event_object.repo_namespace == "example"
        assert event_object.repo_name == "upstream_buildah"
        assert event_object.branch == "aaa"
        assert event_object.project_url == "https://github.com/example/upstream_buildah"

    def test_json_testing_farm_notification(
        self, testing_farm_notification, testing_farm_results
    ):
        flexmock(TestingFarmJobHelper).should_receive("get_request_details").and_return(
            testing_farm_results
        )
        flexmock(TFTTestRunModel).should_receive("get_by_pipeline_id").and_return(
            flexmock(data={"base_project_url": "abc"}, commit_sha="12345")
        )
        event_object = Parser.parse_event(testing_farm_notification)
        assert json.dumps(event_object.pipeline_id)


class TestCentOSEventParser:
    @classmethod
    def setup_class(cls):
        service_config = ServiceConfig()
        service_config.services = {
            GithubService(token="12345"),
            PagureService(instance_url="https://git.stg.centos.org", token="6789"),
        }
        service_config.github_requests_log_path = "/path"
        ServiceConfig.service_config = service_config

    @pytest.fixture()
    def copr_build_centos_pr(self):
        return copr_build_model(
            repo_name="packit-hello-world",
            repo_namespace="source-git",
            forge_instance="git.stg.centos.org",
        )

    @pytest.fixture()
    def pagure_pr_new(self):
        with open(DATA_DIR / "centosmsg" / "pull-request.new.json") as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def pagure_pr_update(self):
        with open(DATA_DIR / "centosmsg" / "pull-request.updated.json") as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def pagure_pr_comment_added(self):
        with open(
            DATA_DIR / "centosmsg" / "pull-request.comment.added.json"
        ) as outfile:
            return json.load(outfile)

    @pytest.fixture()
    def pagure_pr_tag_added(self):
        with open(DATA_DIR / "centosmsg" / "pull-request.tag.added.json") as outfile:
            return json.load(outfile)

    def test_new_pull_request_event(self, pagure_pr_new):
        centos_event_parser = CentosEventParser()
        event_object = centos_event_parser.parse_event(pagure_pr_new)

        assert isinstance(event_object, PullRequestPagureEvent)
        assert event_object.action == PullRequestAction.opened
        assert event_object.pr_id == 12
        assert event_object.base_repo_namespace == "source-git"
        assert event_object.base_repo_name == "packit-hello-world"
        assert event_object.base_repo_owner == "sakalosj"
        assert event_object.base_ref == "master"
        assert event_object.target_repo == "packit-hello-world"
        assert event_object.commit_sha == "bf9701dea5a167caa7a1afa0759342aa0bf0d8fd"
        assert event_object.user_login == "sakalosj"
        assert event_object.identifier == "12"
        assert (
            event_object.project_url
            == "https://git.stg.centos.org/source-git/packit-hello-world"
        )

        assert isinstance(event_object.project, PagureProject)
        assert event_object.project.full_repo_name == "source-git/packit-hello-world"
        assert isinstance(event_object.base_project, PagureProject)
        assert (
            event_object.base_project.full_repo_name
            == "fork/sakalosj/source-git/packit-hello-world"
        )

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=12,
            reference="bf9701dea5a167caa7a1afa0759342aa0bf0d8fd",
            fail_when_missing=False,
            spec_file_path="SPECS/packit-hello-world.spec",
        ).and_return(
            flexmock()
        ).once()
        flexmock(PagureProject).should_receive("get_web_url").and_return(
            "https://git.stg.centos.org/source-git/packit-hello-world"
        )
        assert event_object.package_config

    def test_update_pull_request_event(self, pagure_pr_update):
        centos_event_parser = CentosEventParser()
        event_object = centos_event_parser.parse_event(pagure_pr_update)

        assert isinstance(event_object, PullRequestPagureEvent)
        assert event_object.action == PullRequestAction.synchronize
        assert event_object.pr_id == 13
        assert event_object.base_repo_namespace == "source-git"
        assert event_object.base_repo_name == "packit-hello-world"
        assert event_object.base_repo_owner == "sakalosj"
        assert event_object.base_ref == "master"
        assert event_object.target_repo == "packit-hello-world"
        assert event_object.commit_sha == "b658af51df98c1cbf74a75095ced920bba2ef25e"
        assert event_object.user_login == "sakalosj"
        assert event_object.identifier == "13"
        assert (
            event_object.project_url
            == "https://git.stg.centos.org/source-git/packit-hello-world"
        )

        assert isinstance(event_object.project, PagureProject)
        assert event_object.project.full_repo_name == "source-git/packit-hello-world"
        assert isinstance(event_object.base_project, PagureProject)
        assert (
            event_object.base_project.full_repo_name
            == "fork/sakalosj/source-git/packit-hello-world"
        )

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=13,
            reference="b658af51df98c1cbf74a75095ced920bba2ef25e",
            fail_when_missing=False,
            spec_file_path="SPECS/packit-hello-world.spec",
        ).and_return(
            flexmock()
        ).once()
        flexmock(PagureProject).should_receive("get_web_url").and_return(
            "https://git.stg.centos.org/source-git/packit-hello-world"
        )
        assert event_object.package_config

    def test_pull_request_comment_event(self, pagure_pr_comment_added):
        centos_event_parser = CentosEventParser()
        event_object = centos_event_parser.parse_event(pagure_pr_comment_added)

        assert isinstance(event_object, PullRequestCommentPagureEvent)
        assert event_object.pr_id == 16
        assert event_object.base_repo_namespace == "source-git"
        assert event_object.base_repo_name == "packit-hello-world"
        assert event_object.base_repo_owner == "sakalosj"
        assert event_object.base_ref is None
        assert event_object.target_repo == "packit-hello-world"
        assert event_object.commit_sha == "dfe787d04101728c6ddc213d3f4bf39c969f194c"
        assert event_object.user_login == "sakalosj"
        assert event_object.comment == "/packit copr-build"
        assert (
            event_object.project_url
            == "https://git.stg.centos.org/source-git/packit-hello-world"
        )

        assert isinstance(event_object.project, PagureProject)
        assert event_object.project.full_repo_name == "source-git/packit-hello-world"
        assert isinstance(event_object.base_project, PagureProject)
        assert (
            event_object.base_project.full_repo_name
            == "fork/sakalosj/source-git/packit-hello-world"
        )

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=16,
            reference="dfe787d04101728c6ddc213d3f4bf39c969f194c",
            fail_when_missing=False,
            spec_file_path="SPECS/packit-hello-world.spec",
        ).and_return(
            flexmock()
        ).once()
        flexmock(PagureProject).should_receive("get_web_url").and_return(
            "https://git.stg.centos.org/source-git/packit-hello-world"
        )
        assert event_object.package_config

    def test_parse_copr_build_event_start(
        self, copr_build_results_start, copr_build_centos_pr
    ):
        flexmock(CoprBuildModel).should_receive("get_by_build_id").and_return(
            copr_build_centos_pr
        )

        event_object = Parser.parse_event(copr_build_results_start)

        assert isinstance(event_object, AbstractCoprBuildEvent)
        assert event_object.topic == FedmsgTopic.copr_build_started
        assert event_object.build_id == 1044215
        assert event_object.chroot == "fedora-rawhide-x86_64"
        assert event_object.status == 3
        assert event_object.owner == "packit"
        assert event_object.project_name == "packit-service-hello-world-24-stg"
        assert (
            event_object.project_url
            == "https://git.stg.centos.org/source-git/packit-hello-world"
        )
        assert event_object.base_repo_name == "packit-hello-world"
        assert event_object.base_repo_namespace == "source-git"
        assert event_object.pkg == "hello"

        flexmock(PagureProject).should_receive("get_pr").with_args(
            pr_id=123
        ).and_return(flexmock(author="the-fork"))
        assert isinstance(event_object.project, PagureProject)
        assert event_object.project.full_repo_name == "source-git/packit-hello-world"
        assert isinstance(event_object.base_project, PagureProject)
        assert (
            event_object.base_project.full_repo_name
            == "fork/the-fork/source-git/packit-hello-world"
        )

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=123,
            reference="0011223344",
            fail_when_missing=False,
            spec_file_path="SPECS/packit-hello-world.spec",
        ).and_return(
            flexmock()
        ).once()
        flexmock(PagureProject).should_receive("get_web_url").and_return(
            "https://git.stg.centos.org/source-git/packit-hello-world"
        )
        assert event_object.package_config

    def test_parse_copr_build_event_end(
        self, copr_build_results_end, copr_build_centos_pr
    ):
        flexmock(CoprBuildModel).should_receive("get_by_build_id").and_return(
            copr_build_centos_pr
        )

        event_object = Parser.parse_event(copr_build_results_end)

        assert isinstance(event_object, AbstractCoprBuildEvent)
        assert event_object.topic == FedmsgTopic.copr_build_finished
        assert event_object.build_id == 1044215
        assert event_object.chroot == "fedora-rawhide-x86_64"
        assert event_object.status == 1
        assert event_object.owner == "packit"
        assert event_object.project_name == "packit-service-hello-world-24-stg"
        assert event_object.base_repo_name == "packit-hello-world"
        assert event_object.base_repo_namespace == "source-git"
        assert event_object.pkg == "hello"
        assert event_object.git_ref == "0011223344"

        flexmock(PagureProject).should_receive("get_pr").with_args(
            pr_id=123
        ).and_return(flexmock(author="the-fork"))
        assert isinstance(event_object.project, PagureProject)
        assert event_object.project.full_repo_name == "source-git/packit-hello-world"
        assert isinstance(event_object.base_project, PagureProject)
        assert (
            event_object.base_project.full_repo_name
            == "fork/the-fork/source-git/packit-hello-world"
        )

        flexmock(PackageConfigGetter).should_receive(
            "get_package_config_from_repo"
        ).with_args(
            base_project=event_object.base_project,
            project=event_object.project,
            pr_id=123,
            reference="0011223344",
            fail_when_missing=False,
            spec_file_path="SPECS/packit-hello-world.spec",
        ).and_return(
            flexmock()
        ).once()
        flexmock(PagureProject).should_receive("get_web_url").and_return(
            "https://git.stg.centos.org/source-git/packit-hello-world"
        )
        assert event_object.package_config


def test_event_data_parse_pr(github_pr_event):
    flexmock(AddPullRequestDbTrigger).should_receive("db_trigger").and_return(None)
    data = EventData.from_event_dict(github_pr_event.get_dict())
    assert data.event_type == "PullRequestGithubEvent"
    assert data.user_login == "lbarcziova"
    assert not data.git_ref
    assert data.commit_sha == "528b803be6f93e19ca4130bf4976f2800a3004c4"
    assert data.identifier == "342"
    assert data.pr_id == 342
