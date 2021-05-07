# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

from flexmock import flexmock
from ogr.services.github import GithubProject

from packit_service.constants import KojiBuildState
from packit_service.models import (
    ProjectReleaseModel,
    GitProjectModel,
    GitBranchModel,
    PullRequestModel,
)
from packit_service.service.events import (
    ReleaseEvent,
    PushGitHubEvent,
    PullRequestGithubEvent,
    PullRequestCommentGithubEvent,
    TestingFarmResultsEvent,
    MergeRequestGitlabEvent,
    KojiBuildEvent,
    MergeRequestCommentGitlabEvent,
    PushGitlabEvent,
)
from packit_service.worker.parser import Parser
from packit_service.worker.testing_farm import TestingFarmJobHelper
from tests_requre.conftest import SampleValues


def test_release_event_existing_release(
    clean_before_and_after, release_model, release_event_dict
):
    flexmock(GithubProject).should_receive("get_sha_from_tag").and_return(
        SampleValues.commit_sha
    )

    event_object = Parser.parse_event(release_event_dict)
    assert isinstance(event_object, ReleaseEvent)

    assert event_object.identifier == "v1.0.2"
    assert event_object.git_ref == "v1.0.2"
    assert event_object.commit_sha == "80201a74d96c"
    assert event_object.tag_name == "v1.0.2"

    assert isinstance(event_object.db_trigger, ProjectReleaseModel)
    assert event_object.db_trigger == release_model
    assert event_object.db_trigger.tag_name == "v1.0.2"
    assert event_object.db_trigger.commit_hash == "80201a74d96c"

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "the-repo-name"


def test_release_event_non_existing_release(clean_before_and_after, release_event_dict):
    flexmock(GithubProject).should_receive("get_sha_from_tag").and_return(
        SampleValues.commit_sha
    )

    event_object = Parser.parse_event(release_event_dict)
    assert isinstance(event_object, ReleaseEvent)

    assert event_object.identifier == "v1.0.2"
    assert event_object.git_ref == "v1.0.2"
    assert event_object.commit_sha == "80201a74d96c"
    assert event_object.tag_name == "v1.0.2"

    assert isinstance(event_object.db_trigger, ProjectReleaseModel)
    assert event_object.db_trigger.tag_name == "v1.0.2"
    assert event_object.db_trigger.commit_hash == "80201a74d96c"

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "the-repo-name"


def test_push_branch_event_existing_branch(
    clean_before_and_after, branch_model, push_branch_event_dict
):
    event_object = Parser.parse_event(push_branch_event_dict)
    assert isinstance(event_object, PushGitHubEvent)

    assert event_object.identifier == "build-branch"
    assert event_object.git_ref == "build-branch"
    assert event_object.commit_sha == "04885ff850b0fa0e206cd09db73565703d48f99b"

    assert isinstance(event_object.db_trigger, GitBranchModel)
    assert event_object.db_trigger == branch_model
    assert event_object.db_trigger.name == "build-branch"

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "the-repo-name"


def test_push_branch_event_non_existing_branch(
    clean_before_and_after, push_branch_event_dict
):
    event_object = Parser.parse_event(push_branch_event_dict)
    assert isinstance(event_object, PushGitHubEvent)

    assert event_object.identifier == "build-branch"
    assert event_object.git_ref == "build-branch"
    assert event_object.commit_sha == "04885ff850b0fa0e206cd09db73565703d48f99b"

    assert isinstance(event_object.db_trigger, GitBranchModel)
    assert event_object.db_trigger.name == "build-branch"

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "the-repo-name"


def test_pr_event_existing_pr(clean_before_and_after, pr_model, pr_event_dict):
    event_object = Parser.parse_event(pr_event_dict)
    assert isinstance(event_object, PullRequestGithubEvent)

    assert event_object.identifier == "342"
    assert event_object.git_ref is None
    assert event_object.commit_sha == "528b803be6f93e19ca4130bf4976f2800a3004c4"
    assert event_object.pr_id == 342

    assert isinstance(event_object.db_trigger, PullRequestModel)
    assert event_object.db_trigger == pr_model
    assert event_object.db_trigger.pr_id == 342

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "the-repo-name"


def test_mr_event_existing_mr(clean_before_and_after, mr_model, mr_event_dict):
    event_object = Parser.parse_event(mr_event_dict)
    assert isinstance(event_object, MergeRequestGitlabEvent)

    assert event_object.git_ref is None
    assert event_object.commit_sha == "45e272a57335e4e308f3176df6e9226a9e7805a9"
    assert event_object.pr_id == 2

    assert isinstance(event_object.db_trigger, PullRequestModel)
    assert event_object.db_trigger == mr_model
    assert event_object.db_trigger.pr_id == 2

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "repo-name"


def test_merge_request_comment_event(clean_before_and_after, mr_comment_event_dict):
    event_object = Parser.parse_event(mr_comment_event_dict)
    assert isinstance(event_object, MergeRequestCommentGitlabEvent)

    assert event_object.pr_id == 2
    assert event_object.identifier == "2"
    assert event_object.git_ref is None

    assert event_object.commit_sha == "45e272a57335e4e308f3176df6e9226a9e7805a9"

    assert isinstance(event_object.db_trigger, PullRequestModel)
    assert event_object.db_trigger.pr_id == 2

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "testing-packit"
    assert event_object.db_trigger.project.repo_name == "hello-there"


def test_push_gitlab_event(
    clean_before_and_after, branch_model_gitlab, push_gitlab_event_dict
):
    event_object = Parser.parse_event(push_gitlab_event_dict)
    assert isinstance(event_object, PushGitlabEvent)

    assert event_object.identifier == "build-branch"
    assert event_object.git_ref == "build-branch"
    assert event_object.commit_sha == "cb2859505e101785097e082529dced35bbee0c8f"

    assert isinstance(event_object.db_trigger, GitBranchModel)
    assert event_object.db_trigger == branch_model_gitlab
    assert event_object.db_trigger.name == "build-branch"

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "repo-name"


def test_pr_event_non_existing_pr(clean_before_and_after, pr_event_dict):
    event_object = Parser.parse_event(pr_event_dict)
    assert isinstance(event_object, PullRequestGithubEvent)

    assert event_object.identifier == "342"
    assert event_object.git_ref is None
    assert event_object.commit_sha == "528b803be6f93e19ca4130bf4976f2800a3004c4"
    assert event_object.pr_id == 342

    assert isinstance(event_object.db_trigger, PullRequestModel)
    assert event_object.db_trigger.pr_id == 342

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "the-repo-name"


def test_pr_comment_event_existing_pr(
    clean_before_and_after, pr_model, pr_comment_event_dict_packit_build
):
    event_object = Parser.parse_event(pr_comment_event_dict_packit_build)
    assert isinstance(event_object, PullRequestCommentGithubEvent)

    assert event_object.identifier == "342"
    assert event_object.git_ref is None
    assert event_object.pr_id == 342
    assert event_object.project_url == "https://github.com/the-namespace/the-repo-name"

    flexmock(GithubProject).should_receive("get_pr").with_args(pr_id=342).and_return(
        flexmock(head_commit="12345")
    )
    assert event_object.commit_sha == "12345"

    assert isinstance(event_object.db_trigger, PullRequestModel)
    assert event_object.db_trigger == pr_model
    assert event_object.db_trigger.pr_id == 342

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "the-repo-name"


def test_pr_comment_event_non_existing_pr(
    clean_before_and_after, pr_comment_event_dict_packit_build
):
    event_object = Parser.parse_event(pr_comment_event_dict_packit_build)
    assert isinstance(event_object, PullRequestCommentGithubEvent)

    assert event_object.identifier == "342"
    assert event_object.git_ref is None
    assert event_object.pr_id == 342

    flexmock(GithubProject).should_receive("get_pr").with_args(pr_id=342).and_return(
        flexmock(head_commit="12345")
    )
    assert event_object.commit_sha == "12345"

    assert isinstance(event_object.db_trigger, PullRequestModel)
    assert event_object.db_trigger.pr_id == 342

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "the-repo-name"


def test_testing_farm_response_existing_pr(
    clean_before_and_after, pr_model, a_new_test_run_pr, tf_notification, tf_result
):
    flexmock(TestingFarmJobHelper).should_receive("get_request_details").with_args(
        SampleValues.pipeline_id
    ).and_return(tf_result)
    event_object = Parser.parse_event(tf_notification)
    assert isinstance(event_object, TestingFarmResultsEvent)

    assert event_object.commit_sha == SampleValues.commit_sha

    assert isinstance(event_object.db_trigger, PullRequestModel)
    assert event_object.db_trigger == pr_model
    assert event_object.db_trigger.pr_id == 342

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "the-repo-name"


def test_testing_farm_response_non_existing_pr(
    clean_before_and_after, tf_notification, tf_result
):
    flexmock(TestingFarmJobHelper).should_receive("get_request_details").with_args(
        SampleValues.pipeline_id
    ).and_return(tf_result)
    event_object = Parser.parse_event(tf_notification)
    assert isinstance(event_object, TestingFarmResultsEvent)

    assert event_object.commit_sha == SampleValues.different_commit_sha

    assert not event_object.db_trigger


def test_testing_farm_response_existing_branch_push(
    clean_before_and_after,
    branch_model,
    a_new_test_run_branch_push,
    tf_notification,
    tf_result,
):
    flexmock(TestingFarmJobHelper).should_receive("get_request_details").with_args(
        SampleValues.pipeline_id
    ).and_return(tf_result)
    event_object = Parser.parse_event(tf_notification)
    assert isinstance(event_object, TestingFarmResultsEvent)

    assert event_object.commit_sha == SampleValues.commit_sha

    assert isinstance(event_object.db_trigger, GitBranchModel)
    assert event_object.db_trigger == branch_model
    assert event_object.db_trigger.name == "build-branch"

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "the-repo-name"


def test_testing_farm_response_non_existing_branch_push(
    clean_before_and_after, tf_notification, tf_result
):
    flexmock(TestingFarmJobHelper).should_receive("get_request_details").with_args(
        SampleValues.pipeline_id
    ).and_return(tf_result)
    event_object = Parser.parse_event(tf_notification)

    assert isinstance(event_object, TestingFarmResultsEvent)

    # For backwards compatibility, unknown results are treated as pull-requests
    assert event_object.commit_sha == SampleValues.different_commit_sha

    assert not event_object.db_trigger


def test_koji_build_scratch_start(
    clean_before_and_after, pr_model, a_koji_build_for_pr, koji_build_scratch_start_dict
):
    event_object = Parser.parse_event(koji_build_scratch_start_dict)
    assert isinstance(event_object, KojiBuildEvent)

    assert event_object.build_id == SampleValues.build_id
    assert event_object.state == KojiBuildState.open

    assert isinstance(event_object.db_trigger, PullRequestModel)
    assert event_object.db_trigger == pr_model
    assert event_object.db_trigger.pr_id == 342

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "the-repo-name"


def test_koji_build_scratch_end(
    clean_before_and_after, pr_model, a_koji_build_for_pr, koji_build_scratch_end_dict
):
    event_object = Parser.parse_event(koji_build_scratch_end_dict)
    assert isinstance(event_object, KojiBuildEvent)

    assert event_object.build_id == SampleValues.build_id
    assert event_object.state == KojiBuildState.closed

    assert isinstance(event_object.db_trigger, PullRequestModel)
    assert event_object.db_trigger == pr_model
    assert event_object.db_trigger.pr_id == 342

    assert isinstance(event_object.db_trigger.project, GitProjectModel)
    assert event_object.db_trigger.project.namespace == "the-namespace"
    assert event_object.db_trigger.project.repo_name == "the-repo-name"
