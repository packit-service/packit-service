# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

"""
Parser is transforming github JSONs into `events` objects
"""
import logging
from functools import partial
from typing import Optional, Type, Union, List

import xmltodict
from ogr.parsing import parse_git_repo
from packit.utils import nested_get

from packit_service.config import ServiceConfig
from packit_service.constants import (
    KojiBuildState,
    TESTING_FARM_INSTALLABILITY_TEST_URL,
)
from packit_service.models import TestingFarmResult, TFTTestRunModel
from packit_service.service.events import (
    AbstractPagureEvent,
    CoprBuildEndEvent,
    AbstractCoprBuildEvent,
    CoprBuildStartEvent,
    DistGitCommitEvent,
    GitlabEventAction,
    InstallationEvent,
    IssueCommentAction,
    IssueCommentEvent,
    IssueCommentGitlabEvent,
    KojiBuildEvent,
    MergeRequestCommentGitlabEvent,
    MergeRequestGitlabEvent,
    PullRequestAction,
    PullRequestCommentAction,
    PullRequestCommentGithubEvent,
    PullRequestCommentPagureEvent,
    PullRequestGithubEvent,
    PullRequestPagureEvent,
    PushGitHubEvent,
    PushGitlabEvent,
    PushPagureEvent,
    ReleaseEvent,
    TestingFarmResultsEvent,
    TestResult,
)
from packit_service.worker.handlers import (
    DistGitCommitHandler,
)
from packit_service.worker.testing_farm import TestingFarmJobHelper

logger = logging.getLogger(__name__)


class Parser:
    """
    Once we receive a new event (GitHub/GitLab webhook or Fedmsg/Centosmsg event) for every event
    we need to have method inside the `Parser` class to create objects defined in `events.py`.
    """

    @staticmethod
    def parse_event(
        event: dict,
    ) -> Optional[
        Union[
            PullRequestGithubEvent,
            InstallationEvent,
            ReleaseEvent,
            DistGitCommitEvent,
            TestingFarmResultsEvent,
            PullRequestCommentGithubEvent,
            IssueCommentEvent,
            AbstractCoprBuildEvent,
            PushGitHubEvent,
            MergeRequestGitlabEvent,
            KojiBuildEvent,
            MergeRequestCommentGitlabEvent,
            IssueCommentGitlabEvent,
            PushGitlabEvent,
        ]
    ]:
        """
        Try to parse all JSONs that we process
        :param event: JSON from GitHub/GitLab or fedmsg/centosmsg
        :return: event object
        """

        if not event:
            logger.warning("No event to process!")
            return None

        for response in map(
            lambda parser: parser(event),
            (
                Parser.parse_pr_event,
                Parser.parse_pull_request_comment_event,
                Parser.parse_issue_comment_event,
                Parser.parse_release_event,
                Parser.parse_push_event,
                Parser.parse_installation_event,
                Parser.parse_distgit_commit_event,
                Parser.parse_testing_farm_results_event,
                Parser.parse_copr_event,
                Parser.parse_mr_event,
                Parser.parse_koji_event,
                Parser.parse_merge_request_comment_event,
                Parser.parse_gitlab_issue_comment_event,
                Parser.parse_gitlab_push_event,
            ),
        ):
            if response:
                return response

        logger.debug("We don't process this event.")
        return None

    @staticmethod
    def parse_mr_event(event) -> Optional[MergeRequestGitlabEvent]:
        """Look into the provided event and see if it's one for a new gitlab MR."""
        if event.get("object_kind") != "merge_request":
            return None

        state = event["object_attributes"]["state"]
        if state != "opened":
            return None
        action = nested_get(event, "object_attributes", "action")
        if action not in {"reopen", "update"}:
            action = state

        username = event["user"]["username"]
        if not username:
            logger.warning("No Gitlab username from event.")
            return None

        object_id = event["object_attributes"]["id"]
        if not object_id:
            logger.warning("No object id from the event.")
            return None

        object_iid = event["object_attributes"]["iid"]
        if not object_iid:
            logger.warning("No object iid from the event.")
            return None

        source_project_url = nested_get(event, "object_attributes", "source", "web_url")
        if not source_project_url:
            logger.warning("Source project url not found in the event.")
            return None
        parsed_source_url = parse_git_repo(potential_url=source_project_url)
        source_repo_branch = nested_get(event, "object_attributes", "source_branch")
        logger.info(
            f"Source: "
            f"url={source_project_url} "
            f"namespace={parsed_source_url.namespace} "
            f"repo={parsed_source_url.repo} "
            f"branch={source_repo_branch}."
        )

        target_project_url = nested_get(event, "project", "web_url")
        if not target_project_url:
            logger.warning("Target project url not found in the event.")
            return None
        parsed_target_url = parse_git_repo(potential_url=target_project_url)
        target_repo_branch = nested_get(event, "object_attributes", "target_branch")
        logger.info(
            f"Target: "
            f"url={target_project_url} "
            f"namespace={parsed_target_url.namespace} "
            f"repo={parsed_target_url.repo} "
            f"branch={target_repo_branch}."
        )

        commit_sha = nested_get(event, "object_attributes", "last_commit", "id")

        return MergeRequestGitlabEvent(
            action=GitlabEventAction[action],
            username=username,
            object_id=object_id,
            object_iid=object_iid,
            source_repo_namespace=parsed_source_url.namespace,
            source_repo_name=parsed_source_url.repo,
            source_repo_branch=source_repo_branch,
            target_repo_namespace=parsed_target_url.namespace,
            target_repo_name=parsed_target_url.repo,
            target_repo_branch=target_repo_branch,
            project_url=target_project_url,
            commit_sha=commit_sha,
        )

    @staticmethod
    def parse_pr_event(event) -> Optional[PullRequestGithubEvent]:
        """Look into the provided event and see if it's one for a new github PR."""
        if not event.get("pull_request"):
            return None

        pr_id = event.get("number")
        action = event.get("action")
        if action not in {"opened", "reopened", "synchronize"} or not pr_id:
            return None

        logger.info(f"GitHub PR#{pr_id} {action!r} event.")

        # we can't use head repo here b/c the app is set up against the upstream repo
        # and not the fork, on the other hand, we don't process packit.yaml from
        # the PR but what's in the upstream
        base_repo_namespace = nested_get(
            event, "pull_request", "head", "repo", "owner", "login"
        )
        base_repo_name = nested_get(event, "pull_request", "head", "repo", "name")

        if not (base_repo_name and base_repo_namespace):
            logger.warning("No full name of the repository.")
            return None

        base_ref = nested_get(event, "pull_request", "head", "sha")
        if not base_ref:
            logger.warning("Ref where the PR is coming from is not set.")
            return None

        user_login = nested_get(event, "pull_request", "user", "login")
        if not user_login:
            logger.warning("No GitHub login name from event.")
            return None

        target_repo_namespace = nested_get(
            event, "pull_request", "base", "repo", "owner", "login"
        )
        target_repo_name = nested_get(event, "pull_request", "base", "repo", "name")
        logger.info(f"Target repo: {target_repo_namespace}/{target_repo_name}.")

        commit_sha = nested_get(event, "pull_request", "head", "sha")
        https_url = event["repository"]["html_url"]
        return PullRequestGithubEvent(
            action=PullRequestAction[action],
            pr_id=pr_id,
            base_repo_namespace=base_repo_namespace,
            base_repo_name=base_repo_name,
            base_ref=base_ref,
            target_repo_namespace=target_repo_namespace,
            target_repo_name=target_repo_name,
            project_url=https_url,
            commit_sha=commit_sha,
            user_login=user_login,
        )

    @staticmethod
    def parse_gitlab_push_event(event) -> Optional[PushGitlabEvent]:
        """
        Look into the provided event and see if it's one for a new push to the gitlab branch.
        https://docs.gitlab.com/ee/user/project/integrations/webhooks.html#push-events
        """

        if event.get("object_kind") != "push":
            return None

        raw_ref = event.get("ref")
        before = event.get("before")
        pusher = event.get("user_username")

        commits = event.get("commits")

        if not (raw_ref and commits and before and pusher):
            return None
        elif event.get("after").startswith("0000000"):
            logger.info(
                f"GitLab push event on '{raw_ref}' by {pusher} to delete branch"
            )
            return None

        number_of_commits = event.get("total_commits_count")

        if not number_of_commits:
            logger.warning("No number of commits info from event.")

        raw_ref = raw_ref.split("/", maxsplit=2)

        if not raw_ref:
            logger.warning("No ref info from event.")

        ref = raw_ref[-1]

        head_commit = commits[-1]["id"]

        if not raw_ref:
            logger.warning("No commit_id info from event.")

        logger.info(
            f"Gitlab push event on '{raw_ref}': {before[:8]} -> {head_commit[:8]} "
            f"by {pusher} "
            f"({number_of_commits} {'commit' if number_of_commits == 1 else 'commits'})"
        )

        project_url = nested_get(event, "project", "web_url")
        if not project_url:
            logger.warning("Target project url not found in the event.")
            return None
        parsed_url = parse_git_repo(potential_url=project_url)
        logger.info(
            f"Project: "
            f"repo={parsed_url.repo} "
            f"namespace={parsed_url.namespace} "
            f"url={project_url}."
        )

        return PushGitlabEvent(
            repo_namespace=parsed_url.namespace,
            repo_name=parsed_url.repo,
            git_ref=ref,
            project_url=project_url,
            commit_sha=head_commit,
        )

    @staticmethod
    def parse_push_event(event) -> Optional[PushGitHubEvent]:
        """
        Look into the provided event and see if it's one for a new push to the github branch.
        """
        raw_ref = event.get("ref")
        before = event.get("before")
        pusher = nested_get(event, "pusher", "name")

        # https://developer.github.com/v3/activity/events/types/#pushevent
        # > Note: The webhook payload example following the table differs
        # > significantly from the Events API payload described in the table.
        head_commit = (
            event.get("head") or event.get("after") or event.get("head_commit")
        )

        if not (raw_ref and head_commit and before and pusher):
            return None
        elif event.get("deleted"):
            logger.info(
                f"GitHub push event on '{raw_ref}' by {pusher} to delete branch"
            )
            return None

        number_of_commits = event.get("size")
        if number_of_commits is None and "commits" in event:
            number_of_commits = len(event.get("commits"))

        ref = raw_ref.split("/", maxsplit=2)[-1]

        logger.info(
            f"GitHub push event on '{raw_ref}': {before[:8]} -> {head_commit[:8]} "
            f"by {pusher} "
            f"({number_of_commits} {'commit' if number_of_commits == 1 else 'commits'})"
        )

        repo_namespace = nested_get(event, "repository", "owner", "login")
        repo_name = nested_get(event, "repository", "name")

        if not (repo_namespace and repo_name):
            logger.warning("No full name of the repository.")
            return None

        repo_url = nested_get(event, "repository", "html_url")

        return PushGitHubEvent(
            repo_namespace=repo_namespace,
            repo_name=repo_name,
            git_ref=ref,
            project_url=repo_url,
            commit_sha=head_commit,
        )

    @staticmethod
    def parse_issue_comment_event(event) -> Optional[IssueCommentEvent]:
        """Look into the provided event and see if it is Github issue comment event."""
        if nested_get(event, "issue", "pull_request"):
            return None

        issue_id = nested_get(event, "issue", "number")
        action = event.get("action")
        comment = nested_get(event, "comment", "body")
        if action != "created" or not issue_id or not comment:
            return None

        logger.info(f"Github issue#{issue_id} comment: {comment!r} {action!r} event.")

        base_repo_namespace = nested_get(event, "repository", "owner", "login")
        base_repo_name = nested_get(event, "repository", "name")
        if not (base_repo_namespace and base_repo_name):
            logger.warning("No full name of the repository.")

        user_login = nested_get(event, "comment", "user", "login")
        if not user_login:
            logger.warning("No Github login name from event.")
            return None

        target_repo = nested_get(event, "repository", "full_name")
        logger.info(f"Target repo: {target_repo}.")
        https_url = nested_get(event, "repository", "html_url")
        return IssueCommentEvent(
            IssueCommentAction[action],
            issue_id,
            base_repo_namespace,
            base_repo_name,
            target_repo,
            https_url,
            user_login,
            comment,
        )

    @staticmethod
    def parse_gitlab_issue_comment_event(event) -> Optional[IssueCommentGitlabEvent]:
        """Look into the provided event and see if it is Gitlab Issue comment event."""
        if event.get("object_kind") != "note":
            return None

        issue = event.get("issue")
        if not issue:
            return None

        issue_id = nested_get(event, "issue", "iid")
        if not issue_id:
            logger.warning("No issue id from the event.")
            return None
        comment = nested_get(event, "object_attributes", "note")
        if not comment:
            logger.warning("No note from the event.")
            return None

        state = nested_get(event, "issue", "state")
        if not state:
            logger.warning("No state from the event.")
            return None
        if state != "opened":
            return None
        action = nested_get(event, "object_attributes", "action")
        if action not in {"reopen", "update"}:
            action = state

        logger.info(
            f"Gitlab issue ID: {issue_id} comment: {comment!r} {action!r} event."
        )

        project_url = nested_get(event, "project", "web_url")
        if not project_url:
            logger.warning("Target project url not found in the event.")
            return None
        parsed_url = parse_git_repo(potential_url=project_url)
        logger.info(
            f"Project: "
            f"repo={parsed_url.repo} "
            f"namespace={parsed_url.namespace} "
            f"url={project_url}."
        )

        username = nested_get(event, "user", "username")
        if not username:
            logger.warning("No Gitlab username from event.")
            return None

        return IssueCommentGitlabEvent(
            action=GitlabEventAction[action],
            issue_id=issue_id,
            repo_namespace=parsed_url.namespace,
            repo_name=parsed_url.repo,
            project_url=project_url,
            username=username,
            comment=comment,
        )

    @staticmethod
    def parse_merge_request_comment_event(
        event,
    ) -> Optional[MergeRequestCommentGitlabEvent]:
        """Look into the provided event and see if it is Gitlab MR comment event."""
        if event.get("object_kind") != "note":
            return None

        merge_request = event.get("merge_request")
        if not merge_request:
            return None

        state = nested_get(event, "merge_request", "state")
        if state != "opened":
            return None

        action = nested_get(event, "merge_request", "action")
        if action not in {"reopen", "update"}:
            action = state

        object_iid = nested_get(event, "merge_request", "iid")
        if not object_iid:
            logger.warning("No object iid from the event.")

        object_id = nested_get(event, "merge_request", "id")
        if not object_id:
            logger.warning("No object id from the event.")

        comment = nested_get(event, "object_attributes", "note")
        logger.info(
            f"Gitlab MR id#{object_id} iid#{object_iid} comment: {comment!r} {action!r} event."
        )

        source_project_url = nested_get(event, "merge_request", "source", "web_url")
        if not source_project_url:
            logger.warning("Source project url not found in the event.")
            return None
        parsed_source_url = parse_git_repo(potential_url=source_project_url)
        logger.info(
            f"Source: "
            f"repo={parsed_source_url.repo} "
            f"namespace={parsed_source_url.namespace} "
            f"url={source_project_url}."
        )

        target_project_url = nested_get(event, "project", "web_url")
        if not target_project_url:
            logger.warning("Target project url not found in the event.")
            return None
        parsed_target_url = parse_git_repo(potential_url=target_project_url)
        logger.info(
            f"Target: "
            f"repo={parsed_target_url.repo} "
            f"namespace={parsed_target_url.namespace} "
            f"url={target_project_url}."
        )

        username = nested_get(event, "user", "username")
        if not username:
            logger.warning("No Gitlab username from event.")
            return None

        commit_sha = nested_get(event, "merge_request", "last_commit", "id")
        if not commit_sha:
            logger.warning("No commit_sha from the event.")
            return None

        return MergeRequestCommentGitlabEvent(
            action=GitlabEventAction[action],
            object_id=object_id,
            object_iid=object_iid,
            source_repo_namespace=parsed_source_url.namespace,
            source_repo_name=parsed_source_url.repo,
            target_repo_namespace=parsed_target_url.namespace,
            target_repo_name=parsed_target_url.repo,
            project_url=target_project_url,
            username=username,
            comment=comment,
            commit_sha=commit_sha,
        )

    @staticmethod
    def parse_pull_request_comment_event(
        event,
    ) -> Optional[PullRequestCommentGithubEvent]:
        """Look into the provided event and see if it is Github PR comment event."""
        if not nested_get(event, "issue", "pull_request"):
            return None

        pr_id = nested_get(event, "issue", "number")
        action = event.get("action")
        if action not in {"created", "edited"} or not pr_id:
            return None

        comment = nested_get(event, "comment", "body")
        logger.info(f"Github PR#{pr_id} comment: {comment!r} {action!r} event.")

        base_repo_namespace = nested_get(event, "issue", "user", "login")
        base_repo_name = nested_get(event, "repository", "name")
        if not (base_repo_name and base_repo_namespace):
            logger.warning("No full name of the repository.")
            return None

        user_login = nested_get(event, "comment", "user", "login")
        if not user_login:
            logger.warning("No GitHub login name from event.")
            return None
        if user_login in {"packit-as-a-service[bot]", "packit-as-a-service-stg[bot]"}:
            logger.debug("Our own comment.")
            return None

        target_repo_namespace = nested_get(event, "repository", "owner", "login")
        target_repo_name = nested_get(event, "repository", "name")

        logger.info(f"Target repo: {target_repo_namespace}/{target_repo_name}.")
        https_url = event["repository"]["html_url"]
        return PullRequestCommentGithubEvent(
            action=PullRequestCommentAction[action],
            pr_id=pr_id,
            base_repo_namespace=base_repo_namespace,
            base_repo_name=None,
            base_ref=None,  # the payload does not include this info
            target_repo_namespace=target_repo_namespace,
            target_repo_name=target_repo_name,
            project_url=https_url,
            user_login=user_login,
            comment=comment,
        )

    @staticmethod
    def parse_installation_event(event) -> Optional[InstallationEvent]:
        """Look into the provided event and see Github App installation details."""
        # Check if installation key in JSON isn't enough, we have to check the account as well
        if not nested_get(event, "installation", "account"):
            return None

        action = event["action"]
        if action not in {"created", "added"}:
            # We're currently not interested in removed/deleted/updated event.
            return None
        installation_id = event["installation"]["id"]
        # if action == 'created' then repos are in repositories
        # if action == 'added' then repos are in repositories_added
        repositories = event.get("repositories") or event.get("repositories_added", [])
        repo_names = [repo["full_name"] for repo in repositories]

        logger.info(f"Github App installation {action!r} event. id: {installation_id}")
        logger.debug(
            f"account: {event['installation']['account']}, "
            f"repositories: {repo_names}, sender: {event['sender']}"
        )

        # namespace (user/organization) into which the app has been installed
        account_login = event["installation"]["account"]["login"]
        account_id = event["installation"]["account"]["id"]
        account_url = event["installation"]["account"]["url"]
        account_type = event["installation"]["account"]["type"]  # User or Organization
        created_at = event["installation"]["created_at"]

        # user who installed the app into 'account'
        sender_id = event["sender"]["id"]
        sender_login = event["sender"]["login"]

        return InstallationEvent(
            installation_id,
            account_login,
            account_id,
            account_url,
            account_type,
            created_at,
            repo_names,
            sender_id,
            sender_login,
        )

    @staticmethod
    def parse_release_event(event) -> Optional[ReleaseEvent]:
        """
        https://developer.github.com/v3/activity/events/types/#releaseevent
        https://developer.github.com/v3/repos/releases/#get-a-single-release

        look into the provided event and see if it's one for a published github release;
        if it is, process it and return input for the job handler
        """
        action = event.get("action")
        release = event.get("release")
        if action != "published" or not release:
            return None

        logger.info(f"GitHub release {release} {action!r} event.")

        repo_namespace = nested_get(event, "repository", "owner", "login")
        repo_name = nested_get(event, "repository", "name")
        if not (repo_namespace and repo_name):
            logger.warning("No full name of the repository.")
            return None

        release_ref = nested_get(event, "release", "tag_name")
        if not release_ref:
            logger.warning("Release tag name is not set.")
            return None

        logger.info(
            f"New release event {release_ref!r} for repo {repo_namespace}/{repo_name}."
        )
        https_url = event["repository"]["html_url"]
        return ReleaseEvent(repo_namespace, repo_name, release_ref, https_url)

    @staticmethod
    def parse_distgit_commit_event(event) -> Optional[DistGitCommitEvent]:
        """this corresponds to dist-git event when someone pushes new commits"""
        topic = event.get("topic")
        if topic != DistGitCommitHandler.topic:
            return None

        logger.info(f"Dist-git commit event, topic: {topic}")

        dg_repo_namespace = nested_get(event, "commit", "namespace")
        dg_repo_name = nested_get(event, "commit", "repo")

        if not (dg_repo_namespace and dg_repo_name):
            logger.warning("No full name of the repository.")
            return None

        dg_branch = nested_get(event, "commit", "branch")
        dg_rev = nested_get(event, "commit", "rev")
        if not (dg_branch and dg_rev):
            logger.warning("Target branch/rev for the new commits is not set.")
            return None

        logger.info(
            f"New commits added to dist-git repo {dg_repo_namespace}/{dg_repo_name},"
            f"rev: {dg_rev}, branch: {dg_branch}"
        )

        project_to_sync = ServiceConfig.get_service_config().get_project_to_sync(
            dg_repo_name, dg_branch
        )
        if not project_to_sync:
            logger.info("No matching upstream repo for syncing found.")
            return None

        upstream_project_url = (
            f"{project_to_sync.forge}/{project_to_sync.repo_namespace}/"
            f"{project_to_sync.repo_name}"
        )

        # TODO: get the right hostname without hardcoding
        dg_project_url = (
            f"https://src.fedoraproject.org/{dg_repo_namespace}/{dg_repo_name}"
        )

        return DistGitCommitEvent(
            topic=topic,
            repo_namespace=project_to_sync.repo_namespace,
            repo_name=project_to_sync.repo_name,
            branch=project_to_sync.branch,
            project_url=upstream_project_url,
            dg_repo_namespace=dg_repo_namespace,
            dg_repo_name=dg_repo_name,
            dg_branch=dg_branch,
            dg_rev=dg_rev,
            dg_project_url=dg_project_url,
        )

    @staticmethod
    def _parse_tf_result_xunit(xunit: Optional[str]) -> List[TestResult]:
        """Parse event["result"]["xunit"] to get tests results"""
        if not xunit:
            return []
        xunit_dict = xmltodict.parse(xunit)
        testcases = nested_get(
            xunit_dict, "testsuites", "testsuite", "testcase", default=[]
        )
        try:
            return [
                TestResult(
                    name=testcase["@name"],
                    result=TestingFarmResult(testcase["@result"]),
                    log_url=nested_get(testcase, "logs", "log", 1, "@href", default=""),
                )
                for testcase in testcases
            ]
        except TypeError:
            # packit-service/issues/967
            logger.warning(f"Wrongly parsed TF result xunit: {xunit!r} -> {xunit_dict}")
            # We don't need it in most cases, so let's just continue instead of raising
            return []

    @staticmethod
    def parse_testing_farm_results_event(
        event: dict,
    ) -> Optional[TestingFarmResultsEvent]:
        """this corresponds to testing farm results event"""
        if event.get("source") != "testing-farm" or not event.get("request_id"):
            return None

        request_id: str = event["request_id"]
        logger.info(f"Testing farm notification event. Request ID: {request_id}")

        tft_test_run = TFTTestRunModel.get_by_pipeline_id(request_id)

        # Testing Farm sends only request/pipeline id in a notification.
        # We need to get more details ourselves.
        # It'd be much better to do this in TestingFarmResultsHandler.run(),
        # but all the code along the way to get there expects we already know the details.
        # TODO: Get missing info from db instead of querying TF
        event = TestingFarmJobHelper.get_request_details(request_id)
        if not event:
            # Something's wrong with TF, raise exception so that we can re-try later.
            raise Exception(f"Failed to get {request_id} details from TF.")

        result: TestingFarmResult = TestingFarmResult(
            nested_get(event, "result", "overall") or event.get("state") or "unknown"
        )
        summary: str = nested_get(event, "result", "summary") or ""
        env: dict = nested_get(event, "environments_requested", 0, default={})
        compose: str = nested_get(env, "os", "compose")
        tests: List[TestResult] = Parser._parse_tf_result_xunit(
            nested_get(event, "result", "xunit")
        )

        ref: str = nested_get(event, "test", "fmf", "ref")
        project_url: str = nested_get(event, "test", "fmf", "url")

        # ["test"]["fmf"]["ref"] contains ref to the TF test, i.e. "master",
        # but we need the original commit_sha to be able to continue
        if tft_test_run:
            ref = tft_test_run.commit_sha

        if project_url == TESTING_FARM_INSTALLABILITY_TEST_URL:
            # There are no artifacts in install-test results
            copr_build_id = copr_chroot = ""
        else:
            artifact: dict = nested_get(env, "artifacts", 0, default={})
            a_type: str = artifact.get("type")
            if a_type == "fedora-copr-build":
                copr_build_id = artifact["id"].split(":")[0]
                copr_chroot = artifact["id"].split(":")[1]
            else:
                logger.error(f"{a_type} != fedora-copr-build")
                copr_build_id = copr_chroot = ""

        if not copr_chroot and tft_test_run:
            copr_chroot = tft_test_run.target

        # ["test"]["fmf"]["url"] contains PR's source/fork url or TF's install test url.
        # We need the original/base project url stored in db.
        if tft_test_run and tft_test_run.data:
            base_project_url = tft_test_run.data.get("base_project_url")
            if base_project_url and base_project_url != project_url:
                logger.debug(
                    f"Using project url {base_project_url} instead of {project_url}"
                )
                project_url = base_project_url

        # Temporary until we have a better logs page.
        log_url: str = f"http://artifacts.dev.testing-farm.io/{request_id}"

        logger.debug(
            f"project_url: {project_url}, ref: {ref}, result: {result}, "
            f"summary: {summary!r}, copr-build: {copr_build_id}:{copr_chroot}"
        )

        return TestingFarmResultsEvent(
            pipeline_id=request_id,
            result=result,
            compose=compose,
            summary=summary,
            log_url=log_url,
            copr_build_id=copr_build_id,
            copr_chroot=copr_chroot,
            tests=tests,
            commit_sha=ref,
            project_url=project_url,
        )

    @staticmethod
    def parse_copr_event(event) -> Optional[AbstractCoprBuildEvent]:
        """this corresponds to copr build event e.g:"""
        topic = event.get("topic")

        copr_build_cls: Type["AbstractCoprBuildEvent"]
        if topic == "org.fedoraproject.prod.copr.build.start":
            copr_build_cls = CoprBuildStartEvent
        elif topic == "org.fedoraproject.prod.copr.build.end":
            copr_build_cls = CoprBuildEndEvent
        else:
            # Topic not supported.
            return None

        logger.info(f"Copr event; {event.get('what')}")

        build_id = event.get("build")
        chroot = event.get("chroot")
        status = event.get("status")
        owner = event.get("owner")
        project_name = event.get("copr")
        pkg = event.get("pkg")
        timestamp = event.get("timestamp")

        return copr_build_cls.from_build_id(
            topic, build_id, chroot, status, owner, project_name, pkg, timestamp
        )

    @staticmethod
    def parse_koji_event(event) -> Optional[KojiBuildEvent]:
        if event.get("topic") != "org.fedoraproject.prod.buildsys.task.state.change":
            return None

        build_id = event.get("id")
        logger.info(f"Koji event: build_id={build_id}")

        state = nested_get(event, "info", "state")

        if not state:
            logger.debug("Cannot find build state.")
            return None

        state_enum = KojiBuildState(event.get("new")) if "new" in event else None
        old_state = KojiBuildState(event.get("old")) if "old" in event else None

        start_time = nested_get(event, "info", "start_time")
        completion_time = nested_get(event, "info", "completion_time")

        rpm_build_task_id = None
        for children in nested_get(event, "info", "children", default=[]):
            if children.get("method") == "buildArch":
                rpm_build_task_id = children.get("id")
                break

        return KojiBuildEvent(
            build_id=build_id,
            state=state_enum,
            old_state=old_state,
            start_time=start_time,
            completion_time=completion_time,
            rpm_build_task_id=rpm_build_task_id,
        )


class CentosEventParser:
    """
    Class responsible for parsing events received from CentOS infrastructure
    """

    def __init__(self):
        """
        self.event_mapping: dictionary mapping of topics to corresponding parsing methods

        ..note: action in partial is github counterpart value, as this value is used in code

            e.g.
            pagure pull-request.update == github pull-request.synchronize -> in code is used
            synchronize
        """
        self.event_mapping = {
            "pull-request.new": partial(self._pull_request_event, action="opened"),
            "pull-request.reopened": partial(
                self._pull_request_event, action="reopened"
            ),
            "pull-request.updated": partial(
                self._pull_request_event, action="synchronize"
            ),
            "pull-request.comment.added": partial(
                self._pull_request_comment, action="added"
            ),
            "pull-request.comment.edited": partial(
                self._pull_request_comment, action="edited"
            ),
            "git.receive": self._push_event,
        }

    def parse_event(self, event: dict) -> Optional[AbstractPagureEvent]:
        """
        Entry point for parsing event
        :param event: contains event data
        :return: event object or None
        """
        logger.debug(f"Parsing {event.get('topic')}")

        # e.g. "topic": "git.stg.centos.org/pull-request.tag.added"
        source, git_topic = event.get("topic").split("/")
        event["source"] = source
        event["git_topic"] = git_topic

        if git_topic not in self.event_mapping:
            logger.info(f"Event type {git_topic!r} is not processed.")
            return None

        return self.event_mapping[git_topic](event)

    @staticmethod
    def _pull_request_event(event: dict, action: str) -> PullRequestPagureEvent:
        pullrequest = event["pullrequest"]
        pr_id = pullrequest["id"]
        base_repo_namespace = pullrequest["repo_from"]["namespace"]
        base_repo_name = pullrequest["repo_from"]["name"]
        base_repo_owner = pullrequest["repo_from"]["user"]["name"]
        base_ref = pullrequest["branch"]
        target_repo = pullrequest["project"]["name"]
        https_url = f"https://{event['source']}/{pullrequest['project']['url_path']}"
        commit_sha = pullrequest["commit_stop"]
        pagure_login = pullrequest["user"]["name"]

        return PullRequestPagureEvent(
            action=PullRequestAction[action],
            pr_id=pr_id,
            base_repo_namespace=base_repo_namespace,
            base_repo_name=base_repo_name,
            base_repo_owner=base_repo_owner,
            base_ref=base_ref,
            target_repo=target_repo,
            project_url=https_url,
            commit_sha=commit_sha,
            user_login=pagure_login,
        )

    @staticmethod
    def _pull_request_comment(
        event: dict, action: str
    ) -> PullRequestCommentPagureEvent:
        event[
            "https_url"
        ] = f"https://{event['source']}/{event['pullrequest']['project']['url_path']}"
        action = PullRequestCommentAction.created.value
        pr_id = event["pullrequest"]["id"]
        base_repo_namespace = event["pullrequest"]["project"]["namespace"]
        base_repo_name = event["pullrequest"]["project"]["name"]
        base_repo_owner = event["pullrequest"]["repo_from"]["user"]["name"]
        target_repo = event["pullrequest"]["repo_from"]["name"]
        https_url = (
            f"https://{event['source']}/{event['pullrequest']['project']['url_path']}"
        )
        pagure_login = event["agent"]
        commit_sha = event["pullrequest"]["commit_stop"]

        # gets comment from event.
        # location differs based on topic (pull-request.comment.edited/pull-request.comment.added)
        if "edited" in event["git_topic"]:
            comment = event["comment"]["comment"]
        elif "added" in event["git_topic"]:
            comment = event["pullrequest"]["comments"][-1]["comment"]
        else:
            raise ValueError(
                f"Unknown comment location in response for {event['git_topic']}"
            )

        return PullRequestCommentPagureEvent(
            action=PullRequestCommentAction[action],
            pr_id=pr_id,
            base_repo_namespace=base_repo_namespace,
            base_repo_name=base_repo_name,
            base_repo_owner=base_repo_owner,
            base_ref=None,
            target_repo=target_repo,
            project_url=https_url,
            commit_sha=commit_sha,
            user_login=pagure_login,
            comment=comment,
        )

    @staticmethod
    def _push_event(event: dict) -> PushPagureEvent:
        return PushPagureEvent(
            repo_namespace=event["repo"]["namespace"],
            repo_name=event["repo"]["name"],
            git_ref=f"refs/head/{event['branch']}",
            project_url=f"https://{event['source']}/{event['repo']['url_path']}",
            commit_sha=event["end_commit"],
        )
