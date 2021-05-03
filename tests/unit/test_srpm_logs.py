# MIT License
#
# Copyright (c) 2018-2019 Red Hat, Inc.

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


from typing import Union
import logging
import re

from flexmock import flexmock

from ogr.abstract import GitProject
from packit.api import PackitAPI
from packit.config import (
    PackageConfig,
    JobConfig,
    JobType,
    JobConfigTriggerType,
)
from packit.config.job_config import JobMetadataConfig
from packit_service.config import ServiceConfig
from packit_service.models import SRPMBuildModel
from packit_service.service.db_triggers import AddPullRequestDbTrigger
from packit_service.service.events import (
    PullRequestGithubEvent,
    PullRequestCommentGithubEvent,
    PushGitHubEvent,
    ReleaseEvent,
)
from packit_service.worker.build.koji_build import KojiBuildJobHelper

logger = logging.getLogger(__name__)


def build_helper(
    event: Union[
        PullRequestGithubEvent,
        PullRequestCommentGithubEvent,
        PushGitHubEvent,
        ReleaseEvent,
    ],
    metadata=None,
    trigger=None,
    jobs=None,
    db_trigger=None,
):
    if not metadata:
        metadata = JobMetadataConfig(
            owner="nobody",
        )
    jobs = jobs or []
    jobs.append(
        JobConfig(
            type=JobType.production_build,
            trigger=trigger or JobConfigTriggerType.pull_request,
            metadata=metadata,
        )
    )

    pkg_conf = PackageConfig(jobs=jobs, downstream_package_name="dummy")
    handler = KojiBuildJobHelper(
        service_config=ServiceConfig(),
        package_config=pkg_conf,
        job_config=pkg_conf.jobs[0],
        project=GitProject(repo=flexmock(), service=flexmock(), namespace=flexmock()),
        metadata=flexmock(
            pr_id=event.pr_id,
            git_ref=event.git_ref,
            commit_sha=event.commit_sha,
            identifier=event.identifier,
        ),
        db_trigger=db_trigger,
    )
    handler._api = PackitAPI(config=ServiceConfig(), package_config=pkg_conf)
    return handler


def test_build_srpm_log_format(github_pr_event):
    def mock_packit_log(*args, **kwargs):
        packit_logger = logging.getLogger("packit")
        packit_logger.debug("try debug")
        packit_logger.info("try info")
        return "my.srpm"

    def inspect_log_date_format(logs=None, **_):

        timestamp_reg = re.compile(
            r"[0-9]+-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+\s.*"
        )

        log_lines = 0
        for line in logs.split("\n"):
            logger.debug(line)
            if len(line) == 0:
                continue
            log_lines += 1
            assert timestamp_reg.match(line)

        # Check if both test logs were recorded
        assert log_lines == 2

        return (None, None)

    trigger = flexmock(
        job_config_trigger_type=JobConfigTriggerType.pull_request, id=123
    )
    flexmock(AddPullRequestDbTrigger).should_receive("db_trigger").and_return(trigger)
    helper = build_helper(
        event=github_pr_event,
        metadata=JobMetadataConfig(targets=["bright-future"], scratch=True),
        db_trigger=trigger,
    )

    flexmock(GitProject).should_receive("set_commit_status").and_return().never()
    local_project = flexmock()
    local_project.working_dir = ""
    up = flexmock()
    up.local_project = local_project
    flexmock(PackitAPI).should_receive("up").and_return(up)

    # flexmock(PackitAPI).should_receive("up").and_return()
    flexmock(PackitAPI).should_receive("create_srpm").replace_with(mock_packit_log)
    flexmock(SRPMBuildModel).should_receive("create_with_new_run").replace_with(
        inspect_log_date_format
    )
    helper._create_srpm()
