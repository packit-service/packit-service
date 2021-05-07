# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

import os

import pytest
from flexmock import flexmock

from ogr.abstract import CommitStatus
from ogr.services.github import GithubProject
from packit.config import JobConfig, JobConfigTriggerType, JobType, PackageConfig
from packit.config.job_config import JobMetadataConfig
from packit_service.config import ServiceConfig
from packit_service.constants import KOJI_PRODUCTION_BUILDS_ISSUE
from packit_service.models import GitBranchModel, PullRequestModel
from packit_service.worker.handlers import (
    JobHandler,
    CoprBuildHandler,
    KojiBuildHandler,
)
from packit_service.worker.reporting import StatusReporter


@pytest.fixture()
def trick_p_s_with_k8s():
    os.environ["KUBERNETES_SERVICE_HOST"] = "YEAH"  # trick p-s
    yield
    del os.environ["KUBERNETES_SERVICE_HOST"]


def test_handler_cleanup(tmp_path, trick_p_s_with_k8s):
    tmp_path.joinpath("a").mkdir()
    tmp_path.joinpath("b").write_text("a")
    tmp_path.joinpath("c").symlink_to("b")
    tmp_path.joinpath("d").symlink_to("a", target_is_directory=True)
    tmp_path.joinpath("e").symlink_to("nope", target_is_directory=False)
    tmp_path.joinpath("f").symlink_to("nopez", target_is_directory=True)
    tmp_path.joinpath(".g").write_text("g")
    tmp_path.joinpath(".h").symlink_to(".g", target_is_directory=False)

    c = ServiceConfig()
    pc = flexmock(PackageConfig)
    c.command_handler_work_dir = tmp_path
    jc = JobConfig(
        type=JobType.copr_build,
        trigger=JobConfigTriggerType.pull_request,
        metadata=JobMetadataConfig(),
    )
    j = JobHandler(package_config=pc, job_config=jc, event={})

    flexmock(j).should_receive("service_config").and_return(c)

    j._clean_workplace()

    assert len(list(tmp_path.iterdir())) == 0


def test_precheck(github_pr_event):
    flexmock(PullRequestModel).should_receive("get_or_create").with_args(
        pr_id=342,
        namespace="packit-service",
        repo_name="packit",
        project_url="https://github.com/packit-service/packit",
    ).and_return(
        flexmock(id=342, job_config_trigger_type=JobConfigTriggerType.pull_request)
    )

    copr_build_handler = CoprBuildHandler(
        package_config=PackageConfig(
            jobs=[
                JobConfig(
                    type=JobType.copr_build,
                    trigger=JobConfigTriggerType.pull_request,
                ),
                JobConfig(
                    type=JobType.tests,
                    trigger=JobConfigTriggerType.pull_request,
                ),
            ]
        ),
        job_config=JobConfig(
            type=JobType.copr_build,
            trigger=JobConfigTriggerType.pull_request,
        ),
        event=github_pr_event.get_dict(),
    )
    assert copr_build_handler.pre_check()


def test_precheck_push(github_push_event):
    flexmock(GitBranchModel).should_receive("get_or_create").and_return(
        flexmock(id=1, job_config_trigger_type=JobConfigTriggerType.commit)
    )

    copr_build_handler = CoprBuildHandler(
        package_config=PackageConfig(
            jobs=[
                JobConfig(
                    type=JobType.copr_build,
                    trigger=JobConfigTriggerType.commit,
                    metadata=JobMetadataConfig(branch="build-branch"),
                ),
            ]
        ),
        job_config=JobConfig(
            type=JobType.copr_build,
            trigger=JobConfigTriggerType.commit,
            metadata=JobMetadataConfig(branch="build-branch"),
        ),
        event=github_push_event.get_dict(),
    )

    assert copr_build_handler.pre_check()


def test_precheck_push_to_a_different_branch(github_push_event):
    flexmock(GitBranchModel).should_receive("get_or_create").and_return(
        flexmock(id=1, job_config_trigger_type=JobConfigTriggerType.commit)
    )

    copr_build_handler = CoprBuildHandler(
        package_config=PackageConfig(
            jobs=[
                JobConfig(
                    type=JobType.copr_build,
                    trigger=JobConfigTriggerType.commit,
                    metadata=JobMetadataConfig(branch="bad-branch"),
                ),
            ]
        ),
        job_config=JobConfig(
            type=JobType.copr_build,
            trigger=JobConfigTriggerType.commit,
            metadata=JobMetadataConfig(branch="bad-branch"),
        ),
        event=github_push_event.get_dict(),
    )
    assert not copr_build_handler.pre_check()


def test_precheck_koji_build_non_scratch(github_pr_event):
    flexmock(PullRequestModel).should_receive("get_or_create").with_args(
        pr_id=342,
        namespace="packit-service",
        repo_name="packit",
        project_url="https://github.com/packit-service/packit",
    ).and_return(
        flexmock(id=342, job_config_trigger_type=JobConfigTriggerType.pull_request)
    )
    flexmock(StatusReporter).should_receive("set_status").with_args(
        state=CommitStatus.error,
        description="Non-scratch builds not possible from upstream.",
        check_name="packit-stg/production-build-bright-future",
        url=KOJI_PRODUCTION_BUILDS_ISSUE,
    ).and_return().once()
    flexmock(GithubProject).should_receive("can_merge_pr").and_return(True)
    koji_build_handler = KojiBuildHandler(
        package_config=PackageConfig(
            jobs=[
                JobConfig(
                    type=JobType.production_build,
                    trigger=JobConfigTriggerType.pull_request,
                    metadata=JobMetadataConfig(
                        targets=["bright-future"], scratch=False
                    ),
                ),
            ]
        ),
        job_config=JobConfig(
            type=JobType.production_build,
            trigger=JobConfigTriggerType.pull_request,
            metadata=JobMetadataConfig(targets=["bright-future"], scratch=False),
        ),
        event=github_pr_event.get_dict(),
    )
    assert not koji_build_handler.pre_check()
