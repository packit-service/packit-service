# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

from typing import Optional

import pytest
from flexmock import flexmock
from marshmallow import ValidationError

from ogr.abstract import GitProject, GitService
from packit.config import PackageConfig
from packit.exceptions import PackitConfigException
from packit.sync import SyncFilesItem
from packit_service.config import ServiceConfig, Deployment, PackageConfigGetter
from packit_service.constants import TESTING_FARM_API_URL

try:
    from packit.config import SyncFilesConfig
except ImportError:
    pass


@pytest.fixture(scope="module")
def service_config_valid():
    return {
        "debug": True,
        "deployment": "prod",
        "authentication": {
            "github.com": {
                "github_app_id": "11111",
                "github_app_cert_path": "/path/lib",
            },
            "src.fedoraproject.org": {
                "instance_url": "https://src.fedoraproject.org",
                "token": "BINGO",
            },
        },
        "fas_user": "santa",
        "fas_password": "does-not-exist",
        "keytab_path": "/secrets/fedora.keytab",
        "webhook_secret": "secret",
        "validate_webhooks": True,
        "testing_farm_secret": "granko",
        "bugzilla_url": "https://ladybug-zilla",
        "bugzilla_api_key": "ratamahatta",
        "bugz_namespaces": ["^magic/namespace"],
        "bugz_branches": ["^onlythis$"],
        "command_handler": "sandcastle",
        "command_handler_work_dir": "/sandcastle",
        "command_handler_image_reference": "quay.io/packit/sandcastle",
        "command_handler_k8s_namespace": "packit-test-sandbox",
        "admins": ["Dasher", "Dancer", "Vixen", "Comet", "Blitzen"],
        "server_name": "hub.packit.org",
        "gitlab_webhook_tokens": ["token1", "token2", "token3", "aged"],
        "gitlab_token_secret": "jwt_secret",
        "enabled_private_namespaces": [
            "gitlab.com/private/namespace",
            "github.com/other-private-namespace",
        ],
    }


def test_parse_valid(service_config_valid):
    config = ServiceConfig.get_from_dict(service_config_valid)
    assert config.debug
    assert config.deployment == Deployment.prod
    assert config.fas_user == "santa"
    assert config.fas_password == "does-not-exist"
    assert config.keytab_path == "/secrets/fedora.keytab"
    assert config.webhook_secret == "secret"
    assert config.validate_webhooks
    assert config.testing_farm_secret == "granko"
    assert config.testing_farm_api_url == TESTING_FARM_API_URL
    assert config.bugzilla_url == "https://ladybug-zilla"
    assert config.bugzilla_api_key == "ratamahatta"
    assert config.bugz_namespaces == {"^magic/namespace"}
    assert config.bugz_branches == {"^onlythis$"}
    assert config.command_handler_work_dir == "/sandcastle"
    assert config.admins == {"Dasher", "Dancer", "Vixen", "Comet", "Blitzen"}
    assert config.server_name == "hub.packit.org"
    assert config.gitlab_token_secret == "jwt_secret"
    assert config.gitlab_webhook_tokens == {"token1", "token2", "token3", "aged"}
    assert config.enabled_private_namespaces == {
        "gitlab.com/private/namespace",
        "github.com/other-private-namespace",
    }


def test_parse_optional_values(service_config_valid):
    """When optional values are set, they are correctly parsed"""
    config = ServiceConfig.get_from_dict(
        {**service_config_valid, "testing_farm_api_url": "https://other.url"}
    )
    assert config.testing_farm_api_url == "https://other.url"


@pytest.fixture(scope="module")
def service_config_invalid():
    return {
        "deployment": False,  # wrong option
        "authentication": {
            "github.com": {
                "github_app_id": "11111",
                "github_app_cert_path": "/path/lib",
            }
        },
        "webhook_secret": "secret",
        "command_handler_work_dir": "/sandcastle",
        "command_handler_image_reference": "quay.io/packit/sandcastle",
        "command_handler_k8s_namespace": "packit-test-sandbox",
    }


def test_parse_invalid(service_config_invalid):
    with pytest.raises(ValidationError):
        ServiceConfig.get_from_dict(service_config_invalid)


@pytest.fixture()
def service_config_missing():
    return {}


def test_parse_missing(service_config_missing):
    with pytest.raises(ValidationError):
        ServiceConfig.get_from_dict(service_config_missing)


@pytest.mark.parametrize(
    "sc",
    (
        (ServiceConfig.get_from_dict({"deployment": "stg"})),
        (ServiceConfig()),
    ),
)
def test_config_opts(sc):
    """test that ServiceConfig knows all the options"""
    assert sc.server_name is not None
    assert sc.deployment == Deployment.stg
    assert sc.admins is not None
    assert sc.command_handler is not None
    assert sc.command_handler_work_dir is not None
    assert sc.command_handler_pvc_env_var is not None
    assert sc.command_handler_image_reference is not None
    assert sc.command_handler_k8s_namespace is not None
    assert sc.fas_password is not None
    assert sc.testing_farm_secret is not None
    assert sc.github_requests_log_path is not None
    assert sc.webhook_secret is not None
    assert sc.validate_webhooks is not None
    assert sc.gitlab_token_secret is not None
    assert sc.gitlab_webhook_tokens is not None


@pytest.mark.skipif(
    "SyncFilesConfig" not in globals(),
    reason="Remove after braking change in Packit is released.",
)
@pytest.mark.parametrize(
    "content,project,mock_spec_search,spec_path_option,spec_path,reference",
    [
        (
            "---\nspecfile_path: packit.spec\n"
            "synced_files:\n"
            "  - packit.spec\n"
            "  - src: .packit.yaml\n"
            "    dest: .packit2.yaml",
            GitProject(repo="", service=GitService(), namespace=""),
            True,
            None,
            "packit.spec",
            None,
        ),
        (
            "---\nspecfile_path: packit.spec\n"
            "synced_files:\n"
            "  - packit.spec\n"
            "  - src: .packit.yaml\n"
            "    dest: .packit2.yaml",
            GitProject(repo="", service=GitService(), namespace=""),
            True,
            None,
            "packit.spec",
            "some-branch",
        ),
        (
            "synced_files:\n"
            "  - packit.spec\n"
            "  - src: .packit.yaml\n"
            "    dest: .packit2.yaml",
            GitProject(repo="", service=GitService(), namespace=""),
            True,
            None,
            "packit.spec",
            None,
        ),
        (
            "synced_files:\n"
            "  - packit.spec\n"
            "  - src: .packit.yaml\n"
            "    dest: .packit2.yaml",
            GitProject(repo="", service=GitService(), namespace=""),
            False,
            "packit.spec",
            "packit.spec",
            None,
        ),
        (
            "---\n"
            "synced_files:\n"
            "  - src: .packit.yaml\n"
            "    dest: .packit2.yaml\n"
            "jobs: [{job: build, trigger: pull_request}]\n",
            GitProject(repo="", service=GitService(), namespace=""),
            False,
            "packit.spec",
            "packit.spec",
            None,
        ),
    ],
)
def test_get_package_config_from_repo(
    content,
    project: GitProject,
    mock_spec_search: bool,
    spec_path: Optional[str],
    spec_path_option: Optional[str],
    reference: str,
):
    gp = flexmock(GitProject)
    gp.should_receive("full_repo_name").and_return("a/b")
    gp.should_receive("get_file_content").with_args(
        path=".packit.yaml", ref=reference
    ).and_return(content)
    if mock_spec_search:
        gp.should_receive("get_files").and_return(["packit.spec"]).once()
    config = PackageConfigGetter.get_package_config_from_repo(
        project=project, reference=reference, spec_file_path=spec_path_option
    )
    assert isinstance(config, PackageConfig)
    assert config.specfile_path == spec_path
    assert set(config.get_all_files_to_sync().files_to_sync) == set(
        SyncFilesConfig(
            files_to_sync=[
                SyncFilesItem(src="packit.spec", dest="packit.spec"),
                SyncFilesItem(src=".packit.yaml", dest=".packit2.yaml"),
            ]
        ).files_to_sync
    )
    assert config.create_pr
    for j in config.jobs:
        assert j.specfile_path == spec_path
        assert j.downstream_package_name == config.downstream_package_name
        assert j.upstream_package_name == config.upstream_package_name


@pytest.mark.skipif(
    "SyncFilesConfig" not in globals(),
    reason="Remove after braking change in Packit is released.",
)
def test_get_package_config_from_repo_alternative_config_name():
    gp = flexmock(GitProject)
    gp.should_receive("full_repo_name").and_return("a/b")
    gp.should_receive("get_file_content").with_args(
        path=".packit.yaml", ref=None
    ).and_raise(FileNotFoundError, "not found")
    gp.should_receive("get_file_content").with_args(
        path=".packit.yml", ref=None
    ).and_return(
        "---\nspecfile_path: packit.spec\n"
        "synced_files:\n"
        "  - packit.spec\n"
        "  - src: .packit.yaml\n"
        "    dest: .packit2.yaml"
    )
    config = PackageConfigGetter.get_package_config_from_repo(
        project=GitProject(repo="", service=GitService(), namespace=""),
        reference=None,
        spec_file_path="packit.spec",
    )
    assert isinstance(config, PackageConfig)
    assert config.specfile_path == "packit.spec"
    assert config.synced_files == SyncFilesConfig(
        files_to_sync=[
            SyncFilesItem(src="packit.spec", dest="packit.spec"),
            SyncFilesItem(src=".packit.yaml", dest=".packit2.yaml"),
        ]
    )
    assert config.create_pr


def test_get_package_config_from_repo_not_found_exception_pr():
    gp = flexmock(GitProject)
    gp.should_receive("full_repo_name").and_return("a/b")
    gp.should_receive("get_file_content").and_raise(FileNotFoundError, "not found")
    gp.should_receive("pr_comment").and_return(None).once()
    with pytest.raises(PackitConfigException):
        PackageConfigGetter.get_package_config_from_repo(
            project=GitProject(repo="", service=GitService(), namespace=""),
            reference=None,
            pr_id=2,
        )


def test_get_package_config_from_repo_not_found():
    gp = flexmock(GitProject)
    gp.should_receive("full_repo_name").and_return("a/b")
    gp.should_receive("get_file_content").and_raise(FileNotFoundError, "not found")
    assert (
        PackageConfigGetter.get_package_config_from_repo(
            project=GitProject(repo="", service=GitService(), namespace=""),
            reference=None,
            pr_id=2,
            fail_when_missing=False,
        )
        is None
    )


def test_get_package_config_from_repo_not_found_exception_existing_issue():
    flexmock(GitService).should_receive("user").and_return(
        flexmock().should_receive("get_username").and_return("packit").mock()
    )
    gp = flexmock(GitProject)
    gp.should_receive("full_repo_name").and_return("a/b")
    gp.should_receive("get_file_content").and_raise(FileNotFoundError, "not found")
    gp.should_receive("get_issue_list").and_return(
        [flexmock(title="[packit] Invalid config")]
    ).once()
    with pytest.raises(PackitConfigException):
        PackageConfigGetter.get_package_config_from_repo(
            project=GitProject(repo="", service=GitService(), namespace=""),
            reference=None,
        )


def test_get_package_config_from_repo_not_found_exception_nonexisting_issue():
    flexmock(GitService).should_receive("user").and_return(
        flexmock().should_receive("get_username").and_return("packit").mock()
    )
    gp = flexmock(GitProject)
    gp.should_receive("full_repo_name").and_return("a/b")
    gp.should_receive("get_file_content").and_raise(FileNotFoundError, "not found")
    gp.should_receive("get_issue_list").and_return(
        [flexmock(title="issue 1"), flexmock(title="issue 2")]
    ).once()
    gp.should_receive("create_issue").and_return(flexmock(url="the url")).once()
    with pytest.raises(PackitConfigException):
        PackageConfigGetter.get_package_config_from_repo(
            project=GitProject(repo="", service=GitService(), namespace=""),
            reference=None,
        )


@pytest.mark.parametrize(
    "issues, create_new, title, message",
    [
        (
            [flexmock(title="Some random issue"), flexmock(title="Many issues")],
            True,
            "Created issue",
            "Let's make sure to deliver the message",
        ),
        (
            [
                flexmock(title="Some random issue"),
                flexmock(title="[packit] I was here"),
                flexmock(title="Many issues"),
            ],
            False,
            "I was here",
            "Down the rabbit hole",
        ),
        (
            [
                flexmock(title="Some random issue"),
                flexmock(title="[packit] I was here"),
                flexmock(title="Many issues"),
            ],
            True,
            "Something new",
            "Knock, knock! Here we go again!",
        ),
    ],
)
def test_create_issue_if_needed(issues, create_new, title, message):
    project = flexmock()
    check = lambda value: value is None  # noqa
    project.should_receive("get_issue_list").and_return(issues).once()

    if create_new:
        project.should_receive("create_issue").with_args(
            title=f"[packit] {title}", body=message
        ).and_return(flexmock(title="new issue")).once()
        check = lambda value: value.title == "new issue"  # noqa

    issue_created = PackageConfigGetter.create_issue_if_needed(project, title, message)
    assert check(issue_created)
