"""
Let's test flask views.
"""
from datetime import datetime

import pytest
from flexmock import flexmock

from packit_service.models import (
    CoprBuildModel,
    JobTriggerModelType,
    SRPMBuildModel,
)
from packit_service.service.app import packit_as_a_service as application
from packit_service.service.urls import (
    get_copr_build_info_url_from_flask,
    get_srpm_log_url_from_flask,
)


@pytest.fixture
def client():
    application.config["TESTING"] = True
    # this affects all tests actually, heads up!
    application.config["SERVER_NAME"] = "localhost:5000"
    application.config["PREFERRED_URL_SCHEME"] = "https"

    with application.test_client() as client:
        yield client


@pytest.fixture(autouse=True)
def _setup_app_context_for_test():
    """
    Given app is session-wide, sets up a app context per test to ensure that
    app and request stack is not shared between tests.
    """
    ctx = application.app_context()
    ctx.push()
    yield  # tests will run here
    ctx.pop()


def test_get_logs(client):
    chroot = "foo-1-x86_64"
    state = "success"
    build_id = 2

    project_mock = flexmock()
    project_mock.namespace = "john-foo"
    project_mock.repo_name = "bar"

    pr_mock = flexmock()
    pr_mock.job_trigger_model_type = JobTriggerModelType.pull_request
    pr_mock.pr_id = 234
    pr_mock.project = project_mock

    srpm_build_mock = flexmock()
    srpm_build_mock.id = 11
    srpm_build_mock.url = "https://some.random.copr.subdomain.org/my_srpm.srpm"
    srpm_build_mock.build_submitted_time = datetime(
        year=2020, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
    )

    copr_build_mock = flexmock()
    copr_build_mock.target = chroot
    copr_build_mock.build_id = str(build_id)
    copr_build_mock.status = state
    copr_build_mock.web_url = (
        "https://copr.fedorainfracloud.org/coprs/john-foo-bar/john-foo-bar/build/2/"
    )
    copr_build_mock.build_logs_url = "https://localhost:5000/build/2/foo-1-x86_64/logs"
    copr_build_mock.owner = "packit"
    copr_build_mock.build_submitted_time = datetime(
        year=2020, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
    )
    copr_build_mock.project_name = "example_project"
    copr_build_mock.should_receive("get_trigger_object").and_return(pr_mock)
    copr_build_mock.should_receive("get_project").and_return(project_mock)
    copr_build_mock.should_receive("get_srpm_build").and_return(srpm_build_mock)

    flexmock(CoprBuildModel).should_receive("get_by_id").and_return(copr_build_mock)

    url = "/copr-build/1"
    logs_url = get_copr_build_info_url_from_flask(1)
    assert logs_url.endswith(url)

    resp = client.get(url).data.decode()
    assert f"srpm-build/{srpm_build_mock.id}/logs" in resp
    assert copr_build_mock.web_url in resp
    assert copr_build_mock.build_logs_url in resp
    assert copr_build_mock.target in resp
    assert "Status: success" in resp
    assert "You can install" in resp

    assert "Download SRPM" in resp
    assert srpm_build_mock.url in resp


def test_get_srpm_logs(client):
    srpm_build_mock = flexmock()
    srpm_build_mock.id = 2
    srpm_build_mock.logs = "asd\nqwe"

    flexmock(SRPMBuildModel).should_receive("get_by_id").and_return(srpm_build_mock)

    url = "/srpm-build/2/logs"
    logs_url = get_srpm_log_url_from_flask(2)
    assert logs_url.endswith(url)

    resp = client.get(url).data.decode()
    assert srpm_build_mock.logs in resp
    assert f"build {srpm_build_mock.id}" in resp
