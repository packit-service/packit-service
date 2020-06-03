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

"""
Let's test that Steve's as awesome as we think he is.
"""
from json import dumps

import pytest
from flexmock import flexmock
from github import Github

from ogr.services.github import GithubProject
from packit.api import PackitAPI
from packit.config import JobConfigTriggerType
from packit.local_project import LocalProject
from packit_service.config import ServiceConfig
from packit_service.constants import SANDCASTLE_WORK_DIR
from packit_service.service.db_triggers import AddReleaseDbTrigger
from packit_service.worker.jobs import SteveJobs
from packit_service.worker.whitelist import Whitelist
from tests.spellbook import first_dict_value


@pytest.mark.parametrize(
    "event",
    (
        (
            {
                "action": "published",
                "release": {"tag_name": "1.2.3"},
                "repository": {
                    "name": "bar",
                    "html_url": "https://github.com/foo/bar",
                    "owner": {"login": "foo"},
                },
            }
        ),
    ),
)
def test_process_message(event):
    packit_yaml = {
        "specfile_path": "bar.spec",
        "synced_files": [],
        "jobs": [{"trigger": "release", "job": "propose_downstream"}],
    }

    flexmock(Github, get_repo=lambda full_name_or_id: None)
    flexmock(
        GithubProject,
        get_file_content=lambda path, ref: dumps(packit_yaml),
        full_repo_name="foo/bar",
        get_files=lambda ref, filter_regex: [],
        get_sha_from_tag=lambda tag_name: "12345",
        get_web_url=lambda: "https://github.com/the-namespace/the-repo",
        is_private=lambda: False,
    )
    flexmock(LocalProject, refresh_the_arguments=lambda: None)
    config = ServiceConfig()
    config.command_handler_work_dir = SANDCASTLE_WORK_DIR
    flexmock(ServiceConfig).should_receive("get_service_config").and_return(config)
    flexmock(PackitAPI).should_receive("sync_release").with_args(
        dist_git_branch="master", version="1.2.3"
    ).once()
    flexmock(AddReleaseDbTrigger).should_receive("db_trigger").and_return(
        flexmock(job_config_trigger_type=JobConfigTriggerType.release)
    )
    flexmock(Whitelist, check_and_report=True)
    results = SteveJobs().process_message(event)
    j = first_dict_value(results["jobs"])
    assert "propose_downstream" in list(results["jobs"].keys())[0]
    assert j["success"]
    assert results["event"]["trigger"] == "release"
