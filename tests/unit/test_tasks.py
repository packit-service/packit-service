# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

import pytest
from celery.app.task import Task
from copr.v3 import CoprRequestException
from flexmock import flexmock
from packit_service.worker.tasks import run_copr_build_handler
from packit_service.worker.handlers import CoprBuildHandler


def test_autoretry():
    flexmock(CoprBuildHandler).should_receive("run_job").and_raise(
        CoprRequestException
    ).once()

    # verify that retry is called automatically
    flexmock(Task).should_receive("retry").and_raise(CoprRequestException).once()
    with pytest.raises(CoprRequestException):
        run_copr_build_handler({}, {}, {})
