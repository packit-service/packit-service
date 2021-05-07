# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

import typing

from marshmallow import ValidationError, fields, post_load, Schema

from packit.schema import UserConfigSchema
from packit_service.config import Deployment, ServiceConfig, ProjectToSync


class DeploymentField(fields.Field):
    def _serialize(self, value: typing.Any, attr: str, obj: typing.Any, **kwargs):
        raise NotImplementedError

    def _deserialize(
        self,
        value: typing.Any,
        attr: typing.Optional[str],
        data: typing.Optional[typing.Mapping[str, typing.Any]],
        **kwargs,
    ) -> Deployment:
        if not isinstance(value, str):
            raise ValidationError("Invalid data provided. str required")

        return Deployment(value)


class ProjectToSyncSchema(Schema):
    """
    Schema for projects to sync.
    """

    forge = fields.String(required=True)
    repo_namespace = fields.String(required=True)
    repo_name = fields.String(required=True)
    branch = fields.String(required=True)
    dg_repo_name = fields.String(required=True)
    dg_branch = fields.String(required=True)

    @post_load
    def make_instance(self, data, **_):
        return ProjectToSync(**data)


class ServiceConfigSchema(UserConfigSchema):
    deployment = DeploymentField(required=True)
    webhook_secret = fields.String()
    testing_farm_secret = fields.String()
    testing_farm_api_url = fields.String()
    fas_password = fields.String(default="")
    validate_webhooks = fields.Bool(default=False)
    bugzilla_url = fields.String(default="")
    bugzilla_api_key = fields.String(default="")
    bugz_namespaces = fields.List(fields.String())
    bugz_branches = fields.List(fields.String())
    admins = fields.List(fields.String())
    server_name = fields.String()
    gitlab_webhook_tokens = fields.List(fields.String())
    gitlab_token_secret = fields.String()
    enabled_private_namespaces = fields.List(fields.String())
    projects_to_sync = fields.List(fields.Nested(ProjectToSyncSchema), missing=None)
    dashboard_url = fields.String()
    koji_logs_url = fields.String()
    koji_web_url = fields.String()

    @post_load
    def make_instance(self, data, **kwargs):
        return ServiceConfig(**data)
