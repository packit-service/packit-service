# Copyright Contributors to the Packit project.
# SPDX-License-Identifier: MIT

import enum
import logging
from pathlib import Path
from typing import List, Optional, Set, Union, NamedTuple

from yaml import safe_load

from ogr.abstract import GitProject, Issue
from packit.config import (
    Config,
    PackageConfig,
    RunCommandType,
    get_package_config_from_repo,
)
from packit.exceptions import PackitConfigException, PackitException
from packit_service.constants import (
    CONFIG_FILE_NAME,
    SANDCASTLE_DEFAULT_PROJECT,
    SANDCASTLE_IMAGE,
    SANDCASTLE_PVC,
    SANDCASTLE_WORK_DIR,
    TESTING_FARM_API_URL,
)

logger = logging.getLogger(__name__)


class Deployment(enum.Enum):
    dev = "dev"
    stg = "stg"
    prod = "prod"


class ProjectToSync(NamedTuple):
    """
    Project we want to sync from downstream.
    """

    forge: str
    repo_namespace: str
    repo_name: str
    branch: str
    dg_repo_name: str
    dg_branch: str

    def __repr__(self):
        return (
            f"ProjectToSync(forge={self.forge}, repo_namespace={self.repo_namespace}, "
            f"repo_name={self.repo_name}, branch={self.branch}, "
            f"dg_repo_name={self.dg_repo_name}, dg_branch={self.dg_branch})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ProjectToSync):
            raise NotImplementedError()

        return (
            self.forge == other.forge
            and self.repo_name == other.repo_name
            and self.repo_namespace == other.repo_namespace
            and self.branch == other.branch
            and self.dg_repo_name == other.dg_repo_name
            and self.dg_branch == other.dg_branch
        )


class ServiceConfig(Config):
    def __init__(
        self,
        deployment: Deployment = Deployment.stg,
        webhook_secret: str = "",
        testing_farm_secret: str = "",
        testing_farm_api_url: str = "",
        validate_webhooks: bool = True,
        admins: list = None,
        fas_password: Optional[str] = "",
        bugzilla_url: str = "",
        bugzilla_api_key: str = "",
        bugz_namespaces: List[str] = None,
        bugz_branches: List[str] = None,
        gitlab_webhook_tokens: List[str] = None,
        enabled_private_namespaces: Union[Set[str], List[str]] = None,
        gitlab_token_secret: str = "",
        projects_to_sync: List[ProjectToSync] = None,
        dashboard_url: str = "",
        koji_logs_url: str = "https://kojipkgs.fedoraproject.org",
        koji_web_url: str = "https://koji.fedoraproject.org",
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.deployment = deployment
        self.webhook_secret = webhook_secret
        # Common secret to authenticate both, packit service (when sending request to testing farm)
        # and testing farm (when sending notification to packit service's webhook).
        # We might later use different secrets for those two use cases.
        self.testing_farm_secret = testing_farm_secret
        self.testing_farm_api_url = testing_farm_api_url
        self.validate_webhooks = validate_webhooks

        # fas.fedoraproject.org needs password to authenticate
        # 'fas_user' is inherited from packit.config.Config
        self.fas_password = fas_password

        self.bugzilla_url = bugzilla_url
        self.bugzilla_api_key = bugzilla_api_key
        # Create bugs only for MRs against these namespaces
        self.bugz_namespaces: Set[str] = set(
            bugz_namespaces or ["redhat/centos-stream/src"]
        )
        # Create bugs only for MRs against one of these branches (regex set)
        self.bugz_branches: Set[str] = set(bugz_branches or [r"^c8s"])

        # List of github users who are allowed to trigger p-s on any repository
        self.admins: Set[str] = set(admins or [])

        # for flask SERVER_NAME so we can create links to logs
        self.server_name: str = ""

        # Makeshift for now to authenticate webhooks coming from gitlab instances
        # Old way of authenticating
        self.gitlab_webhook_tokens: Set[str] = set(gitlab_webhook_tokens or [])

        # Gitlab token secret to decode JWT tokens
        self.gitlab_token_secret: str = gitlab_token_secret

        # Explicit list of private namespaces we work with
        # e.g.:
        #  - github.com/other-private-namespace
        #  - gitlab.com/private/namespace
        self.enabled_private_namespaces: Set[str] = set(
            enabled_private_namespaces or []
        )

        self.projects_to_sync = projects_to_sync or []

        # Full URL to the dashboard, e.g. https://dashboard.packit.dev
        self.dashboard_url = dashboard_url
        self.koji_logs_url = koji_logs_url
        self.koji_web_url = koji_web_url

    service_config = None

    def __repr__(self):
        def hide(token: str) -> str:
            return f"{token[:1]}***{token[-1:]}" if token else ""

        return (
            f"{self.__class__.__name__}("
            f"{super().__repr__()}, "
            f"deployment='{self.deployment}', "
            f"webhook_secret='{hide(self.webhook_secret)}', "
            f"testing_farm_secret='{hide(self.testing_farm_secret)}', "
            f"testing_farm_api_url='{self.testing_farm_api_url}', "
            f"validate_webhooks='{self.validate_webhooks}', "
            f"admins='{self.admins}', "
            f"fas_password='{hide(self.fas_password)}', "
            f"bugzilla_url='{self.bugzilla_url}', "
            f"bugzilla_api_key='{hide(self.bugzilla_api_key)}', "
            f"gitlab_webhook_tokens='{self.gitlab_webhook_tokens}',"
            f"gitlab_token_secret='{hide(self.gitlab_token_secret)}',"
            f"enabled_private_namespaces='{self.enabled_private_namespaces}',"
            f"server_name='{self.server_name}', "
            f"dashboard_url='{self.dashboard_url}', "
            f"koji_logs_url='{self.koji_logs_url}', "
            f"koji_web_url='{self.koji_web_url}')"
        )

    def use_stage(self) -> bool:
        return self.deployment != Deployment.prod

    @classmethod
    def get_from_dict(cls, raw_dict: dict) -> "ServiceConfig":
        # required to avoid circular imports
        from packit_service.schema import ServiceConfigSchema

        config = ServiceConfigSchema().load(raw_dict)

        config.server_name = raw_dict.get("server_name", "localhost:5000")

        config.command_handler = RunCommandType.local
        a_h = raw_dict.get("command_handler")
        if a_h:
            config.command_handler = RunCommandType(a_h)
        config.command_handler_work_dir = raw_dict.get(
            "command_handler_work_dir", SANDCASTLE_WORK_DIR
        )
        config.command_handler_pvc_env_var = raw_dict.get(
            "command_handler_pvc_env_var", SANDCASTLE_PVC
        )
        config.command_handler_image_reference = raw_dict.get(
            "command_handler_image_reference", SANDCASTLE_IMAGE
        )
        # default project for oc cluster up
        config.command_handler_k8s_namespace = raw_dict.get(
            "command_handler_k8s_namespace", SANDCASTLE_DEFAULT_PROJECT
        )

        config.testing_farm_api_url = raw_dict.get(
            "testing_farm_api_url", TESTING_FARM_API_URL
        )

        logger.debug(f"Loaded config: {config}")
        return config

    @classmethod
    def get_service_config(cls) -> "ServiceConfig":
        if cls.service_config is None:
            directory = Path.home() / ".config"
            config_file_name_full = directory / CONFIG_FILE_NAME
            logger.debug(f"Loading service config from directory: {directory}")

            try:
                loaded_config = safe_load(open(config_file_name_full))
            except Exception as ex:
                logger.error(f"Cannot load service config '{config_file_name_full}'.")
                raise PackitException(f"Cannot load service config: {ex}.")

            cls.service_config = ServiceConfig.get_from_dict(raw_dict=loaded_config)
        return cls.service_config

    def get_project_to_sync(self, dg_repo_name, dg_branch) -> Optional[ProjectToSync]:
        projects = [
            project
            for project in self.projects_to_sync
            if project.dg_repo_name == dg_repo_name and project.dg_branch == dg_branch
        ]
        if projects:
            logger.info(f"Found project to sync: {projects[0]}.")
            return projects[0]
        return None


class PackageConfigGetter:
    @staticmethod
    def create_issue_if_needed(
        project: GitProject, title: str, message: str
    ) -> Optional[Issue]:
        # TODO: Improve filtering
        issues = project.get_issue_list()
        title = f"[packit] {title}"

        if any(title in issue.title for issue in issues):
            return None

        # TODO: store in DB
        return project.create_issue(title=title, body=message)

    @staticmethod
    def get_package_config_from_repo(
        project: GitProject,
        reference: Optional[str] = None,
        base_project: Optional[GitProject] = None,
        pr_id: int = None,
        fail_when_missing: bool = True,
        spec_file_path: Optional[str] = None,
    ) -> Optional[PackageConfig]:
        """
        Get the package config and catch the invalid config scenario and possibly no-config scenario
        """

        if not base_project and not project:
            return None

        project_to_search_in = base_project or project
        try:
            package_config: PackageConfig = get_package_config_from_repo(
                project=project_to_search_in,
                ref=reference,
                spec_file_path=spec_file_path,
            )
            if not package_config and fail_when_missing:
                raise PackitConfigException(
                    f"No config file found in {project_to_search_in.full_repo_name} "
                    "on ref '{reference}'"
                )

            if package_config and (
                package_config.current_version_command
                or package_config.create_tarball_command
            ):
                # TODO: Remove once the commands are removed from packit tooling.
                current_version_set = (
                    "- `current_version_command`\n"
                    if package_config.current_version_command
                    else ""
                )
                create_tarball_set = (
                    "- `create_tarball_command`\n"
                    if package_config.create_tarball_command
                    else ""
                )
                message = (
                    "Your config appears to use:\n"
                    f"{current_version_set}{create_tarball_set}"
                    "Those options will soon be deprecated and superseded by actions, "
                    "please adjust you packit configuration.\n\n"
                    "For more info, please check out the documentation: "
                    "https://packit.dev/docs/actions/ or contact us - "
                    "[Packit team]"
                    "(https://github.com/orgs/packit/teams/the-packit-team)"
                )

                if created_issue := PackageConfigGetter.create_issue_if_needed(
                    project,
                    title="Deprecated options used in configuration",
                    message=message,
                ):
                    logger.debug(
                        "Created issue for soon-to-be-deprecated packit config: "
                        f"{created_issue.url}"
                    )
        except PackitConfigException as ex:
            message = (
                f"Failed to load packit config file:\n```\n{str(ex)}\n```\n"
                "For more info, please check out the documentation: "
                "https://packit.dev/packit-as-a-service/ or contact us - "
                "[Packit team]"
                "(https://github.com/orgs/packit/teams/the-packit-team)"
            )

            if pr_id:
                project.pr_comment(pr_id, message)
            elif created_issue := PackageConfigGetter.create_issue_if_needed(
                project, title="Invalid config", message=message
            ):
                logger.debug(
                    f"Created issue for invalid packit config: {created_issue.url}"
                )
            raise ex
        return package_config
