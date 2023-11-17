from common.settings import CommonSettings, NestedSettings, Settings, SecretsConfig
from prefect_sqlalchemy import DatabaseCredentials

CS = CommonSettings()  # type: ignore


class SqlalchemyCredSettings(NestedSettings):
    """Settings that can be passed in to
    Snowflake Credentials.  This is mean to be used as
    a sub model of the SqlalchemySettings model.
    """

    aurora_rds_url: str


class SqlalchemySettings(Settings):
    """Settings that can be passed in for
    Sqlalchemy connection.
    """

    sqlalchemy_credentials: SqlalchemyCredSettings

    class Config(SecretsConfig):
        prefix = f"data-flows/{CS.deployment_type}"


SETTINGS = SqlalchemySettings()  # type: ignore


class MozSqlalchemyCredentials(DatabaseCredentials):
    """Moz Social version of the Sqlalchemy DatabaseCredentials provided
    by Prefect-Sqlalchemy with settings already applied.
    All other base model attributes can be set explicitly here.

    See https://prefecthq.github.io/prefect-sqlalchemy/ for usage.
    """

    url: str

    def __init__(self, **data):
        """Set credentials and other settings on usage of
        model.
        """
        settings = SETTINGS
        data["url"] = settings.sqlalchemy_credentials.dict()[self.url]
        super().__init__(**data)
