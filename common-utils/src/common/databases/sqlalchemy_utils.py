from prefect_sqlalchemy import DatabaseCredentials

from common.settings import CommonSettings, NestedSettings, Settings

CS = CommonSettings()  # type: ignore


class SqlalchemyCredSettings(NestedSettings):
    """Settings that can be passed in to
    Snowflake Credentials.  This is mean to be used as
    a sub model of the SqlalchemySettings model.
    """

    url: str


class SqlalchemySettings(Settings):
    """Settings that can be passed in for
    Sqlalchemy connection.
    """

    sqlalchemy_credentials: SqlalchemyCredSettings


class MzsSqlalchemyCredentials(DatabaseCredentials):
    """Moz Social version of the Sqlalchemy DatabaseCredentials provided
    by Prefect-Sqlalchemy with settings already applied.
    All other base model attributes can be set explicitly here.

    See https://prefecthq.github.io/prefect-sqlalchemy/ for usage.
    """

    def __init__(self, **data):
        """Set credentials and other settings on usage of
        model.
        """
        settings = SqlalchemySettings()  # type: ignore
        data["url"] = settings.sqlalchemy_credentials.url
        super().__init__(**data)
