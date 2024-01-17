from typing import Optional, Callable
from urllib.parse import quote

from sshtunnel import open_tunnel
from sqlalchemy import create_engine, Engine


def _build_engine(
    host: str,
    port: str,
    dbname: str,
    user: str,
    password: str,
    schema: Optional[str] = None,
    schema_translate_map: Optional[dict[str | None, str]] = None,
) -> Engine:
    """
    This is the generic engine builder. This file will also provide the
    specific engine builders which will be called into the main pipeline file.
    """
    if (not schema) & (not schema_translate_map):
        return create_engine(
            f"postgresql+psycopg2://{user}:{quote(password)}@{host}:{port}/{dbname}"
        )

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{quote(password)}@{host}:{port}/{dbname}",
        connect_args={"options": f"-csearch_path={schema},public"},
    )

    if schema_translate_map:
        return engine.execution_options(
            schema_translate_map=schema_translate_map
        )

    return engine


def build_workspace_engine(config, port):
    return _build_engine(
        '127.0.0.1',
        port,
        config["workspace_db"]["dbname"],
        config["workspace_db"]["user"],
        config["workspace_db"]["password"],
        schema=config["workspace_db"].get("schema")
    )


def build_source_engine(config, port, db_name):
    return _build_engine(
        config["source_db"]["host"],
        port,
        db_name,
        config["source_db"]["user"],
        config["source_db"]["password"],
    )


def build_destination_engine(config, port, schema):
    """
    Currently the destination db is on a box that we have to ssh into,
    so we provide slightly different settings.
    """
    return _build_engine(
        config["destination_db"]["host"],
        port,
        config["destination_db"]["dbname"],
        config["destination_db"]["user"],
        config["destination_db"]["password"],
        schema_translate_map={None: "public", "census": schema},
    )


def open_workspace_tunnel(config):
    return open_tunnel(
        (config["workspace_db"]["host"], 22),
        ssh_username=config["workspace_machine"]["user"],
        ssh_password=config["workspace_machine"]["password"],
        remote_bind_address=('127.0.0.1', 5432)
    )


def open_destination_tunnel(config):
    return open_tunnel(
        (config["destination_db"]["host"], 22),
        ssh_username=config["destination_machine"]["user"],
        ssh_password=config["destination_machine"]["password"],
        remote_bind_address=('127.0.0.1', 5432)
    )


def sqlalch_obj_to_dict(alch_obj):
    return {
        key: getattr(alch_obj, key) for key in alch_obj.__table__.columns.keys()
    }
