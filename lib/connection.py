from typing import Optional, Callable
from urllib.parse import quote

from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session


def build_engine(
    host: str,
    dbname: str,
    user: str,
    password: str,
    schema: Optional[str] = None,
    schema_translate_map: Optional[dict[str, str]] = None,
) -> Engine:
    if (not schema) & (not schema_translate_map):
        return create_engine(
            f"postgresql+psycopg2://{user}:{quote(password)}@{host}/{dbname}"
        )

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{quote(password)}@{host}/{dbname}",
        connect_args={"options": f"-csearch_path={schema}"},
    )

    if schema_translate_map:
        return engine.execution_options(schema_translate_map=schema_translate_map)
    
    return engine


def build_connections(config: dict[str, str]) -> tuple[Callable[[str, str], Session], Callable[[],Session], Callable[[str, str], Session]]:
    # This one is remote
    workspace_engine = build_engine(**config["workspace_db"])
    
    # This one is local (through vpn)
    def source_session_maker(db: str, schema: str):
        engine = build_engine(
            config["source_db"]["host"],
            db,
            config["source_db"]["user"],
            config["source_db"]["password"],
            # schema=schema,
        )
        return sessionmaker(engine)

    # Also remote
    def destination_session_maker(schema: str):
        engine = build_engine(
            **config["destination_db"],
            schema_translate_map={None: "public", "census": schema}
        )

        return sessionmaker(engine)

    return source_session_maker, lambda: sessionmaker(workspace_engine), destination_session_maker


def build_local_connection():
    pass


def build_remote_connection():
    pass


def sqlalch_obj_to_dict(alch_obj):
    return {key: getattr(alch_obj, key) for key in alch_obj.__table__.columns.keys()}
