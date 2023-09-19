from typing import Optional
from enum import Enum as _Enum, auto
import pandas as pd
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import (
    Integer,
    String,
    Text,
    ForeignKey,
    Table,
    Column,
    select,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    Session,
    relationship,
)
from sqlalchemy.dialects.postgresql import ENUM
from .connection import sqlalch_obj_to_dict


## Database table definitions
class Base(DeclarativeBase):
    pass


class TimeFrame(_Enum):
    PAST = auto()
    PRESENT = auto()
    UNDESIGNATED = auto()


class D3TableMetadata(Base):
    __tablename__ = "d3_table_metadata"

    id: Mapped[int] = mapped_column(primary_key=True)
    table_name: Mapped[str] = mapped_column(String(8), unique=True)
    category: Mapped[str] = mapped_column(String(25), nullable=True)
    description: Mapped[str] = mapped_column(String(200), nullable=True)
    description_simple: Mapped[str] = mapped_column(String(50), nullable=True)
    table_topics: Mapped[str] = mapped_column(String(200), nullable=True)
    universe: Mapped[str] = mapped_column(String(50), nullable=True)
    subject_area: Mapped[str] = mapped_column(String(50), nullable=True)
    source: Mapped[str] = mapped_column(Text(), nullable=True)
    suppression_threshold: Mapped[int] = mapped_column(Integer(), nullable=True)
    tool: Mapped[str] = mapped_column(String(10), nullable=True)
    documentation: Mapped[str] = mapped_column(Text(), nullable=True)

    variables: Mapped[list["D3VariableMetadata"]] = relationship(
        back_populates="table"
    )
    variable_groups: Mapped[list["D3VariableGroup"]] = relationship(
        back_populates="table"
    )
    all_editions: Mapped[list["D3EditionMetadata"]] = relationship(
        back_populates="table"
    )

    def __str__(self):
        return f"{self.table_name}: {self.description_simple}"

    __repr__ = __str__

    def select_timeframe(self, timeframe: str, db):
        stmt = (
            select(D3EditionMetadata)
            .where(D3EditionMetadata.time_frame == timeframe)
            .where(D3EditionMetadata.table_name == self.table_name)
        )

        return db.scalars(stmt).first()

    def past(self, db):
        return self.select_timeframe('PAST', db)

    def present(self, db):
        return self.select_timeframe('PRESENT', db)


class Unit(_Enum):
    # ACS
    PERSON = auto()
    HOUSEHOLD = auto()

    # HIP (DETODP, ROD)
    PARCELS = auto()
    TRANSACTION = auto()

    # MISC
    SQ_MILE = auto()
    PERCENT = auto()

    # Other
    OTHER = auto()


class D3VariableMetadata(Base):
    __tablename__ = "d3_variable_metadata"

    id: Mapped[int] = mapped_column(primary_key=True)
    variable_name: Mapped[str] = mapped_column(String(12), unique=True)
    table_name: Mapped[str] = mapped_column(
        ForeignKey("d3_table_metadata.table_name")
    )
    indentation: Mapped[int] = mapped_column(Integer(), nullable=True)
    description: Mapped[str] = mapped_column(String(200), nullable=True)
    parent_column: Mapped[str] = mapped_column(String(12), nullable=True)
    sql_aggregation_phrase: Mapped[str] = mapped_column(Text(), nullable=True)
    # units: Mapped[Unit] = mapped_column(Enum(Unit))
    documentation: Mapped[str] = mapped_column(Text(), nullable=True)

    table: Mapped[D3TableMetadata] = relationship(back_populates="variables")
    child_variable_groups: Mapped["D3VariableGroup"] = relationship(
        back_populates="parent_variable"
    )

    def __str__(self):
        return f"{self.variable_name}: {self.description}"

    __repr__ = __str__


class D3EditionMetadata(Base):
    __tablename__ = "d3_edition_metadata"

    id: Mapped[int] = mapped_column(primary_key=True)
    table_name: Mapped[str] = mapped_column(
        ForeignKey("d3_table_metadata.table_name")
    )

    edition: Mapped[str] = mapped_column(
        String(20)
    )  # This is d3_past or d3_present
    documentation: Mapped[str] = mapped_column(Text(), nullable=True)
    raw_table_db: Mapped[str] = mapped_column(String(20), nullable=True)
    raw_table_schema: Mapped[str] = mapped_column(String(50), nullable=True)
    raw_table_name: Mapped[str] = mapped_column(String(50), nullable=True)
    time_frame: Mapped[TimeFrame] = mapped_column(
        ENUM("PAST", "PRESENT", "UNDESIGNATED", name="timeframe"),
        server_default="UNDESIGNATED",
    )

    table: Mapped[D3TableMetadata] = relationship(back_populates="all_editions")

    def __str__(self) -> str:
        return f"{self.table.table_name} for {self.edition}"

    __repr__ = __str__


class D3VariableGroup(Base):
    __tablename__ = "d3_variable_groups"
    """
    This will group breakdown variables to make the dua suppression have
    a lighter touch.
    """

    id: Mapped[int] = mapped_column(Integer(), primary_key=True)
    table_name: Mapped[str] = mapped_column(
        ForeignKey("d3_table_metadata.table_name")
    )
    description: Mapped[str] = mapped_column(String(100), nullable=False)
    documentation: Mapped[str] = mapped_column(Text(), nullable=True)
    parent_variable_name: Mapped[int] = mapped_column(
        ForeignKey("d3_variable_metadata.variable_name")
    )

    parent_variable: Mapped[D3VariableMetadata] = relationship(
        back_populates="child_variable_groups"
    )
    variables: Mapped[list[D3VariableMetadata]] = relationship(
        secondary="d3_variable_group_membership"
    )
    table: Mapped[list[D3TableMetadata]] = relationship(
        back_populates="variable_groups"
    )


membership = Table(
    "d3_variable_group_membership",
    Base.metadata,
    Column("group_id", ForeignKey("d3_variable_groups.id"), primary_key=True),
    Column(
        "variable_id",
        ForeignKey("d3_variable_metadata.variable_name"),
        primary_key=True,
    ),
)


def update_workspace_schema(schema_name: str = "mike"):
    D3TableMetadata.metadata.schema = schema_name
    D3VariableMetadata.metadata.schema = schema_name


def bind_d3_metadata_tables(db: Session):
    Base.metadata.create_all(db.get_bind())


def read_table_variables_to_dataframe(
    variables: list[D3VariableMetadata],
) -> pd.DataFrame:
    return pd.DataFrame.from_records(
        [sqlalch_obj_to_dict(variable) for variable in variables]
    )


def get_variable_metadata(
    db: Session, table_name: str
) -> list[D3VariableMetadata]:
    """
    Pull the metadata object for each variable in the table.
    """
    stmt = (
        select(D3VariableMetadata)
        .where(D3VariableMetadata.table_name == table_name)
        .order_by(D3VariableMetadata.variable_name)
    )

    return list(
        db.scalars(stmt)
    )  # Cast to a list so it's not consumed while building query parts


def get_table_metadata(
    db: Session, table_name: str
) -> Optional[D3TableMetadata]:
    """
    Pull the table metadata object.
    """
    stmt = select(D3TableMetadata).where(
        D3TableMetadata.table_name == table_name
    )

    return db.scalar(stmt)


class InvalidEditionError(Exception):
    pass


class InvalidTableError(Exception):
    pass


def get_latest_edition_metadata(
    db: Session, table_name: str
) -> D3EditionMetadata:
    stmt = (
        select(D3EditionMetadata)
        .where(D3EditionMetadata.table_name == table_name)
        .order_by(D3EditionMetadata.edition.desc())
        .limit(1)
    )

    result = db.scalar(stmt)

    if not result:
        raise InvalidTableError(
            f"'{table_name}' has no editions available -- update d3_edition_metadata to fix."
        )

    return result


def get_edition_metadata(
    db: Session, table_name: str, edition: str
) -> D3EditionMetadata:
    """
    Pull the aggregation edition metadata object.
    """
    stmt = (
        select(D3EditionMetadata)
        .where(D3EditionMetadata.table_name == table_name)
        .where(D3EditionMetadata.edition == edition)
    )
    result = db.scalar(stmt)

    if not result:
        raise InvalidEditionError(
            f"'{edition}' is not a valid edition for table {table_name} -- update d3_edition_metadata to fix."
        )

    return result


if __name__ == "__main__":
    from pathlib import Path
    from connection import build_connections
    import tomli

    with open(Path.cwd() / "pipeline_config.toml", "rb") as f:
        config = tomli.load(f)

    _, workspace_sessionmaker, _ = build_connections(config)

    WorkspaceSession = workspace_sessionmaker()

    Base.metadata.create_all(WorkspaceSession().get_bind())
