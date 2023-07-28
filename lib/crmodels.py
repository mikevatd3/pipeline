from sqlalchemy import ARRAY, TEXT
from sqlalchemy import Integer, String, Text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    Session
)

from .dtypes import Indentation, CensusTableName, CensusVariableName


## Database table definitions
Base = automap_base()


class CRColumnMetadata(Base):
    __tablename__ = "census_column_metadata"
    __table_args__ = {"schema": "census"}

    line_number: Mapped[int] = mapped_column(Integer(), nullable=True)
    indent: Mapped[Indentation] = mapped_column(Integer(), nullable=True)
    table_id: Mapped[str] = mapped_column(String(10), nullable=True)
    column_id: Mapped[CensusVariableName] = mapped_column(String(16), primary_key=True, nullable=False)
    column_title: Mapped[str] = mapped_column(Text(), nullable=True)
    parent_column_id: Mapped[str] = mapped_column(String(16), nullable=True)

    def __str__(self):
        return f"{self.column_id}: {self.column_title}"
    
    __repr__ = __str__


class CRTableMetadata(Base):
    __tablename__ = "census_table_metadata"
    __table_args__ = {"schema": "census"}

    table_id: Mapped[CensusTableName] = mapped_column(String(10), nullable=False, primary_key=True)
    table_title: Mapped[str] = mapped_column(Text(), nullable=True)
    simple_table_title: Mapped[str] = mapped_column(Text(), nullable=True)
    subject_area: Mapped[str] = mapped_column(Text(), nullable=True)
    universe: Mapped[str] = mapped_column(Text(), nullable=True)
    denominator_column_id: Mapped[str] = mapped_column(String(16), nullable=True)
    topics: Mapped[ARRAY[TEXT]] = mapped_column(ARRAY(TEXT, dimensions=1))
    # suppression_level: Mapped[int] = mapped_column(Integer(), nullable=True)

    def __str__(self):
        return f"{self.table_id}: {self.table_title}"

    __repr__ = __str__


class CRTabulationMetadata(Base):
    __tablename__ = "census_tabulation_metadata"

    tabulation_code: Mapped[str] = mapped_column(String(5), nullable=False, primary_key=True)
    table_title: Mapped[str] = mapped_column(Text())
    simple_table_title: Mapped[str] = mapped_column(Text())
    subject_area: Mapped[str] = mapped_column(Text())
    universe: Mapped[str] = mapped_column(Text())
    topics: Mapped[ARRAY[TEXT]] = mapped_column(ARRAY(TEXT, dimensions=1))
    weight: Mapped[int] = mapped_column(Integer())
    tables_in_one_yr: Mapped[ARRAY[TEXT]] = mapped_column(ARRAY(TEXT, dimensions=1))
    tables_in_three_yr: Mapped[ARRAY[TEXT]] = mapped_column(ARRAY(TEXT, dimensions=1))
    tables_in_five_yr: Mapped[ARRAY[TEXT]] = mapped_column(ARRAY(TEXT, dimensions=1))

    def __str__(self):
        return f"{self.tabulation_code}: {self.table_title}"

    __repr__ = __str__


def bind_cr_tables(db: Session):
    Base.prepare(autoload_with=db.get_bind())