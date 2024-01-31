"""
Translation functions between the D3 metadata schema and the Census 
Reporter metadata schema.
"""
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as posgres_upsert

from .connection import sqlalch_obj_to_dict
from .d3models import D3TableMetadata, D3VariableMetadata
from .crmodels import (
    CRColumnMetadata,
    CRTableMetadata,
    CRTabulationMetadata,
    bind_cr_tables,
)


def create_table_metadata_insert(table_metadata: D3TableMetadata) -> CRTableMetadata:
    denominator_column_id = table_metadata.table_name + "001"

    if table_metadata.table_topics is not None:
        table_topics = table_metadata.table_topics.split(",")
    else:
        table_topics = []

    return CRTableMetadata(
        table_id=table_metadata.table_name.upper(),
        table_title=table_metadata.description,
        simple_table_title=table_metadata.description_simple,
        subject_area=table_metadata.subject_area,
        universe=table_metadata.universe,
        denominator_column_id=denominator_column_id,
        topics=table_topics
    )


def create_variable_metadata_insert(
    d3_variables: list[D3VariableMetadata],
) -> list[CRColumnMetadata]:
    return [
        CRColumnMetadata(
            line_number=i,
            indent=d3_variable.indentation,
            table_id=d3_variable.table_name.upper(),
            column_id=d3_variable.variable_name.upper(),
            column_title=d3_variable.description,
            parent_column_id=d3_variable.parent_column,
        )
        for i, d3_variable in enumerate(d3_variables, 1)
    ]


def create_tabulation_metadata_insert(
    table_metadata: D3TableMetadata,
) -> CRTabulationMetadata:

    if table_metadata.table_topics is not None:
        table_topics = table_metadata.table_topics.split(",")
    else:
        table_topics = []

    return CRTabulationMetadata(
        tabulation_code=table_metadata.table_name[1:],  # Peel the 'b' off the front
        table_title=table_metadata.description,
        simple_table_title=table_metadata.description_simple,
        subject_area=table_metadata.subject_area,
        universe=table_metadata.universe,
        topics=table_topics,
        weight=0,
        tables_in_one_yr=[],
        tables_in_three_yr=[],
        tables_in_five_yr=[table_metadata.table_name.upper()],
    )


def update_metadata(
    db: Session,
    table_metadata: D3TableMetadata,
    variable_metadata: list[D3VariableMetadata],
):
    bind_cr_tables(db)

    destination_table_metadata = create_table_metadata_insert(table_metadata)
    destination_variable_metadata = create_variable_metadata_insert(variable_metadata)
    destination_tabulation_metadata = create_tabulation_metadata_insert(table_metadata)

    table_stmt = (
        posgres_upsert(CRTableMetadata)
            .values([sqlalch_obj_to_dict(destination_table_metadata)])
            .on_conflict_do_nothing(
                index_elements=["table_id"]
            )
    )

    variable_stmt = (
        posgres_upsert(CRColumnMetadata)
            .values([sqlalch_obj_to_dict(variable) for variable in destination_variable_metadata])
            .on_conflict_do_nothing(
                index_elements=["column_id"]
            )
    )

    tabulation_stmt = (
        posgres_upsert(CRTabulationMetadata)
            .values([sqlalch_obj_to_dict(destination_tabulation_metadata)])
            .on_conflict_do_nothing(
                index_elements=["tabulation_code"]
            )
    )

    db.execute(table_stmt)
    db.execute(variable_stmt)

    db.execute(tabulation_stmt)

    db.commit()
