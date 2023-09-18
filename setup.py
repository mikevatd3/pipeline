from pathlib import Path

from sqlalchemy.dialects.postgresql import insert as posgres_upsert
import pandas as pd
import tomli

from lib.d3models import (
    D3TableMetadata, 
    D3VariableMetadata, 
    D3EditionMetadata, 
    bind_d3_metadata_tables
)
from lib.connection import build_connections


with open("pipeline_config.toml", "rb") as f:
    config = tomli.load(f)


_, WorkspaceSession, _ = build_connections(config)


new = pd.read_excel(Path.cwd() / "data" /"metadata_source.xlsx")

new_prepped = new.rename(columns={
    "Table": "table_name",
    "Category": "category",
    "Source": "source",
    "Suppression threshold": "suppression_threshold",
    "Documentation": "documentation",
    "Tool": "tool",
    "Year": "edition",
    "Raw table database": "raw_table_db",
    "Raw table schema": "raw_table_schema",
    "Raw table name": "raw_table_name",
    "Variable Name": "variable_name",
    "Indentation": "indentation",
    "Parent column": "parent_column",
    "Table topics": "table_topics",
    "Universe": "universe",
    "Subject area": "subject_area",
    "SQL aggregation phrase": "sql_aggregation_phrase",
})

new_prepped["table_name"] = new_prepped["table_name"].str.lower()
new_prepped["variable_name"] = new_prepped["variable_name"].str.lower()
new_prepped["parent_column"] = new_prepped["parent_column"].str.lower()
new_prepped["documentation"] = None
new_prepped = new_prepped.astype(object).where(pd.notnull(new_prepped), None)

table_metadata_dicts = (
    new_prepped
        .drop_duplicates(subset="table_name")
        .rename(columns={
            "Table description": "description",
            "Table description simple": "description_simple",
        })
)[[
    "table_name",
    "category",
    "description",
    "description_simple",
    "table_topics",
    "universe",
    "subject_area",
    "source",
    "suppression_threshold",
    "tool",
    "documentation"
]].to_dict(orient="records")


def cast_supression_threshold(table_metadata_row: dict):
    if pd.isnull(table_metadata_row["suppression_threshold"]):
        table_metadata_row["suppression_threshold"] = None
    else:
        table_metadata_row["suppression_threshold"] = int(table_metadata_row["suppression_threshold"])

    return table_metadata_row


table_metadata = [cast_supression_threshold(row) for row in table_metadata_dicts]


edition_metadata = (
    new_prepped
        .drop_duplicates(subset=["table_name", "edition"])
        .dropna(subset="edition")
)[[
    "table_name",
    "edition",
    "raw_table_db",
    "raw_table_schema",
    "raw_table_name",
    "documentation"
]]

# .to_csv(Path.cwd() / "data" / "editions_metadata_prepped.csv", index=False)

variable_metadata = new_prepped.rename(columns={"Column description": "description"})[[
    "variable_name",
    "table_name",
    "description",
    "indentation",
    "parent_column",
    "sql_aggregation_phrase",
    "documentation",
]]

#.to_csv(Path.cwd() / "data" / "variable_metadata_prepped.csv", index=False)

# Create all tables according to plan in d3models

bind_d3_metadata_tables(WorkspaceSession())

table_upsert_stmt = (
    posgres_upsert(D3TableMetadata)
        .values(table_metadata)
        .on_conflict_do_nothing()
)

variable_upsert_stmt = (
    posgres_upsert(D3VariableMetadata)
        .values(variable_metadata.to_dict(orient='records'))
        .on_conflict_do_nothing()
)

edition_upsert_stmt = (
    posgres_upsert(D3EditionMetadata)
        .values(edition_metadata.to_dict(orient='records'))
        .on_conflict_do_nothing()
)

with WorkspaceSession() as db:
    db.execute(table_upsert_stmt)
    db.execute(variable_upsert_stmt)
    db.execute(edition_upsert_stmt)

    db.commit()
