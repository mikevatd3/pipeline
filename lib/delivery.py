from sqlalchemy import Engine
import pandas as pd

"""
This is missing (as is the pipeline generally) logic to handle if you 
actually have a table with meaningful '_moe' columns.
"""


def add_moe_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    value_columns = [col for col in df.columns if col != "geoid"]
    for col in value_columns:
        df[col + "_moe"] = None

    return df[["geoid"] + [col for col in sorted(df.columns) if col != "geoid"]]


def push_moe_table(
    table: pd.DataFrame,
    table_name: str,
    engine: Engine,
    schema: str = "d3_present",
) -> None:
    table.to_sql(
        table_name + "_moe", engine, schema=schema, if_exists="replace", index=False
    )


# This one needs to change to create a view
def push_base_table(
    table: pd.DataFrame, table_name: str, engine: Engine, schema: str = "d3_present"
) -> None:
    table.to_sql(table_name, engine, schema=schema, if_exists="replace", index=False)


# Options no moe
# Moe with direct sum
# Moe with l2


