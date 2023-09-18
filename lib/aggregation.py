from textwrap import indent
from sqlalchemy import text, Engine
import pandas as pd

from .d3models import D3VariableMetadata


def build_outer_select(variables: list[D3VariableMetadata]) -> str:
    """
    Returns the argument to the outer SELECT statement of the final query. 
    Looks basically like

    all_geoms.geoid,
    COALESCE(match_geoms.b01001001, 0) b01001001,
    COALESCE(match_geoms.b01001002, 0) b01001002,
    ... and so on.
    """
    return indent(",\n".join(
        ['all_geoms.geoid'] 
        + [
            f"COALESCE(match_geoms.{variable.variable_name}, 0) {variable.variable_name}"
            for variable in variables
        ] 
    ), "\t")


def build_inner_select(
    variables: list[D3VariableMetadata]
) -> str:
    """
    This builds the inner select from the rows in the aggregation field maps.
    """
    return indent(",\n".join(
        [
            f"{variable.sql_aggregation_phrase} AS {variable.variable_name}"
            for variable in variables
        ]
    ), "\t")


def build_query(outer_select, inner_select, source_table_name) -> text:
    result = text(f"""
    SELECT 
        {outer_select}
    FROM
        (
            SELECT unnest(geoids) geoid,
                {inner_select}
            FROM
                {source_table_name} aa
                    INNER JOIN
                shp.blockgeom2geoids20 bb on st_intersects(aa.geom, bb.geom)
            GROUP BY geoid
        ) match_geoms
            RIGHT JOIN (
                SELECT unnest(geoids) geoid FROM shp.blockgeom2geoids20 
                GROUP BY geoid
            ) all_geoms on all_geoms.geoid = match_geoms.geoid
    """)

    return result


def run_aggregation(
    source_table_name, variables: list[D3VariableMetadata], engine: Engine
) -> pd.DataFrame:

    outer_select = build_outer_select(variables)
    inner_select = build_inner_select(variables)    

    data_query = build_query(outer_select, inner_select, source_table_name) 

    with engine.connect() as connection:
        aggregated = pd.read_sql(
            data_query, 
            connection
        )

    return aggregated
