from pathlib import Path
from textwrap import indent, dedent
from sqlalchemy import text
import pandas as pd
import numpy as np
import tomli

from lib.aggregation import (
    build_outer_select, 
    build_inner_select,
    build_query,
)
from lib.d3models import get_variable_metadata, read_table_variables_to_dataframe
from lib.connection import build_sessions
from lib.suppression import apply_suppression


with open("pipeline_config.toml", "rb") as f:
    config = tomli.load(f)


_, WorkingSession, _ = build_sessions(config)


def check_lines_match(a: str, b: str):
    for left, right in zip(str(a).split("\n"), str(b).split("\n")):
        if left.strip() != right.strip():
            raise AssertionError(f"{left.strip()} != {right.strip()}")


def test_build_outer_select():
    db = WorkingSession()

    variables = get_variable_metadata(db, "b01980")    
    statement = build_outer_select(variables)

    # Awkward indenting / dedenting

    check_lines_match(statement, """\
        all_geoms.geoid,
        COALESCE(match_geoms.b01980001, 0) b01980001,
        COALESCE(match_geoms.b01980002, 0) b01980002,
        COALESCE(match_geoms.b01980003, 0) b01980003,
        COALESCE(match_geoms.b01980004, 0) b01980004,
        COALESCE(match_geoms.b01980005, 0) b01980005,
        COALESCE(match_geoms.b01980006, 0) b01980006,
        COALESCE(match_geoms.b01980007, 0) b01980007,
        COALESCE(match_geoms.b01980008, 0) b01980008,
        COALESCE(match_geoms.b01980009, 0) b01980009,
        COALESCE(match_geoms.b01980010, 0) b01980010,
        COALESCE(match_geoms.b01980011, 0) b01980011
        """
    )


def test_build_inner_select():
    db = WorkingSession()

    variables = get_variable_metadata(db, "b01980")    
    statement = build_inner_select(variables)

    check_lines_match(statement, """\
        COUNT(*) AS b01980001,
        COUNT(*) FILTER(WHERE license_type like 'Licensed Centers') AS b01980002,
        COUNT(*) FILTER(WHERE license_type like 'Licensed Group Homes') AS b01980003,
        COUNT(*) FILTER(WHERE license_type like 'Licensed Family Homes') AS b01980004,
        COUNT(*) FILTER(WHERE early_head_start = 'Yes') AS b01980005,
        COUNT(*) FILTER(WHERE great_start_readiness = 'Yes') AS b01980006,
        COUNT(*) FILTER(WHERE head_start = 'Yes') AS b01980007,
        SUM(capacity) AS b01980008,
        SUM(capacity) FILTER(WHERE early_head_start = 'Yes') AS b01980009,
        SUM(capacity) FILTER(WHERE great_start_readiness = 'Yes') AS b01980010,
        SUM(capacity) FILTER(WHERE head_start = 'Yes') AS b01980011"""
    )


def test_build_query():

    db = WorkingSession()

    variables = list(get_variable_metadata(db, "b01980"))

    inner_select = build_inner_select(variables)
    outer_select = build_outer_select(variables)

    query = build_query(outer_select, inner_select, 'childcare') # source table name

    correct = text(f"""
    SELECT 
        all_geoms.geoid,
        COALESCE(match_geoms.b01980001, 0) b01980001,
        COALESCE(match_geoms.b01980002, 0) b01980002,
        COALESCE(match_geoms.b01980003, 0) b01980003,
        COALESCE(match_geoms.b01980004, 0) b01980004,
        COALESCE(match_geoms.b01980005, 0) b01980005,
        COALESCE(match_geoms.b01980006, 0) b01980006,
        COALESCE(match_geoms.b01980007, 0) b01980007,
        COALESCE(match_geoms.b01980008, 0) b01980008,
        COALESCE(match_geoms.b01980009, 0) b01980009,
        COALESCE(match_geoms.b01980010, 0) b01980010,
        COALESCE(match_geoms.b01980011, 0) b01980011
    FROM
        (
            SELECT unnest(geoids) geoid,
                COUNT(*) AS b01980001,
                COUNT(*) FILTER(WHERE license_type like 'Licensed Centers') AS b01980002,
                COUNT(*) FILTER(WHERE license_type like 'Licensed Group Homes') AS b01980003,
                COUNT(*) FILTER(WHERE license_type like 'Licensed Family Homes') AS b01980004,
                COUNT(*) FILTER(WHERE early_head_start = 'Yes') AS b01980005,
                COUNT(*) FILTER(WHERE great_start_readiness = 'Yes') AS b01980006,
                COUNT(*) FILTER(WHERE head_start = 'Yes') AS b01980007,
                SUM(capacity) AS b01980008,
                SUM(capacity) FILTER(WHERE early_head_start = 'Yes') AS b01980009,
                SUM(capacity) FILTER(WHERE great_start_readiness = 'Yes') AS b01980010,
                SUM(capacity) FILTER(WHERE head_start = 'Yes') AS b01980011
            FROM
                childcare aa
                    INNER JOIN
                shp.blockgeom2geoids20 bb on st_intersects(aa.geom, bb.geom)
            GROUP BY geoid
        ) match_geoms
            RIGHT JOIN (
                SELECT unnest(geoids) geoid FROM shp.blockgeom2geoids20 
                GROUP BY geoid
            ) all_geoms on all_geoms.geoid = match_geoms.geoid
    """)

    check_lines_match(query, correct)


def test_suppression():

    db = WorkingSession()

    variables = list(get_variable_metadata(db, "b01992"))
    variable_metadata_df = read_table_variables_to_dataframe(variables)

    unsuppressed = pd.DataFrame({
        'b01992001': [100, 6, 7],
        'b01992002': [50, 3, 2],
        'b01992003': [50, 3, 2],
        'b01992004': [50, 3, 2],
        'b01992005': [50, 3, 2],
        'b01992006': [50, 3, 2],
        'b01992007': [50, 3, 2],
        'b01992008': [50, 3, 2],
        'b01992009': [50, 3, 2],
    })

    correct = pd.DataFrame({
        'b01992001': [100, 6, 7],
        'b01992002': [50, np.nan, np.nan],
        'b01992003': [50, np.nan, np.nan],
        'b01992004': [50, np.nan, np.nan],
        'b01992005': [50, np.nan, np.nan],
        'b01992006': [50, np.nan, np.nan],
        'b01992007': [50, np.nan, np.nan],
        'b01992008': [50, np.nan, np.nan],
        'b01992009': [50, np.nan, np.nan],
    })

    suppressed = apply_suppression(unsuppressed, variable_metadata_df, 6)

    assert (correct.fillna(-100) == suppressed.fillna(-100)).all().all()


if __name__ == "__main__":
    test_build_outer_select()
    test_build_inner_select()
    test_build_query()
    test_suppression()