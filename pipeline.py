import sys
from textwrap import dedent
from pathlib import Path
import argparse
from argparse import RawTextHelpFormatter

import tomli

from lib.connection import build_connections
from lib.d3models import (
    get_table_metadata,
    get_edition_metadata,
    get_latest_edition_metadata,
    get_variable_metadata,
    read_table_variables_to_dataframe,
    InvalidEditionError,
    D3EditionMetadata,
)
from lib.aggregation import run_aggregation
from lib.suppression import apply_suppression
from lib.empty import build_empty_table
from lib.delivery import (
    push_base_table,
    add_moe_columns,
    push_moe_table,
)
from lib.metadata import (
    update_metadata
)


__version__ = "0.0.1"


parser = argparse.ArgumentParser(
    prog="D3 HIP / SDC aggregator & DUA suppressor",
    description=dedent(
        """
    Aggregates the data according to the sql statements in the config files 
    provided in the table_config directory.

    Removes any value that is below the threshold provided (reads from metadata 
    but usually 6), and all values that could be used to infer those values.

    This is calculated based on the indentation level provided in the Census 
    Reporter metadata for all variables.
    """
    ),
    formatter_class=RawTextHelpFormatter,
)
parser.add_argument(
    "-v",
    "--version",
    action="version",
    version=f"D3 HIP / SDC aggregator & DUA suppressor {__version__}",
)
parser.add_argument("table_name", help="The name of the table that you're building.")
parser.add_argument(
    "-e",
    "--edition",
    help="The year of the edition entry in the d3_edition_metadata table.",
)
parser.add_argument(
    "-ds",
    "--destination_schema",
    help="The time frame of the table that you're building: ['d3_preset', 'd3_past'].",
)
parser.add_argument(
    "-hlw", 
    "--hollow", 
    action="store_true",
    help="Pushes empty base and moe tables to the database with the correct fields."
)
parser.add_argument(
    "-rbm", 
    "--rebuild_metadata", 
    action="store_true",
    help="Rebuilds the metadata--use this if a change has happened in d3 metadata."
)
parser.add_argument(
    "--config",
    default="pipeline_config.toml",
    help=dedent(
        """\
    The config file must have your database credentials for source, 
    workspace, and destination databases.

    pipeline_config.toml ---------------------------------------------------------

    [source_db]
    host = "<host name or ip address>"
    dbname = "<db name>"
    user = "<username>"
    password = "<your password>"

    [workspace_db]
    host = "<host name or ip address>"
    dbname = "<db name>"
    user = "<username>"
    password = "<your password>"

    [destination_db]
    host = "<host name or ip address>"
    dbname = "<db name>"
    user = "<username>"
    password = "<your password>"

    ---------------------------------------------------------------------
    """
    ),
)


if __name__ == "__main__":
    namespace = parser.parse_args()

    with open(namespace.config, "rb") as f:
        config = tomli.load(f)

    # The source db is provided on a per-table basis, so this will have to change
    source_session_maker, workspace_session_maker, destination_session_maker = build_connections(config)

    WorkspaceSession = workspace_session_maker()

    with WorkspaceSession() as db:

        # Load the metadata for the process
        if namespace.hollow:
            if namespace.destination_schema is None:
                print("""
                    When pushing an hollow (no data) table, you must also provide a destination schema name 
                    with -ds, --destination_schema flag.
                """)
                sys.exit()
            destination_schema = namespace.destination_schema

            edition_metadata = D3EditionMetadata(table_name=namespace.table_name, edition="PLACEHOLDER")
        elif not namespace.edition:
            edition_metadata = get_latest_edition_metadata(db, namespace.table_name)
            destination_schema = "d3_present"
        else:
            try:
                edition_metadata = get_edition_metadata(
                    db, namespace.table_name, namespace.edition
                )
            except InvalidEditionError as e:
                print(e)
                sys.exit()

            if namespace.destination_schema is None:
                print("""
                    If you provide an edition, you must also provide a destination schema name 
                    with -ds, --destination_schema flag.
                """)
                sys.exit()

            destination_schema = namespace.destination_schema

        table_metadata = get_table_metadata(db, namespace.table_name)
        variable_metadata = get_variable_metadata(db, namespace.table_name)
        variable_metadata_df = read_table_variables_to_dataframe(variable_metadata)

        if (
            (edition_metadata.raw_table_db is None) 
            | (edition_metadata.raw_table_schema is None)
        ) & (not namespace.hollow):
             print("No database or schema provided in the editions metadata table. Update before attempting again.")
             sys.exit()

        print("Metadata loaded, beginning aggregation.")

        # Run the aggregation according to the plan

        if namespace.hollow:
            unsuppressed = build_empty_table(variable_metadata)

        else:
            SourceSession = source_session_maker(
                edition_metadata.raw_table_db, 
                edition_metadata.raw_table_schema
            )
            unsuppressed = run_aggregation(
                # Have to do it this way because the postgis stuff isn't available in the lower namespaces.
                f"{edition_metadata.raw_table_schema}.{edition_metadata.raw_table_name}", 
                variable_metadata,
                SourceSession().get_bind(),
            )

        # Apply suppression if necessary
        if not table_metadata.suppression_threshold:
            print("Aggregation complete.")
            final = unsuppressed
        elif namespace.hollow:
            print("Hollow table ready.")
            final = unsuppressed
        else:
            print("Aggregation complete, beginning suppression.")
            final = apply_suppression(
                unsuppressed, 
                variable_metadata_df, 
                table_metadata.suppression_threshold
            )


        print("Pushing updated tables to destination database.")
        # Deliver the tables to the destination table
        # tables in the correct form.

        DestinationSession = destination_session_maker(destination_schema)

        push_base_table(final, namespace.table_name, DestinationSession().get_bind(), schema=destination_schema)

        final_moe = add_moe_columns(final)
        push_moe_table(final_moe, namespace.table_name, DestinationSession().get_bind(), schema=destination_schema)

        print("Saving backups.")
        # Save backups
        final.to_csv(Path.cwd() / "products" / f"{namespace.table_name}_{edition_metadata.edition}.csv", index=False)
        final_moe.to_csv(Path.cwd() / "products" / f"{namespace.table_name}_{edition_metadata.edition}_moe.csv", index=False)

        # Update the metadata tables if necessary 
        if namespace.rebuild_metadata:
            print("Updating metadata on destination database.")
            with DestinationSession() as db:
                update_metadata(
                    db,
                    table_metadata,
                    variable_metadata,
                )

        print("Complete!")