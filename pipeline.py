import sys
from textwrap import dedent
from pathlib import Path
import argparse
from argparse import RawTextHelpFormatter

from sqlalchemy.orm import sessionmaker
import tomli

from lib.connection import (
    build_workspace_engine,
    build_source_engine,
    build_destination_engine,
    open_workspace_tunnel,
    open_destination_tunnel,
)
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
from lib.metadata import update_metadata


__version__ = "0.0.3"


parser = argparse.ArgumentParser(
    prog="D3 HIP / SDC aggregator & DUA suppressor",
    description=dedent(
        """

    8888888b.   .d8888b.       8888888b.  d8b                   888 d8b                   
    888  "Y88b d88P  Y88b      888   Y88b Y8P                   888 Y8P                   
    888    888      .d88P      888    888                       888                       
    888    888     8888"       888   d88P 888 88888b.   .d88b.  888 888 88888b.   .d88b.  
    888    888      "Y8b.      8888888P"  888 888 "88b d8P  Y8b 888 888 888 "88b d8P  Y8b 
    888    888 888    888      888        888 888  888 88888888 888 888 888  888 88888888 
    888  .d88P Y88b  d88P      888        888 888 d88P Y8b.     888 888 888  888 Y8b.     
    8888888P"   "Y8888P"       888        888 88888P"   "Y8888  888 888 888  888  "Y8888  
                                              888                                         
                                              888                                         
                                              888                                        

    D3 Pipeline is the aggregation tool for the D3 API.

    The tool draws data from EDW and builds the target aggregate table following the 
    table build 'recipe' found on sdcapi.datadrivendetroit.org/admin. It applies
    the suppression if indicated in the recipe before pushing to the public API.

    To set up, add appropriate credentials to the 'config_template.toml' file and
    rename to 'pipefile_config.toml'. Then you're ready to go!

    Access or update the table build recipes at sdcapi.datadrivendetroit.org/admin 
    (login credentials required).
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
parser.add_argument(
    "table_name", help="The name of the table that you're building."
)
parser.add_argument(
    "-e",
    "--edition",
    help="The year of the edition entry in the d3_edition_metadata table.",
)
parser.add_argument(
    "-ds",
    "--destination_schema",
    help="The time frame of the table that you're building: ['d3_<year>', 'd3_preset', 'd3_past'].",
)
parser.add_argument(
    "-hlw",
    "--hollow",
    action="store_true",
    help="Pushes empty base and moe tables to the database with the correct fields.",
)
parser.add_argument(
    "-nu",
    "--no_update",
    action="store_true",
    help="Simply reads the table metadata from the workspace database--useful if you only want to rebuild metadata.",
)
parser.add_argument(
    "--config",
    default="pipeline_config.toml",
    help="Check the config_template.toml for the correct structure.",
)


def get_destination_schema(namespace):
    if namespace.hollow and (namespace.destination_schema is None):
        print(
            """
            When pushing an hollow (no data) table, you must also provide a destination schema name 
            with -ds, --destination_schema flag.
        """
        )
        sys.exit()
    if namespace.destination_schema is None:
        return "d3_present"

    if (namespace.destination_schema is None) and (
        namespace.edition is not None
    ):
        print(
            """
            If you provide an edition, you must also provide a destination schema name 
            with -ds, --destination_schema flag.
            """
        )
        sys.exit()

    return namespace.destination_schema


def load_metadata(db, namespace):
    """
    Load the metadata for the destination table. This contains the 'recipe' for the
    sql query that generates the final aggregation.
    """

    if namespace.hollow:
        edition_metadata = D3EditionMetadata(
            table_name=namespace.table_name, edition="PLACEHOLDER"
        )
    elif not namespace.edition:
        edition_metadata = get_latest_edition_metadata(db, namespace.table_name)

    else:
        try:
            edition_metadata = get_edition_metadata(
                db, namespace.table_name, namespace.edition
            )
        except InvalidEditionError as e:
            print(e)
            sys.exit()

    table_metadata = get_table_metadata(db, namespace.table_name)
    variable_metadata = get_variable_metadata(db, namespace.table_name)

    if (
        (edition_metadata.raw_table_db is None)
        | (edition_metadata.raw_table_schema is None)
    ) & (not namespace.hollow):
        print(
            "No database or schema provided in the editions metadata table. Add this argument before attempting again."
        )
        sys.exit()

    print(f"Metadata loaded for {table_metadata.table_name}: {table_metadata.description}, beginning aggregation.")

    return edition_metadata, variable_metadata, table_metadata


def main():
    namespace = parser.parse_args()

    with open(namespace.config, "rb") as f:
        config = tomli.load(f)

    # This has some validation side effects, so run it here before any querying happens
    destination_schema = get_destination_schema(namespace)

    # 1. Load metadata
    # because the workspace database is on a box accessible through ssh, open a tunnel
    with open_workspace_tunnel(config) as tunnel:
        workspace_engine = build_workspace_engine(config, tunnel.local_bind_port) # type: ignore
        WorkspaceSession = sessionmaker(workspace_engine)

        with WorkspaceSession() as db:
            # Load metadata from the workspace database
            edition_metadata, variable_metadata, table_metadata = load_metadata(
                db, namespace
            )
            variable_metadata_df = read_table_variables_to_dataframe(
                variable_metadata
            )

    # 2. Run aggregation
    if namespace.hollow or namespace.no_update:
        # If the hollow flag is set, build an empty dataframe with the correct shape.
        unsuppressed = build_empty_table(variable_metadata)

    else:
        # otherwise run the aggregation to obtain the dataframe
        source_engine = build_source_engine(
            config,
            edition_metadata.raw_table_db,
        )
        unsuppressed = run_aggregation(
            # Have to do it this way because the postgis stuff isn't available in the lower namespaces.
            # Maybe there is a way to handle this by adding to the schema instead of replacing the schema name.
            f"{edition_metadata.raw_table_schema}.{edition_metadata.raw_table_name}",
            variable_metadata,
            source_engine,
        )

    # 3. Apply suppression if necessary
    if not table_metadata.suppression_threshold:
        print("Aggregation complete.")
        final = unsuppressed
    elif namespace.hollow:
        print("Hollow table ready.")
        final = unsuppressed
    elif namespace.no_update:
        final = unsuppressed
    else:
        print("Aggregation complete, beginning suppression.")
        final = apply_suppression(
            unsuppressed,
            variable_metadata_df,
            threshold=table_metadata.suppression_threshold, 
        )

    # 4. Deliver tables
    # Need an ssh tunnel like the workspace connection above
    with open_destination_tunnel(config) as tunnel:
        destination_engine = build_destination_engine(
            config, str(tunnel.local_bind_port), destination_schema # type: ignore
        )
        DestinationSession = sessionmaker(destination_engine)
        if not namespace.no_update:
            print(f"Pushing {namespace.table_name} to schema {destination_schema} on destination database.")

            push_base_table(
                final,
                namespace.table_name,
                destination_engine,
                schema=destination_schema,
            )

            final_moe = add_moe_columns(final)
            push_moe_table(
                final_moe,
                namespace.table_name,
                destination_engine,
                schema=destination_schema,
            )
        else:
            print("No-update flag was selected so no data is moving.")


        # Update the metadata tables if necessary
        print("Updating metadata on destination database.")
        try:
            with DestinationSession() as db:
                update_metadata(
                    db,
                    table_metadata,
                    variable_metadata,
                )
        except (TypeError, AttributeError) as e:
            print(f"ERROR: Unable to update metadata--{e}")

    print("Complete!")


if __name__ == "__main__":
    main()
